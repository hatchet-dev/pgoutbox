package partitions

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Manager struct {
	schema string
}

type topicPartition struct {
	index int
	from  int64
	to    int64
}

func NewManager(schema string) *Manager {
	return &Manager{schema: schema}
}

func (m *Manager) schemaIdent() string {
	return pgx.Identifier{m.schema}.Sanitize()
}

func (m *Manager) qualified(rel string) string {
	return m.schemaIdent() + "." + pgx.Identifier{rel}.Sanitize()
}

func (m *Manager) LockTopic(ctx context.Context, db sqlc.DBTX, topic string) error {
	_, err := db.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", "pgoutbox:"+m.schema+":"+topic)
	return err
}

func (m *Manager) EnsureHorizon(
	ctx context.Context,
	db sqlc.DBTX,
	q *sqlc.Queries,
	topic string,
	slug string,
	partitionSize int64,
	partitionCount int,
	startID int64,
	endID int64,
	fillHigh int64,
) error {
	highID := endID
	if fillHigh > highID {
		highID = fillHigh
	}

	if err := m.ensureListPartition(ctx, db, topic, slug); err != nil {
		return err
	}

	parts, err := m.loadTopicPartitions(ctx, db, topic)
	if err != nil {
		return err
	}

	if len(parts) == 0 {
		parts = append(parts, topicPartition{index: 0, from: 1, to: 1 + partitionSize})
		if err := m.createRangePartition(ctx, db, q, topic, slug, parts[0], partitionSize, "active"); err != nil {
			return err
		}
	}

	highIdx := partitionIndexContaining(parts, highID)
	for highIdx == -1 || parts[len(parts)-1].index < highIdx+partitionCount {
		last := parts[len(parts)-1]
		next := topicPartition{
			index: last.index + 1,
			from:  last.to,
			to:    last.to + partitionSize,
		}
		if err := m.createRangePartition(ctx, db, q, topic, slug, next, partitionSize, "future"); err != nil {
			return err
		}
		parts = append(parts, next)
		highIdx = partitionIndexContaining(parts, highID)
	}

	endIdx := partitionIndexContaining(parts, endID)

	for _, part := range parts {
		if part.to <= startID || part.from > endID {
			continue
		}
		highWater := endID
		if highWater >= part.to {
			highWater = part.to - 1
		}
		if err := q.UpdateTopicPartitionHighWater(ctx, db, sqlc.UpdateTopicPartitionHighWaterParams{
			Topic:          topic,
			PartitionIndex: int32(part.index),
			HighWaterID:    highWater,
		}); err != nil {
			return fmt.Errorf("update high water for partition %d: %w", part.index, err)
		}
	}

	if endIdx > 0 {
		if err := q.SealTopicPartitionsUpTo(ctx, db, sqlc.SealTopicPartitionsUpToParams{
			Topic:          topic,
			PartitionIndex: int32(endIdx),
		}); err != nil {
			return fmt.Errorf("seal prior partitions: %w", err)
		}
	}

	return nil
}

func (m *Manager) createRangePartition(
	ctx context.Context,
	db sqlc.DBTX,
	q *sqlc.Queries,
	topic string,
	slug string,
	part topicPartition,
	partitionSize int64,
	status string,
) error {
	listRel := ListPartitionRelname(slug)
	rangeRel := RangePartitionRelname(slug, part.index)

	if err := m.ensureRangePartition(ctx, db, listRel, rangeRel, part.from, part.to); err != nil {
		return err
	}

	if err := q.UpsertTopicPartition(ctx, db, sqlc.UpsertTopicPartitionParams{
		Topic:          topic,
		PartitionIndex: int32(part.index),
		Relname:        rangeRel,
		IDFrom:         part.from,
		IDTo:           part.to,
		PartitionSize:  partitionSize,
		Status:         status,
	}); err != nil {
		return fmt.Errorf("record topic partition %d: %w", part.index, err)
	}

	return nil
}

func (m *Manager) loadTopicPartitions(ctx context.Context, db sqlc.DBTX, topic string) ([]topicPartition, error) {
	sql := fmt.Sprintf(
		"SELECT partition_index, id_from, id_to FROM %s.topic_partitions WHERE topic = $1 AND status <> 'dropped' ORDER BY partition_index ASC",
		m.schemaIdent(),
	)
	rows, err := db.Query(ctx, sql, topic)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var parts []topicPartition
	for rows.Next() {
		var part topicPartition
		if err := rows.Scan(&part.index, &part.from, &part.to); err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return parts, nil
}

func partitionIndexContaining(parts []topicPartition, id int64) int {
	if id <= 0 && len(parts) > 0 {
		return parts[0].index
	}
	for _, part := range parts {
		if part.from <= id && id < part.to {
			return part.index
		}
	}
	return -1
}

func (m *Manager) ensureListPartition(ctx context.Context, db sqlc.DBTX, topic, slug string) error {
	listRel := ListPartitionRelname(slug)
	sql := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s.messages FOR VALUES IN (%s) PARTITION BY RANGE (id)",
		m.qualified(listRel),
		m.schemaIdent(),
		quoteLiteral(topic),
	)

	_, err := db.Exec(ctx, sql)
	return ignoreDuplicateTable(err)
}

func (m *Manager) ensureRangePartition(
	ctx context.Context,
	db sqlc.DBTX,
	listRel, rangeRel string,
	from, to int64,
) error {
	sql := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (%d) TO (%d)",
		m.qualified(rangeRel),
		m.qualified(listRel),
		from,
		to,
	)
	_, err := db.Exec(ctx, sql)
	return ignoreDuplicateTable(err)
}

func (m *Manager) EnsureFillSequence(ctx context.Context, db sqlc.DBTX, seqName string) error {
	sql := fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %s CACHE 100", m.qualified(seqName))
	_, err := db.Exec(ctx, sql)
	return ignoreDuplicateTable(err)
}

func (m *Manager) AdvanceFillSequence(ctx context.Context, db sqlc.DBTX, seqName string, count int) (int64, error) {
	if count <= 0 {
		return 0, nil
	}

	var value int64
	seq := quoteLiteral(m.qualified(seqName))
	sql := fmt.Sprintf("SELECT nextval(%s::regclass) FROM generate_series(1, %d) ORDER BY 1 DESC LIMIT 1", seq, count)
	if err := db.QueryRow(ctx, sql).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func (m *Manager) DropPartition(ctx context.Context, db sqlc.DBTX, relname string) error {
	_, err := db.Exec(ctx, "DROP TABLE IF EXISTS "+m.qualified(relname))
	return err
}

func ignoreDuplicateTable(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42P07", "23505":
			return nil
		}
	}
	if strings.Contains(err.Error(), "already exists") {
		return nil
	}
	return err
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
