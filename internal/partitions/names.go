package partitions

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
)

func TopicSlug(topic string) string {
	sum := md5.Sum([]byte(topic))
	return hex.EncodeToString(sum[:])[:16]
}

func FillSeqName(slug string) string {
	return "fill_seq_" + slug
}

func ListPartitionRelname(slug string) string {
	return "messages_t_" + slug
}

func RangePartitionRelname(slug string, index int) string {
	return fmt.Sprintf("messages_t_%s_p%d", slug, index)
}

func PartitionBounds(index int, size int64) (from, to int64) {
	from = int64(index)*size + 1
	to = int64(index+1)*size + 1
	return from, to
}

func PartitionIndexForID(id, size int64) int {
	if id <= 0 {
		return 0
	}
	return int((id - 1) / size)
}

func MaxPartitionIndex(id, size int64) int {
	if id <= 0 {
		return 0
	}
	return int((id - 1) / size)
}
