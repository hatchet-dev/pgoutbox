package sizing

import (
	"time"

	"github.com/hatchet-dev/pgoutbox/sqlc"
)

const (
	MinPartitionSize  int64 = 1_000
	MaxPartitionSize  int64 = 10_000_000
	MinPartitionCount int   = 1
	MaxPartitionCount int   = 16

	resizeWriteThreshold = 50_000
	resizeAckThreshold   = 50_000
)

type Defaults struct {
	PartitionSize  int64
	PartitionCount int
}

func MaybeResize(meta *sqlc.TopicMetum, now time.Time) (size int64, count int, changed bool) {
	size = meta.PartitionSize
	count = int(meta.PartitionCount)

	if meta.WritesSinceResize < resizeWriteThreshold && meta.AcksSinceResize < resizeAckThreshold {
		return size, count, false
	}

	var writeRate, ackRate float64
	if meta.LastWriteAt.Valid {
		elapsed := now.Sub(meta.LastWriteAt.Time)
		if elapsed > 0 {
			writeRate = float64(meta.WritesSinceResize) / elapsed.Seconds()
		}
	}
	if meta.LastProcessAt.Valid {
		elapsed := now.Sub(meta.LastProcessAt.Time)
		if elapsed > 0 {
			ackRate = float64(meta.AcksSinceResize) / elapsed.Seconds()
		}
	}

	pressure := writeRate
	if ackRate > pressure {
		pressure = ackRate
	}

	newSize := size
	newCount := count

	switch {
	case pressure > 5000:
		newSize = grow(size, 2)
		newCount = growInt(count, 1)
	case pressure > 1000:
		newSize = grow(size, 1.5)
	case pressure < 50 && size > MinPartitionSize*2:
		newSize = shrink(size, 2)
		if newCount > MinPartitionCount {
			newCount--
		}
	}

	newSize = clamp(newSize, MinPartitionSize, MaxPartitionSize)
	newCount = clampInt(newCount, MinPartitionCount, MaxPartitionCount)

	if newSize == size && newCount == count {
		return size, count, false
	}

	return newSize, newCount, true
}

func grow(v int64, factor float64) int64 {
	next := int64(float64(v) * factor)
	if next <= v {
		next = v + MinPartitionSize
	}
	return next
}

func shrink(v int64, divisor int64) int64 {
	if divisor <= 0 {
		return v
	}
	next := v / divisor
	if next < MinPartitionSize {
		return MinPartitionSize
	}
	return next
}

func growInt(v, delta int) int {
	return v + delta
}

func clamp(v, min, max int64) int64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}
