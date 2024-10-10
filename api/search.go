package api

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

type SegmentPath string

func (s SegmentPath) Valid() bool {
	return true
}

func findLowestSegmentWithNearbyTimestamp(paths []string, target int64, pred func(offs uint64, ts int64) bool) int {
	sp := parseSegmentPaths(paths)

	l, r := 0, len(sp)-1
	best := -1

	for l <= r {
		mid := l + ((r - l) / 2)
		if pred(sp[mid], int64(target)) {
			l = mid + 1
			best = mid
		} else {
			r = mid - 1
		}
	}

	return best
}

// Assumes that the input is ordered by low -> high
func findLowestSegmentFile(paths []string, offset uint64) string {
	log.Println("SEG_LU", offset, "IN", strings.Join(paths, ";"))

	pathByOffset := parseSegmentPaths(paths)

	left, right := 0, len(pathByOffset)-1
	best := uint64(0)

	for left <= right {
		mid := left + ((right - left) / 2)
		if pathByOffset[mid] <= offset {
			left = mid + 1
			best = pathByOffset[mid]
		} else {
			right = mid - 1
		}
	}

	return offsetToPath(best)
}

func offsetToPath(best uint64) string {
	return fmt.Sprintf("%020d", best)
}

func parseSegmentPaths(paths []string) []uint64 {
	offs := make([]uint64, len(paths))

	for i, p := range paths {
		parts := strings.Split(p, "/")
		log.Println(parts)

		if len(parts) != 2 {
			panic("invalid segment path, expected form: <topic>-<partition>/00000000000000000000")
		}

		fileParts := strings.Split(parts[1], ".")
		if len(fileParts) != 2 {
			panic("invalid segment file")
		}

		v, err := strconv.ParseUint(fileParts[0], 10, 64)
		if err != nil {
			log.Println(err)
			continue
		}

		offs[i] = v
	}
	return offs
}
