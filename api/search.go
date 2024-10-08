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

// Assumes that the input is ordered by low -> high
func findLowestSegmentFile(paths []string, offset uint64) string {
	log.Println("SEG_LU", offset, "IN", strings.Join(paths, ";"))

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

	left, right := 0, len(offs)-1
	best := uint64(0)

	for left <= right {
		mid := left + ((right - left) / 2)
		if offs[mid] <= offset {
			left = mid + 1
			best = offs[mid]
		} else {
			right = mid - 1
		}
	}

	return fmt.Sprintf("%020d", best)
}
