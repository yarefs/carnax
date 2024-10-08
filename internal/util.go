package internal

import (
	"fmt"
	commandv1 "github.com/yarefs/carnax/gen/command/v1"
	"os"
	"strings"
)

func FormatAddr(addr *commandv1.Address) string {
	return fmt.Sprintf("%d:%d", addr.PartitionIndex, addr.Offset)
}

func ShouldHave(key string, fallback ...string) string {
	res := os.Getenv(key)
	if len(strings.TrimSpace(res)) == 0 {
		if len(fallback) == 0 {
			return ""
		}
		return fallback[0]
	}
	return res
}

func MustHave(key string) string {
	res := os.Getenv(key)
	if len(res) == 0 {
		panic("No key set for " + key)
	}
	return res
}
