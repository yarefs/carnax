package internal

import (
	"fmt"
	commandv1 "github.com/yarefs/carnax/gen/command/v1"
)

func FormatAddr(addr *commandv1.Address) string {
	return fmt.Sprintf("%d:%d", addr.PartitionIndex, addr.Offset)
}
