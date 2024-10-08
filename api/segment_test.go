package api

import (
	"github.com/stretchr/testify/assert"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
	"testing"
)

func TestIndexFile_Search(t *testing.T) {
	index := []*apiv1.Index{
		{Offset: 0, Position: 0},
		{Offset: 13, Position: 13},
		{Offset: 26, Position: 26},
		{Offset: 39, Position: 39},
	}

	assert.Equal(t, uint64(26), IndexFile(index).Search(26).Position)
	assert.Equal(t, uint64(0), IndexFile(index).Search(0).Position)
}

func TestIndexFile_Search_ExactMatch(t *testing.T) {
	index := []*apiv1.Index{
		{Offset: 646, Position: 0},
		{Offset: 662, Position: 16},
		{Offset: 678, Position: 32},
		{Offset: 694, Position: 48},
		{Offset: 710, Position: 64},
		{Offset: 726, Position: 80},
		{Offset: 742, Position: 96},
		{Offset: 758, Position: 112},
		{Offset: 774, Position: 128},
		{Offset: 790, Position: 144},
	}

	assert.Equal(t, uint64(144), IndexFile(index).Search(790).Position)
}
