package api

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIndexFile_Search(t *testing.T) {
	index := []Index{
		{0, 0},
		{13, 13},
		{26, 26},
		{39, 39},
	}

	assert.Equal(t, uint64(26), IndexFile(index).Search(26).Position)
	assert.Equal(t, uint64(0), IndexFile(index).Search(0).Position)
}

func TestIndexFile_Search_ExactMatch(t *testing.T) {
	index := []Index{
		{646, 0},
		{662, 16},
		{678, 32},
		{694, 48},
		{710, 64},
		{726, 80},
		{742, 96},
		{758, 112},
		{774, 128},
		{790, 144},
	}

	assert.Equal(t, uint64(144), IndexFile(index).Search(790).Position)
}
