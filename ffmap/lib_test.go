package ffmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceUniqueUnion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  [][]int
		expect []int
	}{
		{
			name:   "unique_values",
			input:  [][]int{{1, 2}, {3, 4}},
			expect: []int{1, 2, 3, 4},
		},
		{
			name:   "middle_overlapping_values",
			input:  [][]int{{1, 2}, {2, 3}, {3, 4}},
			expect: []int{1, 2, 3, 4},
		},
		{
			name:   "single_slice",
			input:  [][]int{{1, 2, 3}},
			expect: []int{1, 2, 3},
		},
		{
			name:   "empty_slice",
			input:  [][]int{},
			expect: nil,
		},
		{
			name:   "nil",
			input:  nil,
			expect: nil,
		},
		{
			name:   "nested_empty_slices",
			input:  [][]int{{}, {}},
			expect: []int{},
		},
		{
			name:   "duplicate_values_in_slice",
			input:  [][]int{{1, 1, 2}, {2, 3, 3}},
			expect: []int{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sliceUniqueUnion(tt.input)

			assert.Len(t, got, len(tt.expect))
			assert.ElementsMatch(t, tt.expect, got)
		})
	}
}
