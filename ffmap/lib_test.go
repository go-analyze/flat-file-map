package ffmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  map[string]int
		expect []string
	}{
		{
			name:   "basic",
			input:  map[string]int{"a": 1, "b": 2, "c": 3},
			expect: []string{"a", "b", "c"},
		},
		{
			name:   "empty_map",
			input:  map[string]int{},
			expect: []string{},
		},
		{
			name:   "nil",
			input:  nil,
			expect: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapKeys(tt.input)

			assert.Equal(t, tt.expect, got)
		})
	}
}

func TestSliceUniqueUnion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input [][]int
		want  []int
	}{
		{
			name:  "unique_values",
			input: [][]int{{1, 2}, {3, 4}},
			want:  []int{1, 2, 3, 4},
		},
		{
			name:  "middle_overlapping_values",
			input: [][]int{{1, 2}, {2, 3}, {3, 4}},
			want:  []int{1, 2, 3, 4},
		},
		{
			name:  "single_slice",
			input: [][]int{{1, 2, 3}},
			want:  []int{1, 2, 3},
		},
		{
			name:  "empty_slice",
			input: [][]int{},
			want:  nil,
		},
		{
			name:  "nil",
			input: nil,
			want:  nil,
		},
		{
			name:  "nested_empty_slices",
			input: [][]int{{}, {}},
			want:  []int{},
		},
		{
			name:  "duplicate_values_in_slice",
			input: [][]int{{1, 1, 2}, {2, 3, 3}},
			want:  []int{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sliceUniqueUnion(tt.input)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
