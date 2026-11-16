package ringbuffer

import (
	"reflect"
	"testing"
)

// Table-driven test: the idiomatic Go way to cover many cases compactly.
func TestSnapshotOrder(t *testing.T) {
	tests := []struct {
		name   string
		cap    int
		writes []int
		want   []int
	}{
		{"empty", 3, nil, []int{}},
		{"partial", 3, []int{1, 2}, []int{1, 2}},
		{"exactly full", 3, []int{1, 2, 3}, []int{1, 2, 3}},
		{"wrapped", 3, []int{1, 2, 3, 4, 5}, []int{3, 4, 5}},
		{"wrapped many times", 2, []int{1, 2, 3, 4, 5, 6, 7}, []int{6, 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := New[int](tt.cap)
			for _, w := range tt.writes {
				b.Push(w)
			}
			if got := b.Snapshot(); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v want %v", got, tt.want)
			}
		})
	}
}
