//go:build !race

package ordered

import (
	"context"
	"testing"
)

func TestPoolSubmitDoesNotAllocate(t *testing.T) {
	done := make(chan struct{}, 1)
	p := New(context.Background(), 2, 8, func(v [4]uintptr) [4]uintptr { return v }, func(context.Context, [4]uintptr) { done <- struct{}{} }, func([4]uintptr) {})
	defer p.Close()
	allocs := testing.AllocsPerRun(1000, func() {
		if err := p.Submit([4]uintptr{1, 2, 3, 4}); err != nil {
			t.Fatal(err)
		}
		<-done
	})
	if allocs != 0 {
		t.Fatalf("Submit and completion allocated %.1f times per item", allocs)
	}
}
