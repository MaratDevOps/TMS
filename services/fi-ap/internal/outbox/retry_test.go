package outbox

import (
	"testing"
	"time"
)

func TestBaseDelay(t *testing.T) {
	initial := time.Second
	max := 60 * time.Second
	cases := []struct {
		n    int
		want time.Duration
	}{
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
	}
	for _, tc := range cases {
		if got := BaseDelay(tc.n, initial, max, 2.0); got != tc.want {
			t.Fatalf("attempt %d: got %s want %s", tc.n, got, tc.want)
		}
	}
}

func TestWithJitterZero(t *testing.T) {
	base := 2 * time.Second
	if got := WithJitter(base, 0, func() float64 { return 0.5 }); got != base {
		t.Fatalf("got %s", got)
	}
}
