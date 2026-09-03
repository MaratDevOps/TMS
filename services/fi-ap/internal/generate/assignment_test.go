package generate

import "testing"

func TestDocumentCount(t *testing.T) {
	if got := DocumentCount(10, 10); got != 1 {
		t.Fatalf("got %d", got)
	}
	if got := DocumentCount(10000, 10); got != 1000 {
		t.Fatalf("got %d", got)
	}
	if got := DocumentCount(11, 10); got != 2 {
		t.Fatalf("got %d", got)
	}
}

func TestAssign(t *testing.T) {
	idx, pos := Assign(1, 10)
	if idx != 0 || pos != 1 {
		t.Fatalf("job 1: index=%d pos=%d", idx, pos)
	}
	idx, pos = Assign(10, 10)
	if idx != 0 || pos != 10 {
		t.Fatalf("job 10: index=%d pos=%d", idx, pos)
	}
	idx, pos = Assign(11, 10)
	if idx != 1 || pos != 1 {
		t.Fatalf("job 11: index=%d pos=%d", idx, pos)
	}
}
