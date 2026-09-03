package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestSourceLineItemID(t *testing.T) {
	got := SourceLineItemID("0001007788", "1900012345", "001", "1000", "2026")
	want := "FI|role=VENDOR|CP=0001007788|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=1900012345-001|BUKRS=1000|GJAHR=2026"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestLineItemID(t *testing.T) {
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	got := LineItemID("1000", "1900012345", date, "2026", "001")
	want := "FI_POSITION|BUKRS=1000|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=1900012345|DOCDATA=2026-08-12|GJAHR=2026|POSNO=001"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestDocumentKey(t *testing.T) {
	date := time.Date(2026, 8, 12, 0, 0, 0, 0, time.UTC)
	got := DocumentKey("1000", DocumentTypeVendorOpenItem, "1900012345", date)
	want := "DOCUMENT|BUKRS=1000|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=1900012345|DOCDATA=2026-08-12"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestSourceLineItemReference(t *testing.T) {
	id := uuid.MustParse("4eb35ed1-3422-4ed4-b1f8-d405ea2838cb")
	got := SourceLineItemReference(id)
	want := "fi-ap://open-vendor-items/4eb35ed1-3422-4ed4-b1f8-d405ea2838cb"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFormatDocumentNumber(t *testing.T) {
	if got := FormatDocumentNumber(1900010000); got != "1900010000" {
		t.Fatalf("got %q", got)
	}
}

func TestFormatPositionNumber(t *testing.T) {
	got, err := FormatPositionNumber(1)
	if err != nil || got != "001" {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := FormatPositionNumber(0); err == nil {
		t.Fatal("expected error")
	}
	if _, err := FormatPositionNumber(1000); err == nil {
		t.Fatal("expected error")
	}
}
