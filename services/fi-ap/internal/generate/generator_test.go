package generate

import (
	"testing"
	"time"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

func testScenario() config.Scenario {
	return config.Scenario{
		PositionCount:        10,
		ParallelThreads:      2,
		RandomSeed:           1001,
		PositionsPerDocument: 10,
		BaseDocumentDate:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		OriginDocumentShare:  1,
		Amount: config.AmountRange{
			Min: "100.00",
			Max: "1000.00",
		},
		DueDays:                 config.IntRange{Min: 1, Max: 30},
		PaymentBlockProbability: 0.05,
		PaymentPurposeTemplate:  "Оплата по FI-позиции %s",
		StopOnError:             true,
	}
}

func testReference() config.ReferenceData {
	return config.ReferenceData{
		CompanyCodes:        []string{"1000"},
		CounterpartyIDs:     []string{"0001007788"},
		Currencies:          []string{"RUB"},
		PaymentMethods:      []string{"BANK_TRANSFER"},
		Statuses:            []string{"CREATED"},
		OriginDocumentTypes: []string{"MIRO"},
	}
}

func testGenerator(share float64) Generator {
	s := testScenario()
	s.OriginDocumentShare = share
	return Generator{
		Scenario:       s,
		Reference:      testReference(),
		Topic:          domain.DefaultTopic,
		ProducerSystem: domain.ProducerSystemSAPFI,
		Now: func() time.Time {
			return time.Date(2026, 8, 12, 10, 15, 0, 0, time.UTC)
		},
	}
}

func TestBuildDeterministicBusinessFields(t *testing.T) {
	g := testGenerator(1)
	a, err := g.Build(3, "1900012345", "001")
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Build(3, "1900012345", "001")
	if err != nil {
		t.Fatal(err)
	}
	if !a.Item.Amount.Decimal().Equal(b.Item.Amount.Decimal()) {
		t.Fatalf("amount %s vs %s", a.Item.Amount.Decimal(), b.Item.Amount.Decimal())
	}
	if !a.Item.DocumentData.Equal(b.Item.DocumentData) || !a.Item.DueDate.Equal(b.Item.DueDate) {
		t.Fatal("dates differ")
	}
	if a.Item.PaymentBlock != b.Item.PaymentBlock {
		t.Fatal("paymentBlock differs")
	}
	if a.Item.CompanyCode != b.Item.CompanyCode || a.Item.CounterpartyID != b.Item.CounterpartyID {
		t.Fatal("reference values differ")
	}
	if (a.Origin == nil) != (b.Origin == nil) {
		t.Fatal("origin presence differs")
	}
	if a.Origin != nil && !a.Origin.DocumentData.Equal(b.Origin.DocumentData) {
		t.Fatal("origin date differs")
	}
	if a.Item.ID == b.Item.ID {
		t.Fatal("technical UUID must not be reused")
	}
}

func TestBuildIdentifiers(t *testing.T) {
	g := testGenerator(0)
	built, err := g.Build(1, "1900012345", "001")
	if err != nil {
		t.Fatal(err)
	}
	item := built.Item
	wantKey := domain.SourceLineItemID(item.CounterpartyID, "1900012345", "001", item.CompanyCode, item.FiscalYear)
	if item.SourceLineItemID != wantKey {
		t.Fatalf("sourceLineItemId %q", item.SourceLineItemID)
	}
	if built.Event.MessageKey != item.SourceLineItemID {
		t.Fatal("message key must equal sourceLineItemId")
	}
	if built.Origin != nil {
		t.Fatal("expected no origin")
	}
	if built.Event.EventType != domain.EventTypePaymentDemand {
		t.Fatal(built.Event.EventType)
	}
}

func TestBuildAlwaysOrigin(t *testing.T) {
	g := testGenerator(1)
	for n := 1; n <= 20; n++ {
		built, err := g.Build(n, "1900010001", "001")
		if err != nil {
			t.Fatal(err)
		}
		if built.Origin == nil || built.Item.OriginDocumentID == nil {
			t.Fatalf("job %d: origin required", n)
		}
		if built.Origin.CompanyCode != built.Item.CompanyCode {
			t.Fatal("origin company must match item")
		}
		if built.Origin.DocumentData.After(built.Item.DocumentData) {
			t.Fatal("origin date after item date")
		}
	}
}

func TestBuildNeverOrigin(t *testing.T) {
	g := testGenerator(0)
	for n := 1; n <= 20; n++ {
		built, err := g.Build(n, "1900010001", "001")
		if err != nil {
			t.Fatal(err)
		}
		if built.Origin != nil {
			t.Fatalf("job %d: origin unexpected", n)
		}
	}
}

func TestDifferentJobsCanDiffer(t *testing.T) {
	g := testGenerator(0.5)
	a, err := g.Build(1, "1900010001", "001")
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Build(2, "1900010001", "002")
	if err != nil {
		t.Fatal(err)
	}
	if a.Item.SourceLineItemID == b.Item.SourceLineItemID {
		t.Fatal("different positions must have different sourceLineItemId")
	}
}
