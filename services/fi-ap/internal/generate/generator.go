package generate

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/config"
	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

type Built struct {
	Item   domain.OpenVendorItem
	Origin *domain.OriginDocument
	Event  domain.OutboxEvent
}

type Generator struct {
	Scenario       config.Scenario
	Reference      config.ReferenceData
	Topic          string
	ProducerSystem string
	Now            func() time.Time
	NewID          func() uuid.UUID
}

func (g Generator) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now().UTC()
}

func (g Generator) newID() uuid.UUID {
	if g.NewID != nil {
		return g.NewID()
	}
	return uuid.New()
}

func (g Generator) Build(jobNumber int, documentNumber, positionNumber string) (Built, error) {
	if jobNumber < 1 {
		return Built{}, fmt.Errorf("job number must be >= 1")
	}
	if documentNumber == "" || positionNumber == "" {
		return Built{}, fmt.Errorf("documentNumber and positionNumber are required")
	}

	rnd := rand.New(rand.NewPCG(uint64(g.Scenario.RandomSeed), uint64(jobNumber)))
	ref := g.Reference

	wantOrigin := rnd.Float64() < g.Scenario.OriginDocumentShare
	companyCode, err := pick(rnd, ref.CompanyCodes)
	if err != nil {
		return Built{}, fmt.Errorf("companyCodes: %w", err)
	}
	counterpartyID, err := pick(rnd, ref.CounterpartyIDs)
	if err != nil {
		return Built{}, fmt.Errorf("counterpartyIds: %w", err)
	}
	currency, err := pick(rnd, ref.Currencies)
	if err != nil {
		return Built{}, fmt.Errorf("currencies: %w", err)
	}
	paymentMethod, err := pick(rnd, ref.PaymentMethods)
	if err != nil {
		return Built{}, fmt.Errorf("paymentMethods: %w", err)
	}
	status, err := pick(rnd, ref.Statuses)
	if err != nil {
		return Built{}, fmt.Errorf("statuses: %w", err)
	}
	amount, err := randomAmount(rnd, g.Scenario.Amount)
	if err != nil {
		return Built{}, err
	}

	docDate := randomDocumentDate(rnd, g.Scenario.BaseDocumentDate)
	dueDays := randomIntInclusive(rnd, g.Scenario.DueDays.Min, g.Scenario.DueDays.Max)
	dueDate := docDate.AddDate(0, 0, dueDays)
	paymentBlock := rnd.Float64() < g.Scenario.PaymentBlockProbability
	fiscalYear := fmt.Sprintf("%04d", docDate.Year())

	itemID := g.newID()
	item := domain.OpenVendorItem{
		ID:               itemID,
		DocumentType:     domain.DocumentTypeVendorOpenItem,
		CompanyCode:      companyCode,
		FiscalYear:       fiscalYear,
		DocumentNumber:   documentNumber,
		DocumentData:     docDate,
		PositionNumber:   positionNumber,
		CounterpartyID:   counterpartyID,
		CounterpartyRole: domain.CounterpartyRoleVendor,
		Amount:           amount,
		Currency:         currency,
		DueDate:          dueDate,
		PaymentMethod:    paymentMethod,
		PaymentBlock:     paymentBlock,
		Status:           status,
	}
	item.SourceLineItemID = domain.SourceLineItemID(counterpartyID, documentNumber, positionNumber, companyCode, fiscalYear)
	item.LineItemID = domain.LineItemID(companyCode, documentNumber, docDate, fiscalYear, positionNumber)
	item.SourceLineItemReference = domain.SourceLineItemReference(itemID)
	item.PaymentPurpose = fmt.Sprintf(g.Scenario.PaymentPurposeTemplate, item.LineItemID)

	var origin *domain.OriginDocument
	if wantOrigin {
		originType, err := pick(rnd, ref.OriginDocumentTypes)
		if err != nil {
			return Built{}, fmt.Errorf("originDocumentTypes: %w", err)
		}
		daysBefore := randomIntInclusive(rnd, 0, 7)
		originID := g.newID()
		o := domain.OriginDocument{
			ID:             originID,
			CompanyCode:    companyCode,
			DocumentType:   originType,
			DocumentNumber: originDocumentNumber(originID),
			DocumentData:   docDate.AddDate(0, 0, -daysBefore),
		}
		origin = &o
		item.OriginDocumentID = &originID
	}

	if err := item.Validate(); err != nil {
		return Built{}, err
	}

	occurredAt := g.now().UTC()
	eventID := g.newID()
	demand, err := domain.NewPaymentDemand(item, origin, eventID, occurredAt, g.ProducerSystem)
	if err != nil {
		return Built{}, err
	}
	payload, err := demand.MarshalPayload()
	if err != nil {
		return Built{}, fmt.Errorf("payload: %w", err)
	}

	return Built{
		Item:   item,
		Origin: origin,
		Event: domain.OutboxEvent{
			EventID:       eventID,
			AggregateID:   item.ID,
			AggregateType: domain.AggregateTypeVendorOpenItem,
			EventType:     domain.EventTypePaymentDemand,
			EventVersion:  domain.EventVersion,
			Topic:         g.Topic,
			MessageKey:    item.SourceLineItemID,
			Payload:       payload,
			OccurredAt:    occurredAt,
			CreatedAt:     occurredAt,
		},
	}, nil
}

func pick[T any](rnd *rand.Rand, values []T) (T, error) {
	var zero T
	if len(values) == 0 {
		return zero, fmt.Errorf("list is empty")
	}
	return values[rnd.IntN(len(values))], nil
}

func randomDocumentDate(rnd *rand.Rand, base time.Time) time.Time {
	day := time.Date(base.Year(), base.Month(), base.Day(), 0, 0, 0, 0, time.UTC)
	return day.AddDate(0, 0, rnd.IntN(91))
}

func randomIntInclusive(rnd *rand.Rand, min, max int) int {
	if max <= min {
		return min
	}
	return min + rnd.IntN(max-min+1)
}

func randomAmount(rnd *rand.Rand, r config.AmountRange) (domain.Amount, error) {
	minAmt, err := domain.NewAmount(string(r.Min))
	if err != nil {
		return domain.Amount{}, fmt.Errorf("amount.min: %w", err)
	}
	maxAmt, err := domain.NewAmount(string(r.Max))
	if err != nil {
		return domain.Amount{}, fmt.Errorf("amount.max: %w", err)
	}
	minCents := minAmt.Decimal().Shift(2).IntPart()
	maxCents := maxAmt.Decimal().Shift(2).IntPart()
	cents := minCents
	if maxCents > minCents {
		cents = minCents + rnd.Int64N(maxCents-minCents+1)
	}
	return domain.Amount(decimal.New(cents, -2)), nil
}

func originDocumentNumber(id uuid.UUID) string {
	hex := strings.ReplaceAll(id.String(), "-", "")
	if len(hex) > 20 {
		hex = hex[:20]
	}
	return hex
}
