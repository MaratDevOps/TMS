package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type PaymentDemand struct {
	EventID                 uuid.UUID              `json:"eventId"`
	EventType               string                 `json:"eventType"`
	EventVersion            string                 `json:"eventVersion"`
	OccurredAt              time.Time              `json:"occurredAt"`
	ProducerSystem          string                 `json:"producerSystem"`
	DocumentType            string                 `json:"documentType"`
	NumberFiPosition        NumberFiPosition       `json:"numberFiPosition"`
	OriginDocument          *OriginDocumentPayload `json:"originDocument,omitempty"`
	SourceLineItemID        string                 `json:"sourceLineItemId"`
	SourceLineItemReference string                 `json:"sourceLineItemReference"`
	CounterpartyID          string                 `json:"counterpartyId"`
	CounterpartyRole        string                 `json:"counterpartyRole"`
	Amount                  Amount                 `json:"amount"`
	Currency                string                 `json:"currency"`
	DueDate                 string                 `json:"dueDate"`
	PaymentPurpose          string                 `json:"paymentPurpose"`
	PaymentMethod           string                 `json:"paymentMethod"`
	PaymentBlock            bool                   `json:"paymentBlock"`
	Status                  string                 `json:"status"`
}

type NumberFiPosition struct {
	CompanyCode    string         `json:"companyCode"`
	FiscalYear     string         `json:"fiscalYear"`
	PositionNumber string         `json:"positionNumber"`
	LineItemID     string         `json:"lineItemID"`
	SourceDocument SourceDocument `json:"sourceDocument"`
}

type SourceDocument struct {
	CompanyCode    string `json:"companyCode"`
	DocumentType   string `json:"documentType"`
	DocumentNumber string `json:"documentNumber"`
	DocumentData   string `json:"documentData"`
	DocumentKey    string `json:"documentKey"`
}

type OriginDocumentPayload struct {
	CompanyCode    string `json:"companyCode"`
	DocumentType   string `json:"documentType"`
	DocumentNumber string `json:"documentNumber"`
	DocumentData   string `json:"documentData"`
	DocumentKey    string `json:"documentKey"`
}

func NewPaymentDemand(item OpenVendorItem, origin *OriginDocument, eventID uuid.UUID, occurredAt time.Time, producerSystem string) (PaymentDemand, error) {
	if err := item.Validate(); err != nil {
		return PaymentDemand{}, err
	}
	if eventID == uuid.Nil {
		return PaymentDemand{}, fmt.Errorf("eventId: %w", ErrMissingRequiredField)
	}
	if producerSystem == "" {
		return PaymentDemand{}, fmt.Errorf("producerSystem: %w", ErrMissingRequiredField)
	}

	demand := PaymentDemand{
		EventID:        eventID,
		EventType:      EventTypePaymentDemand,
		EventVersion:   EventVersion,
		OccurredAt:     occurredAt.UTC(),
		ProducerSystem: producerSystem,
		DocumentType:   DocumentTypeVendorOpenItem,
		NumberFiPosition: NumberFiPosition{
			CompanyCode:    item.CompanyCode,
			FiscalYear:     item.FiscalYear,
			PositionNumber: item.PositionNumber,
			LineItemID:     item.LineItemID,
			SourceDocument: SourceDocument{
				CompanyCode:    item.CompanyCode,
				DocumentType:   DocumentTypeVendorOpenItem,
				DocumentNumber: item.DocumentNumber,
				DocumentData:   FormatDate(item.DocumentData),
				DocumentKey:    DocumentKey(item.CompanyCode, DocumentTypeVendorOpenItem, item.DocumentNumber, item.DocumentData),
			},
		},
		SourceLineItemID:        item.SourceLineItemID,
		SourceLineItemReference: item.SourceLineItemReference,
		CounterpartyID:          item.CounterpartyID,
		CounterpartyRole:        item.CounterpartyRole,
		Amount:                  item.Amount,
		Currency:                item.Currency,
		DueDate:                 FormatDate(item.DueDate),
		PaymentPurpose:          item.PaymentPurpose,
		PaymentMethod:           item.PaymentMethod,
		PaymentBlock:            item.PaymentBlock,
		Status:                  item.Status,
	}
	if origin != nil {
		if err := origin.Validate(); err != nil {
			return PaymentDemand{}, err
		}
		demand.OriginDocument = &OriginDocumentPayload{
			CompanyCode:    origin.CompanyCode,
			DocumentType:   origin.DocumentType,
			DocumentNumber: origin.DocumentNumber,
			DocumentData:   FormatDate(origin.DocumentData),
			DocumentKey:    origin.DocumentKey(),
		}
	}
	return demand, nil
}

func (d PaymentDemand) MarshalPayload() ([]byte, error) {
	return json.Marshal(d)
}
