package domain

import (
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

type OpenVendorItem struct {
	ID                      uuid.UUID
	SourceLineItemID        string
	DocumentType            string
	CompanyCode             string
	FiscalYear              string
	DocumentNumber          string
	DocumentData            time.Time
	PositionNumber          string
	LineItemID              string
	SourceLineItemReference string
	OriginDocumentID        *uuid.UUID
	CounterpartyID          string
	CounterpartyRole        string
	Amount                  Amount
	Currency                string
	DueDate                 time.Time
	PaymentPurpose          string
	PaymentMethod           string
	PaymentBlock            bool
	Status                  string
}

func (item OpenVendorItem) Validate() error {
	switch {
	case item.ID == uuid.Nil:
		return fmt.Errorf("id: %w", ErrMissingRequiredField)
	case item.SourceLineItemID == "":
		return fmt.Errorf("sourceLineItemId: %w", ErrMissingRequiredField)
	case item.DocumentType != DocumentTypeVendorOpenItem:
		return fmt.Errorf("documentType must be %s", DocumentTypeVendorOpenItem)
	case item.CompanyCode == "":
		return fmt.Errorf("companyCode: %w", ErrMissingRequiredField)
	case item.FiscalYear == "":
		return fmt.Errorf("fiscalYear: %w", ErrMissingRequiredField)
	case item.DocumentNumber == "":
		return fmt.Errorf("documentNumber: %w", ErrMissingRequiredField)
	case item.DocumentData.IsZero():
		return fmt.Errorf("documentData: %w", ErrMissingRequiredField)
	case item.PositionNumber == "":
		return fmt.Errorf("positionNumber: %w", ErrMissingRequiredField)
	case item.LineItemID == "":
		return fmt.Errorf("lineItemID: %w", ErrMissingRequiredField)
	case item.SourceLineItemReference == "":
		return fmt.Errorf("sourceLineItemReference: %w", ErrMissingRequiredField)
	case item.CounterpartyID == "":
		return fmt.Errorf("counterpartyId: %w", ErrMissingRequiredField)
	case item.CounterpartyRole != CounterpartyRoleVendor:
		return fmt.Errorf("counterpartyRole must be %s", CounterpartyRoleVendor)
	case !item.Amount.Decimal().IsPositive():
		return ErrAmountNotPositive
	case utf8.RuneCountInString(item.Currency) != 3:
		return fmt.Errorf("currency must be a 3-letter ISO 4217 code")
	case item.DueDate.IsZero():
		return fmt.Errorf("dueDate: %w", ErrMissingRequiredField)
	case item.DueDate.Before(truncateDate(item.DocumentData)):
		return ErrDueDateBeforeDocument
	case item.PaymentPurpose == "":
		return fmt.Errorf("paymentPurpose: %w", ErrMissingRequiredField)
	case item.PaymentMethod == "":
		return fmt.Errorf("paymentMethod: %w", ErrMissingRequiredField)
	case item.Status == "":
		return fmt.Errorf("status: %w", ErrMissingRequiredField)
	}
	return nil
}

func truncateDate(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, t.Location())
}
