package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

type OriginDocument struct {
	ID             uuid.UUID
	CompanyCode    string
	DocumentType   string
	DocumentNumber string
	DocumentData   time.Time
}

func (d OriginDocument) Validate() error {
	switch {
	case d.ID == uuid.Nil:
		return fmt.Errorf("origin document id: %w", ErrMissingRequiredField)
	case d.CompanyCode == "":
		return fmt.Errorf("origin document company code: %w", ErrMissingRequiredField)
	case d.DocumentType == "":
		return fmt.Errorf("origin document type: %w", ErrMissingRequiredField)
	case d.DocumentNumber == "":
		return fmt.Errorf("origin document number: %w", ErrMissingRequiredField)
	case d.DocumentData.IsZero():
		return fmt.Errorf("origin document date: %w", ErrMissingRequiredField)
	}
	return nil
}

func (d OriginDocument) DocumentKey() string {
	return DocumentKey(d.CompanyCode, d.DocumentType, d.DocumentNumber, d.DocumentData)
}
