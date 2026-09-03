package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

func FormatDocumentNumber(n int64) string {
	return fmt.Sprintf("%010d", n)
}

func FormatPositionNumber(n int) (string, error) {
	if n < 1 || n > 999 {
		return "", ErrInvalidPositionNumber
	}
	return fmt.Sprintf("%03d", n), nil
}

func FormatDate(t time.Time) string {
	return t.Format("2006-01-02")
}

func SourceLineItemID(counterpartyID, documentNumber, positionNumber, companyCode, fiscalYear string) string {
	return fmt.Sprintf(
		"FI|role=VENDOR|CP=%s|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=%s-%s|BUKRS=%s|GJAHR=%s",
		counterpartyID, documentNumber, positionNumber, companyCode, fiscalYear,
	)
}

func LineItemID(companyCode, documentNumber string, documentData time.Time, fiscalYear, positionNumber string) string {
	return fmt.Sprintf(
		"FI_POSITION|BUKRS=%s|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=%s|DOCDATA=%s|GJAHR=%s|POSNO=%s",
		companyCode, documentNumber, FormatDate(documentData), fiscalYear, positionNumber,
	)
}

func DocumentKey(companyCode, documentType, documentNumber string, documentData time.Time) string {
	return fmt.Sprintf(
		"DOCUMENT|BUKRS=%s|DOCTYPE=%s|DOCNO=%s|DOCDATA=%s",
		companyCode, documentType, documentNumber, FormatDate(documentData),
	)
}

func SourceLineItemReference(id uuid.UUID) string {
	return fmt.Sprintf("fi-ap://open-vendor-items/%s", id.String())
}
