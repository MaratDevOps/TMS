package domain

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Amount is a positive decimal matching PostgreSQL numeric(19,2).
type Amount decimal.Decimal

func NewAmount(s string) (Amount, error) {
	d, err := decimal.NewFromString(s)
	if err != nil {
		return Amount{}, fmt.Errorf("amount: %w", err)
	}
	if !d.IsPositive() {
		return Amount{}, ErrAmountNotPositive
	}
	return Amount(d), nil
}

func (a Amount) Decimal() decimal.Decimal {
	return decimal.Decimal(a)
}

func (a Amount) MarshalJSON() ([]byte, error) {
	return []byte(decimal.Decimal(a).StringFixed(2)), nil
}

func (a *Amount) UnmarshalJSON(data []byte) error {
	d, err := decimal.NewFromString(string(data))
	if err != nil {
		return fmt.Errorf("amount: %w", err)
	}
	if !d.IsPositive() {
		return ErrAmountNotPositive
	}
	*a = Amount(d)
	return nil
}
