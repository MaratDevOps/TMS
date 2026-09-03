package domain

import "errors"

var (
	ErrAmountNotPositive     = errors.New("amount must be greater than zero")
	ErrDueDateBeforeDocument = errors.New("due date must not be before document date")
	ErrInvalidPositionNumber = errors.New("position number must be in 1..999")
	ErrMissingRequiredField  = errors.New("required field is empty")
)
