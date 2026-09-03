package outbox

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

var ErrPermanent = errors.New("outbox: permanent publication error")

func ValidateRecord(msg Message) error {
	switch {
	case strings.TrimSpace(msg.Topic) == "":
		return errors.Join(ErrPermanent, errors.New("topic is empty"))
	case strings.TrimSpace(msg.Key) == "":
		return errors.Join(ErrPermanent, errors.New("message key is empty"))
	case len(msg.Payload) == 0:
		return errors.Join(ErrPermanent, errors.New("payload is empty"))
	case !json.Valid(msg.Payload):
		return errors.Join(ErrPermanent, errors.New("payload is not valid JSON"))
	}
	return nil
}

func Retryable(err error) bool {
	if err == nil {
		return false
	}
	return !errors.Is(err, ErrPermanent)
}

func ClassifyLatest(a LatestAttempt, maxAttempts int) (published, finalFailed, pending bool) {
	if !a.Exists {
		return false, false, true
	}
	switch a.Status {
	case domain.AttemptPublished:
		return true, false, false
	case domain.AttemptProcessing:
		return false, false, true
	case domain.AttemptInterrupted:
		if a.AttemptNumber >= maxAttempts {
			return false, true, false
		}
		return false, false, true
	case domain.AttemptFailed:
		if a.NextAttemptAt != nil && a.AttemptNumber < maxAttempts {
			return false, false, true
		}
		return false, true, false
	default:
		return false, false, true
	}
}

func CountDelivery(eventIDs []uuid.UUID, rows []LatestAttempt, maxAttempts int) (published, finalFailed, pending int) {
	byID := make(map[uuid.UUID]LatestAttempt, len(rows))
	for _, row := range rows {
		byID[row.EventID] = row
	}
	for _, id := range eventIDs {
		row, ok := byID[id]
		if !ok {
			pending++
			continue
		}
		p, f, n := ClassifyLatest(row, maxAttempts)
		switch {
		case p:
			published++
		case f:
			finalFailed++
		case n:
			pending++
		}
	}
	return published, finalFailed, pending
}
