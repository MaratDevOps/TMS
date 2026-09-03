package generate

import (
	"context"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

// Store persists an origin document (optional), open vendor item and outbox
// event in a single transaction.
type Store interface {
	Save(ctx context.Context, origin *domain.OriginDocument, item domain.OpenVendorItem, event domain.OutboxEvent) error
}

// DocumentNumbers returns unique FI document numbers from fi_document_number_seq.
type DocumentNumbers interface {
	NextBatch(ctx context.Context, count int) ([]string, error)
}
