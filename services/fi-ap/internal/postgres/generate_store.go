package postgres

import (
	"context"
	"fmt"

	"github.com/MaratDevOps/TMS/services/fi-ap/internal/domain"
)

func (s *Store) Save(ctx context.Context, origin *domain.OriginDocument, item domain.OpenVendorItem, event domain.OutboxEvent) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if origin != nil {
		_, err = tx.Exec(ctx, `
			INSERT INTO origin_documents (
				id, company_code, document_type, document_number, document_data
			) VALUES ($1, $2, $3, $4, $5)
		`, origin.ID, origin.CompanyCode, origin.DocumentType, origin.DocumentNumber, origin.DocumentData)
		if err != nil {
			return fmt.Errorf("insert origin_documents: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO open_vendor_items (
			id, source_line_item_id, document_type, company_code, fiscal_year,
			document_number, document_data, position_number, line_item_id,
			source_line_item_reference, origin_document_id, counterparty_id,
			counterparty_role, amount, currency, due_date, payment_purpose,
			payment_method, payment_block, status
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, $11, $12,
			$13, $14, $15, $16, $17,
			$18, $19, $20
		)
	`,
		item.ID,
		item.SourceLineItemID,
		item.DocumentType,
		item.CompanyCode,
		item.FiscalYear,
		item.DocumentNumber,
		item.DocumentData,
		item.PositionNumber,
		item.LineItemID,
		item.SourceLineItemReference,
		item.OriginDocumentID,
		item.CounterpartyID,
		item.CounterpartyRole,
		item.Amount.Decimal().StringFixed(2),
		item.Currency,
		item.DueDate,
		item.PaymentPurpose,
		item.PaymentMethod,
		item.PaymentBlock,
		item.Status,
	)
	if err != nil {
		return fmt.Errorf("insert open_vendor_items: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (
			event_id, aggregate_id, aggregate_type, event_type, event_version,
			topic, message_key, payload, occurred_at, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`,
		event.EventID,
		event.AggregateID,
		event.AggregateType,
		event.EventType,
		event.EventVersion,
		event.Topic,
		event.MessageKey,
		[]byte(event.Payload),
		event.OccurredAt,
		event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
