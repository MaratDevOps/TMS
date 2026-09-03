-- +goose Up

CREATE SEQUENCE fi_document_number_seq
    AS BIGINT
    START WITH 1900010000
    INCREMENT BY 1;

CREATE TABLE origin_documents (
    id UUID PRIMARY KEY,
    company_code VARCHAR(7) NOT NULL,
    document_type VARCHAR(30) NOT NULL,
    document_number VARCHAR(20) NOT NULL,
    document_data DATE NOT NULL,
    CONSTRAINT uq_origin_documents_business_key
        UNIQUE (company_code, document_type, document_number, document_data)
);

CREATE TABLE open_vendor_items (
    id UUID PRIMARY KEY,
    source_line_item_id VARCHAR(512) NOT NULL,
    document_type VARCHAR(30) NOT NULL DEFAULT 'VENDOR_OPEN_ITEM',
    company_code VARCHAR(7) NOT NULL,
    fiscal_year CHAR(4) NOT NULL,
    document_number VARCHAR(20) NOT NULL,
    document_data DATE NOT NULL,
    position_number VARCHAR(3) NOT NULL,
    line_item_id VARCHAR(512) NOT NULL,
    source_line_item_reference VARCHAR(1024) NOT NULL,
    origin_document_id UUID NULL,
    counterparty_id VARCHAR(64) NOT NULL,
    counterparty_role VARCHAR(20) NOT NULL DEFAULT 'VENDOR',
    amount NUMERIC(19, 2) NOT NULL,
    currency CHAR(3) NOT NULL,
    due_date DATE NOT NULL,
    payment_purpose VARCHAR(500) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    payment_block BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(50) NOT NULL,
    CONSTRAINT uq_open_vendor_items_source_line_item_id UNIQUE (source_line_item_id),
    CONSTRAINT uq_open_vendor_items_line_item_id UNIQUE (line_item_id),
    CONSTRAINT uq_open_vendor_items_fi_position
        UNIQUE (company_code, fiscal_year, document_number, position_number),
    CONSTRAINT chk_open_vendor_items_document_type CHECK (document_type = 'VENDOR_OPEN_ITEM'),
    CONSTRAINT chk_open_vendor_items_counterparty_role CHECK (counterparty_role = 'VENDOR'),
    CONSTRAINT chk_open_vendor_items_amount CHECK (amount > 0),
    CONSTRAINT chk_open_vendor_items_due_date CHECK (due_date >= document_data),
    CONSTRAINT fk_open_vendor_items_origin_document
        FOREIGN KEY (origin_document_id) REFERENCES origin_documents (id)
);

CREATE INDEX ix_open_vendor_items_origin_document_id
    ON open_vendor_items (origin_document_id);

CREATE TABLE outbox (
    event_id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(50) NOT NULL DEFAULT 'VENDOR_OPEN_ITEM',
    event_type VARCHAR(100) NOT NULL DEFAULT 'payment.demand',
    event_version VARCHAR(20) NOT NULL,
    topic VARCHAR(249) NOT NULL DEFAULT 'payment.demand',
    message_key VARCHAR(512) NOT NULL,
    payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_outbox_aggregate
        FOREIGN KEY (aggregate_id) REFERENCES open_vendor_items (id) ON DELETE RESTRICT
);

CREATE INDEX ix_outbox_aggregate_id ON outbox (aggregate_id);
CREATE INDEX ix_outbox_created_at ON outbox (created_at, event_id);

CREATE TABLE outbox_delivery_attempts (
    id UUID PRIMARY KEY,
    event_id UUID NOT NULL,
    attempt_number INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL,
    next_attempt_at TIMESTAMPTZ NULL,
    publisher_instance VARCHAR(255) NOT NULL,
    locked_until TIMESTAMPTZ NOT NULL,
    partition INTEGER NULL,
    offset_value BIGINT NULL,
    error_message TEXT NULL,
    CONSTRAINT uq_outbox_attempt_event_number UNIQUE (event_id, attempt_number),
    CONSTRAINT chk_outbox_attempts_attempt_number CHECK (attempt_number > 0),
    CONSTRAINT chk_outbox_attempts_status
        CHECK (status IN ('PROCESSING', 'PUBLISHED', 'FAILED', 'INTERRUPTED')),
    CONSTRAINT chk_outbox_attempts_partition CHECK (partition IS NULL OR partition >= 0),
    CONSTRAINT chk_outbox_attempts_offset CHECK (offset_value IS NULL OR offset_value >= 0),
    CONSTRAINT fk_outbox_attempts_event
        FOREIGN KEY (event_id) REFERENCES outbox (event_id) ON DELETE RESTRICT
);

CREATE INDEX ix_outbox_attempt_event_latest
    ON outbox_delivery_attempts (event_id, attempt_number DESC);

CREATE INDEX ix_outbox_attempt_retry
    ON outbox_delivery_attempts (status, next_attempt_at, locked_until);

-- +goose Down

DROP TABLE IF EXISTS outbox_delivery_attempts;
DROP TABLE IF EXISTS outbox;
DROP TABLE IF EXISTS open_vendor_items;
DROP TABLE IF EXISTS origin_documents;
DROP SEQUENCE IF EXISTS fi_document_number_seq;
