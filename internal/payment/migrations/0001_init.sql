-- Payments database schema (owned exclusively by the payment-service).

CREATE TABLE payments (
    id             UUID        PRIMARY KEY,
    -- UNIQUE is the last line of defence against charging an order twice.
    order_id       UUID        NOT NULL UNIQUE,
    customer_id    TEXT        NOT NULL,
    amount_minor   BIGINT      NOT NULL CHECK (amount_minor >= 0),
    currency       TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('COMPLETED','FAILED','REFUNDED')),
    failure_reason TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);

CREATE TABLE inbox (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    topic        TEXT        NOT NULL,
    msg_key      TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
