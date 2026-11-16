-- Orders database schema (owned exclusively by the order-service:
-- "database per service" — no other service may read these tables).

CREATE TABLE orders (
    id              UUID        PRIMARY KEY,
    customer_id     TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('PENDING','PAID','PAYMENT_FAILED','CANCELLED')),
    currency        TEXT        NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    total_minor     BIGINT      NOT NULL CHECK (total_minor >= 0),
    failure_reason  TEXT        NOT NULL DEFAULT '',
    idempotency_key TEXT,       -- NULL when the client sent none
    version         BIGINT      NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    -- Makes CreateOrder idempotent. NULLs are distinct in a UNIQUE
    -- constraint, so orders without a key never conflict.
    CONSTRAINT orders_idempotency_uniq UNIQUE (customer_id, idempotency_key)
);

-- Serves ListOrders: WHERE customer_id = ? ORDER BY created_at DESC, id DESC
CREATE INDEX orders_customer_created_idx ON orders (customer_id, created_at DESC, id DESC);

CREATE TABLE order_items (
    order_id         UUID   NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    line_no          INT    NOT NULL,
    product_id       TEXT   NOT NULL,
    quantity         INT    NOT NULL CHECK (quantity > 0),
    unit_price_minor BIGINT NOT NULL CHECK (unit_price_minor >= 0),
    PRIMARY KEY (order_id, line_no)
);

-- Inbox: IDs of events this service already consumed (idempotent consumer).
CREATE TABLE inbox (
    consumer     TEXT        NOT NULL,
    event_id     TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- Outbox: events to publish, written in the same transaction as the change.
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
