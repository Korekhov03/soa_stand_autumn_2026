CREATE TABLE IF NOT EXISTS purchases (
    id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    items JSONB NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents >= 0),
    phase TEXT NOT NULL DEFAULT 'stock'
        CHECK (phase IN ('stock', 'payment', 'uncertain', 'release', 'paid', 'rejected')),
    failures INTEGER NOT NULL DEFAULT 0,
    due_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS purchases_due ON purchases (phase, due_at);

CREATE TABLE IF NOT EXISTS outgoing_events (
    order_id UUID PRIMARY KEY REFERENCES purchases(id),
    payload JSONB NOT NULL,
    failures INTEGER NOT NULL DEFAULT 0,
    due_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outgoing_due ON outgoing_events (due_at)
    WHERE delivered_at IS NULL;

CREATE TABLE IF NOT EXISTS delivery_credits (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    points BIGINT NOT NULL CHECK (points > 0),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS delivery_credits_user ON delivery_credits (user_id);
