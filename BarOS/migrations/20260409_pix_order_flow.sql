BEGIN;

ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS payment_provider TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS payment_provider_id TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS provider_payment_id TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS provider_status TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS pix_qr_code TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS pix_copy_paste TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS pix_expires_at TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS expires_at TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS public_token TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS pickup_code TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS paid_at TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS delivered_at TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS webhook_received_at TEXT;
ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS payment_confirmed_by TEXT;

CREATE TABLE IF NOT EXISTS payment_webhook_events (
    id BIGSERIAL PRIMARY KEY,
    provider TEXT NOT NULL,
    provider_event_id TEXT NOT NULL,
    order_id BIGINT REFERENCES pedidos(id) ON DELETE SET NULL,
    event_type TEXT,
    status TEXT NOT NULL DEFAULT 'received',
    suspicious_reason TEXT,
    payload_json TEXT,
    headers_json TEXT,
    received_at TEXT NOT NULL,
    processed_at TEXT,
    UNIQUE(provider, provider_event_id)
);

CREATE TABLE IF NOT EXISTS order_audit_logs (
    id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES pedidos(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    actor TEXT NOT NULL,
    details_json TEXT,
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pedidos_public_token ON pedidos(public_token);
CREATE INDEX IF NOT EXISTS idx_pedidos_pickup_code ON pedidos(pickup_code);
CREATE INDEX IF NOT EXISTS idx_pedidos_provider_payment_id ON pedidos(provider_payment_id);
CREATE INDEX IF NOT EXISTS idx_pedidos_turno_payment_status ON pedidos(turno_id, payment_status);
CREATE INDEX IF NOT EXISTS idx_pedidos_status ON pedidos(status);
CREATE INDEX IF NOT EXISTS idx_itens_pedido_pedido_id ON itens_pedido(pedido_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_webhook_events_provider_event
    ON payment_webhook_events(provider, provider_event_id);

COMMIT;
