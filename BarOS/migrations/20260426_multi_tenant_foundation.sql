BEGIN;

CREATE TABLE IF NOT EXISTS public.tenants (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'active',
    plan TEXT NOT NULL DEFAULT 'legacy',
    created_at TEXT NOT NULL
);

INSERT INTO public.tenants (name, slug, status, plan, created_at)
VALUES ('Default BarOS', 'default', 'active', 'legacy', NOW()::TEXT)
ON CONFLICT (slug) DO UPDATE SET
    name = COALESCE(NULLIF(TRIM(public.tenants.name), ''), EXCLUDED.name),
    status = COALESCE(NULLIF(TRIM(public.tenants.status), ''), EXCLUDED.status),
    plan = COALESCE(NULLIF(TRIM(public.tenants.plan), ''), EXCLUDED.plan);

ALTER TABLE public.bebidas ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);
ALTER TABLE public.pedidos ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);
ALTER TABLE public.itens_pedido ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);
ALTER TABLE public.turnos ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);
ALTER TABLE public.combo_items ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);
ALTER TABLE public.inventory_items ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);
ALTER TABLE public.shift_notes ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);
ALTER TABLE public.preorder_settings ADD COLUMN IF NOT EXISTS tenant_id BIGINT REFERENCES public.tenants(id);

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.bebidas
SET tenant_id = (SELECT id FROM default_tenant)
WHERE tenant_id IS NULL;

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.pedidos
SET tenant_id = (SELECT id FROM default_tenant)
WHERE tenant_id IS NULL;

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.itens_pedido ip
SET tenant_id = COALESCE(
    (SELECT p.tenant_id FROM public.pedidos p WHERE p.id = ip.pedido_id),
    (SELECT id FROM default_tenant)
)
WHERE tenant_id IS NULL;

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.turnos
SET tenant_id = (SELECT id FROM default_tenant)
WHERE tenant_id IS NULL;

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.combo_items
SET tenant_id = (SELECT id FROM default_tenant)
WHERE tenant_id IS NULL;

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.inventory_items
SET tenant_id = (SELECT id FROM default_tenant)
WHERE tenant_id IS NULL;

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.shift_notes
SET tenant_id = (SELECT id FROM default_tenant)
WHERE tenant_id IS NULL;

WITH default_tenant AS (
    SELECT id FROM public.tenants WHERE slug = 'default'
)
UPDATE public.preorder_settings
SET tenant_id = (SELECT id FROM default_tenant)
WHERE tenant_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_tenants_slug ON public.tenants(slug);
CREATE INDEX IF NOT EXISTS idx_bebidas_tenant_active ON public.bebidas(tenant_id, is_active, is_combo, nome);
CREATE INDEX IF NOT EXISTS idx_bebidas_tenant_id ON public.bebidas(tenant_id, id);
CREATE INDEX IF NOT EXISTS idx_pedidos_tenant_codigo ON public.pedidos(tenant_id, codigo_retirada);
CREATE INDEX IF NOT EXISTS idx_pedidos_tenant_request_id ON public.pedidos(tenant_id, request_id);
CREATE INDEX IF NOT EXISTS idx_pedidos_tenant_public_token ON public.pedidos(tenant_id, public_token);
CREATE INDEX IF NOT EXISTS idx_pedidos_tenant_shift_status ON public.pedidos(tenant_id, turno_id, status, horario_pedido DESC);
CREATE INDEX IF NOT EXISTS idx_turnos_tenant_status_id ON public.turnos(tenant_id, status, id DESC);
CREATE INDEX IF NOT EXISTS idx_itens_pedido_tenant_pedido ON public.itens_pedido(tenant_id, pedido_id);
CREATE INDEX IF NOT EXISTS idx_combo_items_tenant_combo ON public.combo_items(tenant_id, combo_beverage_id);
CREATE INDEX IF NOT EXISTS idx_inventory_items_tenant_name ON public.inventory_items(tenant_id, name);
CREATE INDEX IF NOT EXISTS idx_shift_notes_tenant_status ON public.shift_notes(tenant_id, status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_preorder_settings_tenant ON public.preorder_settings(tenant_id);

ALTER TABLE public.tenants ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE public.tenants FROM anon, authenticated;

COMMENT ON TABLE public.tenants IS
    'BarOS tenant registry. Legacy production data is assigned to the default tenant during the SaaS foundation migration.';

COMMIT;
