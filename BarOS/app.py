from __future__ import annotations

import csv
import json
import io
import os
import re
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from functools import wraps
from html import escape
from pathlib import Path

from flask import Flask, Response, g, jsonify, redirect, render_template, request, session, url_for
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from werkzeug.security import check_password_hash, generate_password_hash


BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
INSTANCE_DIR.mkdir(exist_ok=True)

# Render so preserva arquivos locais dentro do mount path de um Persistent Disk.
# Em servicos free do Render, o filesystem local e efemero: um SQLite salvo fora
# de um disco persistente pode sumir em restart ou redeploy.
# Para persistencia real, configure DATABASE_PATH apontando para um arquivo dentro
# do mount path do Persistent Disk do seu servico pago, por exemplo /var/data/baros.db.
#
# Compatibilidade: DATABASE_PATH tem prioridade. BAROS_DB_PATH continua aceito
# como fallback legado para nao quebrar ambientes ja configurados.
DATABASE_PATH = Path(
    os.getenv(
        "DATABASE_PATH",
        os.getenv("BAROS_DB_PATH", str(INSTANCE_DIR / "baros.db")),
    )
).expanduser()
BAR_USERNAME = os.getenv("BAROS_USERNAME", "admin")
BAR_PASSWORD = os.getenv("BAROS_PASSWORD", "bar123")
BAR_OPERATOR_USERNAME = os.getenv("BAROS_OPERATOR_USERNAME", "operacao")
BAR_OPERATOR_PASSWORD = os.getenv("BAROS_OPERATOR_PASSWORD", BAR_PASSWORD)
DEFAULT_SECRET = "troque-esta-chave-em-producao"
DEFAULT_ORDER_SOURCE = "menu-digital"
ROLE_LABELS = {
    "admin": "Administrador",
    "operator": "Operacao",
}
PAYMENT_METHOD_LABELS = {
    "counter": "Pagar no balcao",
    "pix": "Pix",
}
PAYMENT_STATUS_LABELS = {
    "pending": "Pendente",
    "paid": "Pago",
    "failed": "Falhou",
    "cancelled": "Cancelado",
}
ACTIVE_ORDER_STATUSES = ("new", "pending")

BEVERAGE_SEED = [
    {
        "nome": "Negroni House",
        "preco_venda": 34.0,
        "custo_estimado": 11.8,
        "category": "Autoral",
        "description": "Gin, vermute rosso e bitter italiano com zest de laranja.",
        "prep_time": "4 min",
    },
    {
        "nome": "Gin Tonica Citrica",
        "preco_venda": 29.0,
        "custo_estimado": 9.4,
        "category": "Classico",
        "description": "Gin seco, tonica premium, pepino e limao siciliano.",
        "prep_time": "3 min",
    },
    {
        "nome": "Moscow Mule",
        "preco_venda": 31.0,
        "custo_estimado": 10.2,
        "category": "Assinatura",
        "description": "Vodka, espuma de gengibre e limao fresco.",
        "prep_time": "5 min",
    },
    {
        "nome": "Old Fashioned Reserve",
        "preco_venda": 38.0,
        "custo_estimado": 13.7,
        "category": "Premium",
        "description": "Bourbon, angostura e acucar demerara defumado.",
        "prep_time": "6 min",
    },
    {
        "nome": "Spritz Rosato",
        "preco_venda": 28.0,
        "custo_estimado": 8.1,
        "category": "Leve",
        "description": "Aperitivo rosato, espumante brut e soda.",
        "prep_time": "3 min",
    },
    {
        "nome": "Lager Artesanal",
        "preco_venda": 18.0,
        "custo_estimado": 6.0,
        "category": "Chopp",
        "description": "Pint gelado da casa com final limpo e refrescante.",
        "prep_time": "2 min",
    },
]

BEVERAGE_META = {
    item["nome"]: {
        "category": item["category"],
        "description": item["description"],
        "prep_time": item["prep_time"],
    }
    for item in BEVERAGE_SEED
}

LOGISTICS_SEED = [
    {
        "name": "Gin London Dry",
        "category": "Destilados",
        "unit": "ml",
        "stock_level": 1800,
        "par_level": 2500,
        "status": "attention",
    },
    {
        "name": "Tonica Premium",
        "category": "Misturadores",
        "unit": "latas",
        "stock_level": 14,
        "par_level": 24,
        "status": "attention",
    },
    {
        "name": "Limao Siciliano",
        "category": "Pereciveis",
        "unit": "un",
        "stock_level": 8,
        "par_level": 20,
        "status": "critical",
    },
    {
        "name": "Campari",
        "category": "Destilados",
        "unit": "ml",
        "stock_level": 1200,
        "par_level": 1600,
        "status": "attention",
    },
    {
        "name": "Vermute Rosso",
        "category": "Destilados",
        "unit": "ml",
        "stock_level": 1500,
        "par_level": 1800,
        "status": "attention",
    },
    {
        "name": "Vodka",
        "category": "Destilados",
        "unit": "ml",
        "stock_level": 1700,
        "par_level": 1800,
        "status": "attention",
    },
    {
        "name": "Espuma de Gengibre",
        "category": "Misturadores",
        "unit": "ml",
        "stock_level": 900,
        "par_level": 1200,
        "status": "attention",
    },
    {
        "name": "Bourbon",
        "category": "Destilados",
        "unit": "ml",
        "stock_level": 1400,
        "par_level": 1500,
        "status": "attention",
    },
    {
        "name": "Acucar Demerara",
        "category": "Insumos",
        "unit": "doses",
        "stock_level": 80,
        "par_level": 90,
        "status": "attention",
    },
    {
        "name": "Aperitivo Rosato",
        "category": "Destilados",
        "unit": "ml",
        "stock_level": 1100,
        "par_level": 1400,
        "status": "attention",
    },
    {
        "name": "Espumante Brut",
        "category": "Misturadores",
        "unit": "ml",
        "stock_level": 1800,
        "par_level": 2000,
        "status": "attention",
    },
    {
        "name": "Soda",
        "category": "Misturadores",
        "unit": "ml",
        "stock_level": 1600,
        "par_level": 1600,
        "status": "ok",
    },
    {
        "name": "Chopp Lager",
        "category": "Chopp",
        "unit": "ml",
        "stock_level": 12000,
        "par_level": 14000,
        "status": "attention",
    },
]

BEVERAGE_RECIPES = {
    "Negroni House": {
        "Gin London Dry": 50,
        "Campari": 30,
        "Vermute Rosso": 30,
        "Limao Siciliano": 0.25,
    },
    "Gin Tonica Citrica": {
        "Gin London Dry": 50,
        "Tonica Premium": 1,
        "Limao Siciliano": 0.2,
    },
    "Moscow Mule": {
        "Vodka": 50,
        "Espuma de Gengibre": 120,
        "Limao Siciliano": 0.2,
    },
    "Old Fashioned Reserve": {
        "Bourbon": 60,
        "Acucar Demerara": 1,
    },
    "Spritz Rosato": {
        "Aperitivo Rosato": 60,
        "Espumante Brut": 90,
        "Soda": 30,
    },
    "Lager Artesanal": {
        "Chopp Lager": 473,
    },
}

SHIFT_NOTES_SEED = [
    {
        "title": "Reposicao de garnish",
        "body": "Separar laranja, pepino e hortela antes das 19h30.",
        "priority": "media",
        "status": "open",
    },
    {
        "title": "Conferencia de caixas",
        "body": "Revisar recebimento de tonica e cerveja artesanal no inicio do turno.",
        "priority": "alta",
        "status": "open",
    },
]

ENABLE_BOOTSTRAP_SEED = os.getenv("BAROS_ENABLE_BOOTSTRAP_SEED", "true").lower() == "true"


def normalize_internal_access_path(raw_value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", (raw_value or "").strip().lower()).strip("-")
    return cleaned or "backstage"


INTERNAL_ACCESS_PATH = normalize_internal_access_path(os.getenv("BAROS_INTERNAL_ACCESS_PATH", "backstage"))


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("BAROS_SECRET_KEY", DEFAULT_SECRET)
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.getenv("BAROS_COOKIE_SECURE", "false").lower() == "true"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def display_datetime(value: str | None) -> str:
    if not value:
        return "-"
    return datetime.fromisoformat(value).astimezone().strftime("%d/%m/%Y %H:%M")


def hour_bucket_label(value: str | None) -> str:
    if not value:
        return "-"
    return datetime.fromisoformat(value).astimezone().strftime("%Hh")


def duration_label(start_value: str | None, end_value: str | None) -> str:
    if not start_value or not end_value:
        return "-"
    start = datetime.fromisoformat(start_value)
    end = datetime.fromisoformat(end_value)
    total_minutes = max(0, int((end - start).total_seconds() // 60))
    hours, minutes = divmod(total_minutes, 60)
    if hours and minutes:
        return f"{hours}h {minutes}min"
    if hours:
        return f"{hours}h"
    return f"{minutes}min"


def peak_window_label(start_value: datetime | None) -> str:
    if not start_value:
        return "-"
    end_value = start_value + timedelta(hours=1)
    return f'{start_value.strftime("%d/%m %Hh")} - {end_value.strftime("%Hh")}'


def sanitize_text(value: str | None, fallback: str, limit: int = 40) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return fallback
    return normalized[:limit]


def currency_brl(value: float | int | None) -> str:
    return f"R$ {float(value or 0):.2f}"


def sanitize_optional_text(value: str | None, limit: int = 255) -> str:
    return (value or "").strip()[:limit]


def parse_decimal_input(raw_value: str | None, label: str, minimum: float = 0.0) -> float:
    cleaned = (raw_value or "").strip().replace(",", ".")
    try:
        value = float(cleaned)
    except (TypeError, ValueError):
        raise ValueError(f"{label} invalido.")
    if value < minimum:
        raise ValueError(f"{label} nao pode ser menor que {minimum}.")
    return round(value, 2)


def parse_integer_input(raw_value: str | None, label: str, minimum: int = 0) -> int:
    cleaned = (raw_value or "").strip()
    try:
        value = int(cleaned)
    except (TypeError, ValueError):
        raise ValueError(f"{label} invalido.")
    if value < minimum:
        raise ValueError(f"{label} nao pode ser menor que {minimum}.")
    return value


def checkbox_to_bool(raw_value: str | None) -> bool:
    return str(raw_value or "").strip().lower() in {"1", "true", "on", "yes"}


def normalize_product_image(value: str | None) -> str:
    cleaned = sanitize_optional_text(value, limit=255)
    if cleaned.startswith("static/"):
        return f"/{cleaned}"
    return cleaned


def build_initials(value: str | None) -> str:
    words = [chunk[:1] for chunk in re.findall(r"[A-Za-z0-9À-ÿ]+", value or "")]
    if not words:
        return "BO"
    return "".join(words[:2]).upper()


def load_summary_payload(raw_value: str | None) -> dict:
    if not raw_value:
        return {}
    try:
        data = json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def ensure_database_parent() -> None:
    database_parent = DATABASE_PATH.parent
    if database_parent and str(database_parent) not in {"", "."}:
        database_parent.mkdir(parents=True, exist_ok=True)


def open_db_connection() -> sqlite3.Connection:
    ensure_database_parent()
    connection = sqlite3.connect(DATABASE_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = open_db_connection()
    return g.db


@app.teardown_appcontext
def close_db(error=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


@app.after_request
def apply_response_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if request.path.startswith("/api/") or request.path == "/painel":
        response.headers["Cache-Control"] = "no-store"
    return response


def seed_user(db: sqlite3.Connection, username: str, password: str, role: str, display_name: str) -> None:
    if not username or not password:
        return

    existing = db.execute(
        "SELECT id FROM staff_users WHERE username = ?",
        (username,),
    ).fetchone()
    if existing:
        return

    password_hash = generate_password_hash(password)
    db.execute(
        """
        INSERT INTO staff_users (username, password_hash, role, display_name, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, 1, ?, ?)
        """,
        (username, password_hash, role, display_name, utc_now_iso(), utc_now_iso()),
    )


def init_db() -> None:
    db = open_db_connection()
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS bebidas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL UNIQUE,
            preco_venda REAL NOT NULL,
            custo_estimado REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pedidos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo_retirada TEXT NOT NULL UNIQUE,
            horario_pedido TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            valor_total REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS turnos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            aberto_em TEXT NOT NULL,
            fechado_em TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            resumo_fechamento TEXT
        );

        CREATE TABLE IF NOT EXISTS itens_pedido (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id INTEGER NOT NULL,
            bebida_id INTEGER NOT NULL,
            quantidade INTEGER NOT NULL,
            subtotal REAL NOT NULL,
            FOREIGN KEY(pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE,
            FOREIGN KEY(bebida_id) REFERENCES bebidas(id)
        );

        CREATE TABLE IF NOT EXISTS combo_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            combo_beverage_id INTEGER NOT NULL,
            component_beverage_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL DEFAULT 1,
            FOREIGN KEY(combo_beverage_id) REFERENCES bebidas(id) ON DELETE CASCADE,
            FOREIGN KEY(component_beverage_id) REFERENCES bebidas(id),
            UNIQUE(combo_beverage_id, component_beverage_id)
        );

        CREATE TABLE IF NOT EXISTS inventory_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            unit TEXT NOT NULL,
            stock_level REAL NOT NULL,
            par_level REAL NOT NULL,
            status TEXT NOT NULL DEFAULT 'ok',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS shift_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'media',
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS staff_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'operator',
            display_name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        """
    )

    beverage_columns = [row["name"] for row in db.execute("PRAGMA table_info(bebidas)").fetchall()]
    if "categoria" not in beverage_columns:
        db.execute("ALTER TABLE bebidas ADD COLUMN categoria TEXT")
    if "descricao" not in beverage_columns:
        db.execute("ALTER TABLE bebidas ADD COLUMN descricao TEXT")
    if "tempo_preparo" not in beverage_columns:
        db.execute("ALTER TABLE bebidas ADD COLUMN tempo_preparo TEXT")
    if "imagem_url" not in beverage_columns:
        db.execute("ALTER TABLE bebidas ADD COLUMN imagem_url TEXT")
    if "is_active" not in beverage_columns:
        db.execute("ALTER TABLE bebidas ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if "is_combo" not in beverage_columns:
        db.execute("ALTER TABLE bebidas ADD COLUMN is_combo INTEGER NOT NULL DEFAULT 0")

    pedido_columns = [row["name"] for row in db.execute("PRAGMA table_info(pedidos)").fetchall()]
    if "turno_id" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN turno_id INTEGER")
    if "customer_name" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN customer_name TEXT")
    if "table_label" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN table_label TEXT")
    if "source" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN source TEXT")
    if "completed_at" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN completed_at TEXT")
    if "completed_by_user_id" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN completed_by_user_id INTEGER")
    if "order_number" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN order_number TEXT")
    if "payment_method" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN payment_method TEXT")
    if "payment_status" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN payment_status TEXT")
    if "payment_provider" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN payment_provider TEXT")
    if "payment_provider_id" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN payment_provider_id TEXT")
    if "paid_at" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN paid_at TEXT")
    if "pix_qr_code" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN pix_qr_code TEXT")
    if "pix_copy_paste" not in pedido_columns:
        db.execute("ALTER TABLE pedidos ADD COLUMN pix_copy_paste TEXT")

    order_item_columns = [row["name"] for row in db.execute("PRAGMA table_info(itens_pedido)").fetchall()]
    if "item_name_snapshot" not in order_item_columns:
        db.execute("ALTER TABLE itens_pedido ADD COLUMN item_name_snapshot TEXT")
    if "item_type_snapshot" not in order_item_columns:
        db.execute("ALTER TABLE itens_pedido ADD COLUMN item_type_snapshot TEXT")
    if "unit_price_snapshot" not in order_item_columns:
        db.execute("ALTER TABLE itens_pedido ADD COLUMN unit_price_snapshot REAL")
    if "unit_cost_snapshot" not in order_item_columns:
        db.execute("ALTER TABLE itens_pedido ADD COLUMN unit_cost_snapshot REAL")

    beverages_total = db.execute("SELECT COUNT(*) AS total FROM bebidas").fetchone()["total"]
    inventory_total = db.execute("SELECT COUNT(*) AS total FROM inventory_items").fetchone()["total"]
    notes_total = db.execute("SELECT COUNT(*) AS total FROM shift_notes").fetchone()["total"]

    # Dados bootstrap so entram em banco vazio. Isso evita reintroduzir
    # dados de exemplo sobre uma base de producao ja operando.
    if ENABLE_BOOTSTRAP_SEED and beverages_total == 0:
        for beverage in BEVERAGE_SEED:
            db.execute(
                """
                INSERT INTO bebidas (
                    nome,
                    preco_venda,
                    custo_estimado,
                    categoria,
                    descricao,
                    tempo_preparo,
                    imagem_url,
                    is_active,
                    is_combo
                )
                VALUES (
                    :nome,
                    :preco_venda,
                    :custo_estimado,
                    :category,
                    :description,
                    :prep_time,
                    :image_url,
                    1,
                    0
                )
                """,
                {**beverage, "image_url": ""},
            )

    for beverage in BEVERAGE_SEED:
        exists = db.execute(
            "SELECT id, categoria, descricao, tempo_preparo, is_active, is_combo FROM bebidas WHERE nome = ?",
            (beverage["nome"],),
        ).fetchone()
        if not exists:
            continue
        db.execute(
            """
            UPDATE bebidas
            SET
                categoria = COALESCE(NULLIF(TRIM(categoria), ''), ?),
                descricao = COALESCE(NULLIF(TRIM(descricao), ''), ?),
                tempo_preparo = COALESCE(NULLIF(TRIM(tempo_preparo), ''), ?),
                is_active = COALESCE(is_active, 1),
                is_combo = COALESCE(is_combo, 0)
            WHERE id = ?
            """,
            (
                beverage["category"],
                beverage["description"],
                beverage["prep_time"],
                exists["id"],
            ),
        )

    if ENABLE_BOOTSTRAP_SEED and inventory_total == 0:
        for item in LOGISTICS_SEED:
            db.execute(
                """
                INSERT INTO inventory_items (name, category, unit, stock_level, par_level, status, updated_at)
                VALUES (:name, :category, :unit, :stock_level, :par_level, :status, :updated_at)
                """,
                {**item, "updated_at": utc_now_iso()},
            )

    if ENABLE_BOOTSTRAP_SEED and notes_total == 0:
        for note in SHIFT_NOTES_SEED:
            db.execute(
                """
                INSERT INTO shift_notes (title, body, priority, status, created_at)
                VALUES (:title, :body, :priority, :status, :created_at)
                """,
                {**note, "created_at": utc_now_iso()},
            )

    seed_user(db, BAR_USERNAME, BAR_PASSWORD, "admin", "Administrador")
    seed_user(db, BAR_OPERATOR_USERNAME, BAR_OPERATOR_PASSWORD, "operator", "Operacao")

    open_shift = db.execute("SELECT id FROM turnos WHERE status = 'open' ORDER BY id DESC LIMIT 1").fetchone()
    if not open_shift:
        cursor = db.execute(
            """
            INSERT INTO turnos (aberto_em, status)
            VALUES (?, 'open')
            """,
            (utc_now_iso(),),
        )
        open_shift_id = cursor.lastrowid
    else:
        open_shift_id = open_shift["id"]

    db.execute(
        "UPDATE pedidos SET turno_id = ? WHERE turno_id IS NULL",
        (open_shift_id,),
    )
    db.execute(
        "UPDATE pedidos SET customer_name = 'Cliente' WHERE customer_name IS NULL OR TRIM(customer_name) = ''"
    )
    db.execute(
        "UPDATE pedidos SET table_label = 'Retirada' WHERE table_label IS NULL OR TRIM(table_label) = ''"
    )
    db.execute(
        "UPDATE pedidos SET source = ? WHERE source IS NULL OR TRIM(source) = ''",
        (DEFAULT_ORDER_SOURCE,),
    )
    db.execute(
        "UPDATE pedidos SET status = 'new' WHERE status IS NULL OR status = '' OR status = 'pending'"
    )
    db.execute(
        "UPDATE pedidos SET payment_method = 'counter' WHERE payment_method IS NULL OR TRIM(payment_method) = ''"
    )
    db.execute(
        "UPDATE pedidos SET payment_status = 'pending' WHERE payment_status IS NULL OR TRIM(payment_status) = ''"
    )
    db.execute(
        """
        UPDATE pedidos
        SET order_number = printf('%02d-%04d', COALESCE(turno_id, 0), id)
        WHERE order_number IS NULL OR TRIM(order_number) = ''
        """
    )
    db.execute(
        """
        UPDATE bebidas
        SET categoria = COALESCE(NULLIF(TRIM(categoria), ''), 'Bebida'),
            descricao = COALESCE(descricao, ''),
            tempo_preparo = COALESCE(NULLIF(TRIM(tempo_preparo), ''), '3 min'),
            imagem_url = COALESCE(imagem_url, ''),
            is_active = COALESCE(is_active, 1),
            is_combo = COALESCE(is_combo, 0)
        """
    )
    db.execute(
        """
        UPDATE itens_pedido
        SET item_name_snapshot = (
            SELECT bebidas.nome
            FROM bebidas
            WHERE bebidas.id = itens_pedido.bebida_id
        )
        WHERE item_name_snapshot IS NULL OR TRIM(item_name_snapshot) = ''
        """
    )
    db.execute(
        """
        UPDATE itens_pedido
        SET item_type_snapshot = (
            CASE
                WHEN (
                    SELECT bebidas.is_combo
                    FROM bebidas
                    WHERE bebidas.id = itens_pedido.bebida_id
                ) = 1 THEN 'combo'
                ELSE 'product'
            END
        )
        WHERE item_type_snapshot IS NULL OR TRIM(item_type_snapshot) = ''
        """
    )
    db.execute(
        """
        UPDATE itens_pedido
        SET unit_price_snapshot = ROUND(
            subtotal / CASE WHEN quantidade > 0 THEN quantidade ELSE 1 END,
            2
        )
        WHERE unit_price_snapshot IS NULL
        """
    )
    db.execute(
        """
        UPDATE itens_pedido
        SET unit_cost_snapshot = (
            SELECT bebidas.custo_estimado
            FROM bebidas
            WHERE bebidas.id = itens_pedido.bebida_id
        )
        WHERE unit_cost_snapshot IS NULL
        """
    )

    db.commit()
    db.close()


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not session.get("bar_authenticated"):
            return redirect(url_for("staff_access"))
        return view(*args, **kwargs)

    return wrapped_view


def role_required(*allowed_roles: str):
    def decorator(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            if not session.get("bar_authenticated"):
                return redirect(url_for("staff_access"))
            if session.get("bar_role") not in allowed_roles:
                if request.path.startswith("/api/"):
                    return jsonify({"error": "Voce nao tem permissao para esta acao."}), 403
                return redirect(url_for("dashboard"))
            return view(*args, **kwargs)

        return wrapped_view

    return decorator


def get_current_user() -> dict:
    role = session.get("bar_role", "operator")
    return {
        "id": session.get("bar_user_id"),
        "display_name": session.get("bar_display_name", "Equipe"),
        "role": role,
        "role_label": ROLE_LABELS.get(role, role.title()),
        "can_manage_bar": role == "admin",
    }


def generate_order_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    db = get_db()
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(4))
        exists = db.execute(
            "SELECT 1 FROM pedidos WHERE codigo_retirada = ?",
            (code,),
        ).fetchone()
        if not exists:
            return code


def generate_order_number(order_id: int, shift_id: int) -> str:
    return f"{shift_id:02d}-{order_id:04d}"


def normalize_payment_method(raw_value: str | None) -> str:
    value = sanitize_text(raw_value, "counter", limit=16).lower()
    return value if value in PAYMENT_METHOD_LABELS else "counter"


def normalize_payment_status(raw_value: str | None, fallback: str = "pending") -> str:
    value = sanitize_text(raw_value, fallback, limit=16).lower()
    return value if value in PAYMENT_STATUS_LABELS else fallback


def get_payment_method_label(method: str | None) -> str:
    return PAYMENT_METHOD_LABELS.get(method or "", "Pagar no balcao")


def get_payment_status_label(status: str | None) -> str:
    return PAYMENT_STATUS_LABELS.get(status or "", "Pendente")


def fetch_order_row_by_code(code: str, shift_id: int | None = None) -> sqlite3.Row | None:
    query = """
        SELECT
            id,
            codigo_retirada,
            horario_pedido,
            status,
            valor_total,
            customer_name,
            table_label,
            source,
            completed_at,
            order_number,
            payment_method,
            payment_status,
            payment_provider,
            payment_provider_id,
            paid_at,
            pix_qr_code,
            pix_copy_paste
        FROM pedidos
        WHERE codigo_retirada = ?
    """
    params: list = [code]
    if shift_id is not None:
        query += " AND turno_id = ?"
        params.append(shift_id)
    return get_db().execute(query, params).fetchone()


def mark_as_paid(code: str, shift_id: int | None = None) -> dict:
    db = get_db()
    current_shift_id = shift_id or get_current_shift_id()
    row = fetch_order_row_by_code(code, current_shift_id)
    if not row:
        raise LookupError("Pedido nao encontrado.")

    if normalize_payment_status(row["payment_status"], "pending") == "paid":
        return serialize_order(row)

    paid_at = utc_now_iso()
    db.execute(
        """
        UPDATE pedidos
        SET payment_status = 'paid',
            paid_at = ?,
            status = CASE
                WHEN payment_method = 'pix' AND status = 'pending' THEN 'new'
                ELSE status
            END
        WHERE id = ?
        """,
        (paid_at, row["id"]),
    )
    db.commit()
    updated = fetch_order_row_by_code(code, current_shift_id)
    if not updated:
        raise LookupError("Pedido nao encontrado.")
    return serialize_order(updated)


def build_ticket(order: dict) -> dict:
    return {
        "id": order["id"],
        "code": order["code"],
        "order_number": order["order_number"],
        "customer_name": order["customer_name"],
        "table_label": order["table_label"],
        "created_at": order["created_at"],
        "payment_method": order["payment_method"],
        "payment_method_label": order["payment_method_label"],
        "payment_status": order["payment_status"],
        "payment_status_label": order["payment_status_label"],
        "paid_at": order["paid_at"],
        "total": order["total"],
        "items": order["items"],
        "printed_at": display_datetime(utc_now_iso()),
    }


def get_current_shift_id() -> int:
    db = get_db()
    row = db.execute(
        "SELECT id FROM turnos WHERE status = 'open' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row:
        return row["id"]

    cursor = db.execute(
        """
        INSERT INTO turnos (aberto_em, status)
        VALUES (?, 'open')
        """,
        (utc_now_iso(),),
    )
    db.commit()
    return cursor.lastrowid


def open_new_shift() -> int:
    db = get_db()
    cursor = db.execute(
        """
        INSERT INTO turnos (aberto_em, status)
        VALUES (?, 'open')
        """,
        (utc_now_iso(),),
    )
    db.execute("UPDATE shift_notes SET status = 'open'")
    db.commit()
    return cursor.lastrowid


def calculate_inventory_status(stock_level: float, par_level: float) -> str:
    if stock_level <= 0:
        return "critical"
    if stock_level < (par_level * 0.4):
        return "critical"
    if stock_level < par_level:
        return "attention"
    return "ok"


def get_inventory_by_name() -> dict[str, sqlite3.Row]:
    rows = get_db().execute(
        "SELECT name, stock_level, unit FROM inventory_items"
    ).fetchall()
    return {row["name"]: row for row in rows}


def fetch_beverage_rows(include_inactive: bool = True) -> list[sqlite3.Row]:
    query = """
        SELECT
            id,
            nome,
            preco_venda,
            custo_estimado,
            categoria,
            descricao,
            tempo_preparo,
            imagem_url,
            is_active,
            is_combo
        FROM bebidas
    """
    params: list = []
    if not include_inactive:
        query += " WHERE is_active = 1"
    query += " ORDER BY is_combo ASC, nome ASC"
    return get_db().execute(query, params).fetchall()


def fetch_beverage_map(include_inactive: bool = True) -> dict[int, sqlite3.Row]:
    return {row["id"]: row for row in fetch_beverage_rows(include_inactive=include_inactive)}


def fetch_combo_components_map(combo_ids: list[int] | None = None) -> dict[int, list[dict]]:
    if combo_ids is not None and not combo_ids:
        return {}

    query = """
        SELECT
            ci.combo_beverage_id,
            ci.component_beverage_id,
            ci.quantity,
            b.nome AS component_name,
            b.custo_estimado AS component_cost,
            b.is_active AS component_active,
            b.is_combo AS component_is_combo
        FROM combo_items ci
        JOIN bebidas b ON b.id = ci.component_beverage_id
    """
    params: list = []
    if combo_ids:
        query += " WHERE ci.combo_beverage_id IN ({})".format(",".join("?" for _ in combo_ids))
        params.extend(combo_ids)
    query += " ORDER BY ci.combo_beverage_id ASC, b.nome ASC"
    rows = get_db().execute(query, params).fetchall()
    components: dict[int, list[dict]] = {}
    for row in rows:
        components.setdefault(row["combo_beverage_id"], []).append(
            {
                "component_beverage_id": row["component_beverage_id"],
                "quantity": int(row["quantity"] or 0),
                "name": row["component_name"],
                "cost": float(row["component_cost"] or 0),
                "is_active": bool(row["component_active"]),
                "is_combo": bool(row["component_is_combo"]),
            }
        )
    return components


def get_beverage_display_data(row: sqlite3.Row | dict) -> dict:
    row_data = dict(row)
    fallback = BEVERAGE_META.get(row["nome"], {})
    category = row_data["categoria"] if row_data["categoria"] else fallback.get("category", "Bebida")
    if row_data["is_combo"]:
        category = category or "Combo"
    prep_time = row_data["tempo_preparo"] if row_data["tempo_preparo"] else fallback.get("prep_time", "3 min")
    if row_data["is_combo"] and not prep_time:
        prep_time = "4 min"
    description = row_data["descricao"] if row_data["descricao"] else fallback.get("description", "")
    image_url = normalize_product_image(row_data["imagem_url"])
    return {
        "id": row_data["id"],
        "name": row_data["nome"],
        "price": float(row_data["preco_venda"] or 0),
        "cost": float(row_data["custo_estimado"] or 0),
        "category": category or ("Combo" if row_data["is_combo"] else "Bebida"),
        "description": description or "Item cadastrado no sistema.",
        "prep_time": prep_time or "3 min",
        "image_url": row_data["imagem_url"] or "",
        "image_src": image_url,
        "placeholder": build_initials(row_data["nome"]),
        "is_active": bool(row_data["is_active"]),
        "is_combo": bool(row_data["is_combo"]),
    }


def build_item_requirements(
    beverage_row: sqlite3.Row | dict,
    quantity: int,
    beverages_by_id: dict[int, sqlite3.Row],
    combo_components_map: dict[int, list[dict]],
    *,
    stack: set[int] | None = None,
) -> tuple[dict[str, float], str | None]:
    stack = stack or set()
    beverage_id = int(beverage_row["id"])
    if beverage_id in stack:
        return {}, f"Composicao circular detectada em {beverage_row['nome']}."

    if not beverage_row["is_active"]:
        return {}, f"{beverage_row['nome']} esta inativo no cardapio."

    if beverage_row["is_combo"]:
        components = combo_components_map.get(beverage_id, [])
        if sum(component["quantity"] for component in components) < 2:
            return {}, f"Combo {beverage_row['nome']} ainda nao possui composicao valida."

        required: dict[str, float] = {}
        for component in components:
            component_row = beverages_by_id.get(component["component_beverage_id"])
            if not component_row:
                return {}, f"Componente do combo {beverage_row['nome']} nao encontrado."
            if component_row["is_combo"]:
                return {}, f"Combo {beverage_row['nome']} nao pode conter outro combo."
            partial_requirements, error = build_item_requirements(
                component_row,
                quantity * component["quantity"],
                beverages_by_id,
                combo_components_map,
                stack=stack | {beverage_id},
            )
            if error:
                return {}, error
            for ingredient_name, amount in partial_requirements.items():
                required[ingredient_name] = required.get(ingredient_name, 0) + amount
        return required, None

    recipe = BEVERAGE_RECIPES.get(beverage_row["nome"], {})
    required = {
        ingredient_name: amount * quantity
        for ingredient_name, amount in recipe.items()
    }
    return required, None


def beverage_availability(
    beverage_row: sqlite3.Row | dict,
    inventory_by_name: dict[str, sqlite3.Row],
    beverages_by_id: dict[int, sqlite3.Row],
    combo_components_map: dict[int, list[dict]],
) -> tuple[bool, str | None]:
    if not beverage_row["is_active"]:
        return False, "Item inativo no cardapio."

    required, error = build_item_requirements(
        beverage_row,
        1,
        beverages_by_id,
        combo_components_map,
    )
    if error:
        return False, f"Indisponivel: {error}"
    if not required:
        return True, None

    for ingredient_name, amount in required.items():
        inventory_item = inventory_by_name.get(ingredient_name)
        if not inventory_item:
            return False, f"Indisponivel: {ingredient_name} nao cadastrado."
        if inventory_item["stock_level"] < amount:
            return False, f"Indisponivel por estoque de {ingredient_name}."

    return True, None


def build_required_ingredients(
    items: list[dict],
    beverages_by_id: dict[int, sqlite3.Row] | None = None,
    combo_components_map: dict[int, list[dict]] | None = None,
) -> tuple[dict[str, float], str | None]:
    catalog = beverages_by_id or fetch_beverage_map(include_inactive=True)
    combos = combo_components_map or fetch_combo_components_map()
    required: dict[str, float] = {}

    for item in items:
        beverage_row = catalog.get(item["bebida_id"])
        if not beverage_row:
            return {}, f"Item {item.get('name', 'desconhecido')} nao encontrado."
        partial_requirements, error = build_item_requirements(
            beverage_row,
            int(item["quantity"]),
            catalog,
            combos,
        )
        if error:
            return {}, error
        for ingredient_name, amount in partial_requirements.items():
            required[ingredient_name] = required.get(ingredient_name, 0) + amount

    return required, None


def apply_stock_deductions(
    items: list[dict],
    beverages_by_id: dict[int, sqlite3.Row] | None = None,
    combo_components_map: dict[int, list[dict]] | None = None,
) -> None:
    db = get_db()
    recipes_to_apply, error = build_required_ingredients(items, beverages_by_id, combo_components_map)
    if error or not recipes_to_apply:
        return

    inventory_rows = db.execute(
        "SELECT id, name, stock_level, par_level FROM inventory_items WHERE name IN ({})".format(
            ",".join("?" for _ in recipes_to_apply)
        ),
        list(recipes_to_apply.keys()),
    ).fetchall()

    for row in inventory_rows:
        deducted_amount = recipes_to_apply[row["name"]]
        next_stock = max(0, row["stock_level"] - deducted_amount)
        next_status = calculate_inventory_status(next_stock, row["par_level"])
        db.execute(
            """
            UPDATE inventory_items
            SET stock_level = ?, status = ?, updated_at = ?
            WHERE id = ?
            """,
            (next_stock, next_status, utc_now_iso(), row["id"]),
        )


def check_stock_availability(
    items: list[dict],
    beverages_by_id: dict[int, sqlite3.Row] | None = None,
    combo_components_map: dict[int, list[dict]] | None = None,
) -> list[dict]:
    db = get_db()
    required, error = build_required_ingredients(items, beverages_by_id, combo_components_map)
    if error:
        return [{"item": error, "needed": 0, "available": 0, "unit": ""}]
    if not required:
        return []

    rows = db.execute(
        "SELECT name, stock_level, unit FROM inventory_items WHERE name IN ({})".format(
            ",".join("?" for _ in required)
        ),
        list(required.keys()),
    ).fetchall()
    by_name = {row["name"]: row for row in rows}

    shortages = []
    for ingredient_name, needed in required.items():
        row = by_name.get(ingredient_name)
        if not row:
            shortages.append(
                {
                    "item": ingredient_name,
                    "needed": needed,
                    "available": 0,
                    "unit": "",
                }
            )
            continue
        if row["stock_level"] < needed:
            shortages.append(
                {
                    "item": ingredient_name,
                    "needed": needed,
                    "available": row["stock_level"],
                    "unit": row["unit"],
                }
            )

    return shortages


def fetch_menu() -> list[dict]:
    inventory_by_name = get_inventory_by_name()
    rows = fetch_beverage_rows(include_inactive=False)
    beverages_by_id = fetch_beverage_map(include_inactive=True)
    combo_components_map = fetch_combo_components_map([row["id"] for row in rows if row["is_combo"]])
    menu = []
    for row in rows:
        menu_item = get_beverage_display_data(row)
        is_available, availability_note = beverage_availability(
            row,
            inventory_by_name,
            beverages_by_id,
            combo_components_map,
        )
        menu_item.update(
            {
                "is_available": is_available,
                "availability_note": availability_note,
            }
        )
        menu.append(menu_item)
    return menu


def calculate_combo_cost_estimate(
    selected_components: list[dict],
    beverages_by_id: dict[int, sqlite3.Row] | None = None,
) -> float:
    catalog = beverages_by_id or fetch_beverage_map(include_inactive=True)
    return round(
        sum(
            float((catalog.get(component["component_beverage_id"]) or {"custo_estimado": 0})["custo_estimado"] or 0)
            * int(component["quantity"])
            for component in selected_components
        ),
        2,
    )


def refresh_combo_costs_for_component(component_beverage_id: int) -> None:
    db = get_db()
    combo_ids = db.execute(
        """
        SELECT DISTINCT combo_beverage_id
        FROM combo_items
        WHERE component_beverage_id = ?
        """,
        (component_beverage_id,),
    ).fetchall()
    if not combo_ids:
        return

    catalog = fetch_beverage_map(include_inactive=True)
    combo_components_map = fetch_combo_components_map([row["combo_beverage_id"] for row in combo_ids])
    for row in combo_ids:
        combo_id = row["combo_beverage_id"]
        estimated_cost = calculate_combo_cost_estimate(combo_components_map.get(combo_id, []), catalog)
        db.execute(
            "UPDATE bebidas SET custo_estimado = ? WHERE id = ?",
            (estimated_cost, combo_id),
        )


def fetch_products_management_snapshot() -> dict:
    rows = fetch_beverage_rows(include_inactive=True)
    combo_components_map = fetch_combo_components_map([row["id"] for row in rows if row["is_combo"]])
    products = []
    combos = []
    base_products = []

    for row in rows:
        product = get_beverage_display_data(row)
        product["raw_description"] = row["descricao"] or ""
        product["raw_image_url"] = row["imagem_url"] or ""
        product["component_count"] = 0
        product["components"] = []
        if row["is_combo"]:
            components = combo_components_map.get(row["id"], [])
            product["components"] = components
            product["component_count"] = sum(component["quantity"] for component in components)
            combos.append(product)
        else:
            products.append(product)
            base_products.append(product)

    return {
        "products": products,
        "combos": combos,
        "component_options": base_products,
    }


def build_products_redirect(message: str | None = None, error: str | None = None):
    params = {}
    if message:
        params["message"] = message
    if error:
        params["error"] = error
    return redirect(url_for("products_page", **params))


def parse_combo_components_from_form(selected_ids: list[str]) -> list[dict]:
    component_ids: list[int] = []
    for raw_id in selected_ids:
        try:
            component_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if component_id > 0 and component_id not in component_ids:
            component_ids.append(component_id)

    components = []
    total_units = 0
    for component_id in component_ids:
        quantity = parse_integer_input(
            request.form.get(f"component_quantity_{component_id}"),
            "Quantidade do componente",
            minimum=1,
        )
        components.append(
            {
                "component_beverage_id": component_id,
                "quantity": quantity,
            }
        )
        total_units += quantity

    if total_units < 2:
        raise ValueError("O combo precisa ter pelo menos 2 itens na composicao.")

    return components


def serialize_order(row: sqlite3.Row) -> dict:
    items = get_db().execute(
        """
        SELECT
            ip.id,
            ip.quantidade,
            ip.subtotal,
            ip.item_name_snapshot,
            ip.item_type_snapshot,
            ip.unit_price_snapshot,
            b.id AS bebida_id,
            b.nome,
            b.preco_venda,
            b.is_combo
        FROM itens_pedido ip
        JOIN bebidas b ON b.id = ip.bebida_id
        WHERE ip.pedido_id = ?
        ORDER BY ip.id ASC
        """,
        (row["id"],),
    ).fetchall()

    return {
        "id": row["id"],
        "code": row["codigo_retirada"],
        "order_number": row["order_number"] if "order_number" in row.keys() and row["order_number"] else row["codigo_retirada"],
        "status": row["status"],
        "created_at": display_datetime(row["horario_pedido"]),
        "completed_at": display_datetime(row["completed_at"]) if "completed_at" in row.keys() and row["completed_at"] else None,
        "paid_at": display_datetime(row["paid_at"]) if "paid_at" in row.keys() and row["paid_at"] else None,
        "total": row["valor_total"],
        "customer_name": row["customer_name"] if "customer_name" in row.keys() and row["customer_name"] else "Cliente",
        "table_label": row["table_label"] if "table_label" in row.keys() and row["table_label"] else "Retirada",
        "source": row["source"] if "source" in row.keys() and row["source"] else DEFAULT_ORDER_SOURCE,
        "payment_method": row["payment_method"] if "payment_method" in row.keys() and row["payment_method"] else "counter",
        "payment_method_label": get_payment_method_label(
            row["payment_method"] if "payment_method" in row.keys() else "counter"
        ),
        "payment_status": row["payment_status"] if "payment_status" in row.keys() and row["payment_status"] else "pending",
        "payment_status_label": get_payment_status_label(
            row["payment_status"] if "payment_status" in row.keys() else "pending"
        ),
        "payment_provider": row["payment_provider"] if "payment_provider" in row.keys() and row["payment_provider"] else None,
        "payment_provider_id": row["payment_provider_id"] if "payment_provider_id" in row.keys() and row["payment_provider_id"] else None,
        "pix_qr_code": row["pix_qr_code"] if "pix_qr_code" in row.keys() and row["pix_qr_code"] else None,
        "pix_copy_paste": row["pix_copy_paste"] if "pix_copy_paste" in row.keys() and row["pix_copy_paste"] else None,
        "items": [
            {
                "id": item["bebida_id"],
                "name": item["item_name_snapshot"] or item["nome"],
                "quantity": item["quantidade"],
                "price": item["unit_price_snapshot"] if item["unit_price_snapshot"] is not None else item["preco_venda"],
                "subtotal": item["subtotal"],
                "item_type": item["item_type_snapshot"] or ("combo" if item["is_combo"] else "product"),
            }
            for item in items
        ],
    }


def fetch_orders(status: str | None = None, limit: int | None = None, shift_id: int | None = None) -> list[dict]:
    current_shift_id = shift_id or get_current_shift_id()
    query = """
        SELECT
            id,
            codigo_retirada,
            horario_pedido,
            status,
            valor_total,
            customer_name,
            table_label,
            source,
            completed_at,
            order_number,
            payment_method,
            payment_status,
            payment_provider,
            payment_provider_id,
            paid_at,
            pix_qr_code,
            pix_copy_paste
        FROM pedidos
    """
    params: list = []
    conditions = ["turno_id = ?"]
    params.append(current_shift_id)
    if status:
        if status == "pending":
            conditions.append("status IN (?, ?)")
            params.extend(ACTIVE_ORDER_STATUSES)
        else:
            conditions.append("status = ?")
            params.append(status)
    query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY horario_pedido DESC"
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    rows = get_db().execute(query, params).fetchall()
    return [serialize_order(row) for row in rows]


def validate_order_payload(selected_items: list[dict]) -> tuple[list[dict], str | None]:
    if not isinstance(selected_items, list):
        return [], "Formato de itens invalido."
    if not selected_items:
        return [], "Nenhum item enviado."

    normalized_items = []
    total_quantity = 0
    for entry in selected_items:
        if not isinstance(entry, dict):
            return [], "Formato de item invalido."
        try:
            bebida_id = int(entry.get("id", 0))
            quantidade = int(entry.get("quantity", 0))
        except (TypeError, ValueError):
            return [], "Item com quantidade invalida."
        if bebida_id <= 0 or quantidade <= 0:
            continue
        if quantidade > 24:
            return [], "Quantidade por bebida acima do limite permitido."
        total_quantity += quantidade
        normalized_items.append({"id": bebida_id, "quantity": quantidade})

    if total_quantity > 40:
        return [], "Pedido acima do limite permitido."
    if not normalized_items:
        return [], "Itens invalidos."
    return normalized_items, None


def fetch_logistics_snapshot() -> dict:
    db = get_db()
    items = db.execute(
        """
        SELECT id, name, category, unit, stock_level, par_level, status, updated_at
        FROM inventory_items
        ORDER BY
            CASE status
                WHEN 'critical' THEN 0
                WHEN 'attention' THEN 1
                ELSE 2
            END,
            name ASC
        """
    ).fetchall()
    notes = db.execute(
        """
        SELECT id, title, body, priority, status, created_at
        FROM shift_notes
        ORDER BY
            CASE priority
                WHEN 'alta' THEN 0
                WHEN 'media' THEN 1
                ELSE 2
            END,
            created_at DESC
        LIMIT 6
        """
    ).fetchall()
    return {
        "inventory": [
            {
                "id": row["id"],
                "name": row["name"],
                "category": row["category"],
                "unit": row["unit"],
                "stock_level": row["stock_level"],
                "par_level": row["par_level"],
                "status": row["status"],
                "updated_at": display_datetime(row["updated_at"]),
            }
            for row in items
        ],
        "notes": [
            {
                "id": row["id"],
                "title": row["title"],
                "body": row["body"],
                "priority": row["priority"],
                "status": row["status"],
                "created_at": display_datetime(row["created_at"]),
            }
            for row in notes
        ],
        "inventory_summary": {
            "critical_count": sum(1 for row in items if row["status"] == "critical"),
            "attention_count": sum(1 for row in items if row["status"] == "attention"),
            "tracked_count": len(items),
        },
    }


def parse_shift_observations(summary_data: dict | None) -> list[str]:
    if not isinstance(summary_data, dict):
        return []

    candidates = [
        summary_data.get("observacoes"),
        summary_data.get("observations"),
        summary_data.get("observacoes_turno"),
        summary_data.get("shift_notes"),
    ]
    for candidate in candidates:
        if isinstance(candidate, str):
            cleaned = [line.strip() for line in candidate.splitlines() if line.strip()]
            if cleaned:
                return cleaned
        if isinstance(candidate, list):
            cleaned = [str(item).strip() for item in candidate if str(item).strip()]
            if cleaned:
                return cleaned
    return []


def fetch_shift_orders_rows(shift_id: int) -> list[sqlite3.Row]:
    return get_db().execute(
        """
        SELECT
            id,
            codigo_retirada,
            order_number,
            horario_pedido,
            status,
            valor_total,
            customer_name,
            table_label,
            source,
            completed_at,
            payment_method,
            payment_status,
            payment_provider,
            payment_provider_id,
            paid_at,
            pix_qr_code,
            pix_copy_paste
        FROM pedidos
        WHERE turno_id = ?
        ORDER BY horario_pedido DESC
        """,
        (shift_id,),
    ).fetchall()


def build_peak_window_metrics(order_rows: list[sqlite3.Row]) -> dict | None:
    if not order_rows:
        return None

    buckets: dict[datetime, dict[str, float]] = {}
    for row in order_rows:
        local_dt = datetime.fromisoformat(row["horario_pedido"]).astimezone()
        bucket_start = local_dt.replace(minute=0, second=0, microsecond=0)
        bucket = buckets.setdefault(bucket_start, {"orders": 0, "revenue": 0.0})
        bucket["orders"] += 1
        bucket["revenue"] += float(row["valor_total"] or 0)

    peak_start, peak_data = sorted(
        buckets.items(),
        key=lambda item: (-item[1]["orders"], item[0]),
    )[0]

    return {
        "label": peak_window_label(peak_start),
        "hour": peak_window_label(peak_start),
        "start_iso": peak_start.isoformat(),
        "orders": int(peak_data["orders"]),
        "order_count": int(peak_data["orders"]),
        "revenue": round(peak_data["revenue"], 2),
    }


def build_change_indicator(delta_value: float, tolerance: float = 0.01) -> tuple[str, str]:
    if abs(delta_value) < tolerance:
        return "sem mudanca relevante", "neutral"
    if delta_value > 0:
        return "aumentou", "positive"
    return "caiu", "negative"


def build_numeric_metric_comparison(
    label: str,
    current_value: float,
    compared_value: float,
    *,
    value_type: str = "number",
    percentage: bool = True,
    tolerance: float = 0.01,
) -> dict:
    current_number = float(current_value or 0)
    compared_number = float(compared_value or 0)
    delta_absolute = round(current_number - compared_number, 2)
    indicator, tone = build_change_indicator(delta_absolute, tolerance=tolerance)
    delta_percentage = None
    if percentage and compared_number != 0:
        delta_percentage = round((delta_absolute / compared_number) * 100, 1)

    return {
        "label": label,
        "value_type": value_type,
        "current": current_number,
        "compared": compared_number,
        "delta_absolute": delta_absolute,
        "delta_percentage": delta_percentage,
        "indicator": indicator,
        "tone": tone,
    }


def build_text_metric_comparison(label: str, current_value: str, compared_value: str) -> dict:
    normalized_current = current_value or "Sem dados"
    normalized_compared = compared_value or "Sem dados"
    indicator = "sem mudanca relevante" if normalized_current == normalized_compared else "mudou"
    tone = "neutral"
    return {
        "label": label,
        "value_type": "text",
        "current": normalized_current,
        "compared": normalized_compared,
        "delta_absolute": None,
        "delta_percentage": None,
        "indicator": indicator,
        "tone": tone,
    }


def build_shift_metrics(shift_id: int) -> dict:
    db = get_db()
    totals = db.execute(
        """
        SELECT
            COUNT(*) AS total_pedidos,
            COALESCE(SUM(valor_total), 0) AS total_vendido
        FROM pedidos
        WHERE turno_id = ?
        """,
        (shift_id,),
    ).fetchone()

    ranking_rows = db.execute(
        """
        SELECT
            b.id AS beverage_id,
            COALESCE(ip.item_name_snapshot, b.nome) AS nome,
            SUM(ip.quantidade) AS quantidade,
            COALESCE(SUM(ip.subtotal), 0) AS total_vendido,
            COALESCE(SUM(COALESCE(ip.unit_cost_snapshot, b.custo_estimado) * ip.quantidade), 0) AS custo_estimado
        FROM itens_pedido ip
        JOIN bebidas b ON b.id = ip.bebida_id
        JOIN pedidos p ON p.id = ip.pedido_id
        WHERE p.turno_id = ?
        GROUP BY b.id, COALESCE(ip.item_name_snapshot, b.nome)
        ORDER BY quantidade DESC, nome ASC
        """,
        (shift_id,),
    ).fetchall()

    shift_row = db.execute(
        """
        SELECT id, aberto_em, fechado_em, resumo_fechamento
        FROM turnos
        WHERE id = ?
        """,
        (shift_id,),
    ).fetchone()
    stored_summary = load_summary_payload(shift_row["resumo_fechamento"]) if shift_row else {}

    order_rows = fetch_shift_orders_rows(shift_id)
    peak_window = build_peak_window_metrics(order_rows)

    total_vendido = round(float(totals["total_vendido"] or 0), 2)
    total_pedidos = int(totals["total_pedidos"] or 0)
    custo_estimado = round(
        sum(float(row["custo_estimado"] or 0) for row in ranking_rows),
        2,
    )
    ticket_medio = round(total_vendido / total_pedidos, 2) if total_pedidos else 0.0

    ranking_bebidas = [
        {
            "id": row["beverage_id"],
            "name": row["nome"],
            "quantity": int(row["quantidade"] or 0),
            "revenue": round(float(row["total_vendido"] or 0), 2),
            "cost": round(float(row["custo_estimado"] or 0), 2),
        }
        for row in ranking_rows
    ]
    bebida_mais_vendida = ranking_bebidas[0] if ranking_bebidas else None

    return {
        "total_vendido": total_vendido,
        "total_pedidos": total_pedidos,
        "ticket_medio": ticket_medio,
        "total_itens_vendidos": sum(item["quantity"] for item in ranking_bebidas),
        "bebida_mais_vendida": bebida_mais_vendida,
        "bebida_mais_pedida": bebida_mais_vendida,
        "top_5_bebidas": ranking_bebidas[:5],
        "ranking_bebidas": ranking_bebidas,
        "quantidade_por_bebida": [
            {"name": item["name"], "quantity": item["quantity"]}
            for item in ranking_bebidas
        ],
        "horario_pico": peak_window,
        "pico_atendimento": peak_window,
        "quantidade_pedidos_pico": peak_window["order_count"] if peak_window else 0,
        "valor_vendido_pico": peak_window["revenue"] if peak_window else 0.0,
        "custo_estimado": custo_estimado,
        "custo_total": custo_estimado,
        "lucro_estimado": round(total_vendido - custo_estimado, 2),
        "observacoes": parse_shift_observations(stored_summary),
    }


def fetch_comparable_shift_choices(current_shift_id: int, limit: int = 30) -> list[dict]:
    rows = get_db().execute(
        """
        SELECT id, aberto_em, fechado_em
        FROM turnos
        WHERE status = 'closed' AND id != ?
        ORDER BY id DESC
        LIMIT ?
        """,
        (current_shift_id, limit),
    ).fetchall()
    return [
        {
            "id": row["id"],
            "label": f'Turno {row["id"]} · {display_datetime(row["aberto_em"])} ate {display_datetime(row["fechado_em"])}',
        }
        for row in rows
    ]


def find_previous_closed_shift_id(current_shift_id: int) -> int | None:
    row = get_db().execute(
        """
        SELECT id
        FROM turnos
        WHERE status = 'closed' AND id < ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (current_shift_id,),
    ).fetchone()
    return row["id"] if row else None


def validate_comparison_shift_id(current_shift_id: int, comparison_shift_id: int | None) -> int | None:
    if not comparison_shift_id or comparison_shift_id == current_shift_id:
        return None
    row = get_db().execute(
        """
        SELECT id
        FROM turnos
        WHERE id = ? AND status = 'closed'
        """,
        (comparison_shift_id,),
    ).fetchone()
    return row["id"] if row else None


def summarize_shift_comparison(
    current_metrics: dict,
    compared_metrics: dict,
    revenue_comparison: dict,
    peak_comparison: dict,
) -> str:
    revenue_pct = revenue_comparison["delta_percentage"]
    if revenue_pct is None:
        revenue_part = (
            "Este turno manteve o mesmo faturamento do comparado."
            if revenue_comparison["indicator"] == "sem mudanca relevante"
            else f'Este turno {revenue_comparison["indicator"]} no faturamento em relacao ao comparado.'
        )
    else:
        revenue_part = f'Este turno faturou {abs(revenue_pct):.1f}% {"a mais" if revenue_pct > 0 else "a menos"} que o comparado.'

    current_peak = current_metrics.get("horario_pico") or {}
    compared_peak = compared_metrics.get("horario_pico") or {}
    current_peak_start = current_peak.get("start_iso")
    compared_peak_start = compared_peak.get("start_iso")
    if current_peak_start and compared_peak_start:
        current_dt = datetime.fromisoformat(current_peak_start)
        compared_dt = datetime.fromisoformat(compared_peak_start)
        minute_delta = int((current_dt - compared_dt).total_seconds() // 60)
        if minute_delta == 0:
            peak_part = "O pico aconteceu na mesma faixa horaria do turno comparado."
        elif minute_delta < 0:
            peak_part = f'O pico veio {abs(minute_delta)} minutos mais cedo.'
        else:
            peak_part = f'O pico veio {minute_delta} minutos mais tarde.'
    else:
        peak_part = "Nao ha dados suficientes para comparar o horario de pico."

    leader_current = (current_metrics.get("bebida_mais_vendida") or {}).get("name")
    leader_compared = (compared_metrics.get("bebida_mais_vendida") or {}).get("name")
    if leader_current and leader_compared and leader_current != leader_compared:
        leader_part = f'A bebida lider mudou de {leader_compared} para {leader_current}.'
    elif leader_current:
        leader_part = f'A bebida lider permaneceu {leader_current}.'
    else:
        leader_part = "Sem bebida lider registrada na comparacao."

    return " ".join([revenue_part, peak_part, leader_part])


def build_shift_comparison(current_shift_id: int, comparison_shift_id: int | None = None) -> dict | None:
    selected_shift_id = validate_comparison_shift_id(current_shift_id, comparison_shift_id)
    if selected_shift_id is None:
        selected_shift_id = find_previous_closed_shift_id(current_shift_id)
    if selected_shift_id is None:
        return None

    current_metrics = build_shift_metrics(current_shift_id)
    compared_metrics = build_shift_metrics(selected_shift_id)
    compared_shift = fetch_shift_details(selected_shift_id, include_comparison=False)

    revenue_comparison = build_numeric_metric_comparison(
        "Total vendido",
        current_metrics["total_vendido"],
        compared_metrics["total_vendido"],
        value_type="currency",
    )
    total_orders_comparison = build_numeric_metric_comparison(
        "Total de pedidos",
        current_metrics["total_pedidos"],
        compared_metrics["total_pedidos"],
        value_type="count",
        tolerance=0.5,
    )
    ticket_comparison = build_numeric_metric_comparison(
        "Ticket medio",
        current_metrics["ticket_medio"],
        compared_metrics["ticket_medio"],
        value_type="currency",
    )
    cost_comparison = build_numeric_metric_comparison(
        "Custo estimado",
        current_metrics["custo_estimado"],
        compared_metrics["custo_estimado"],
        value_type="currency",
    )
    profit_comparison = build_numeric_metric_comparison(
        "Lucro estimado",
        current_metrics["lucro_estimado"],
        compared_metrics["lucro_estimado"],
        value_type="currency",
    )
    peak_orders_comparison = build_numeric_metric_comparison(
        "Pedidos no pico",
        current_metrics["quantidade_pedidos_pico"],
        compared_metrics["quantidade_pedidos_pico"],
        value_type="count",
        tolerance=0.5,
    )
    peak_revenue_comparison = build_numeric_metric_comparison(
        "Valor vendido no pico",
        current_metrics["valor_vendido_pico"],
        compared_metrics["valor_vendido_pico"],
        value_type="currency",
    )

    leader_current = current_metrics.get("bebida_mais_vendida") or {}
    leader_compared = compared_metrics.get("bebida_mais_vendida") or {}
    peak_current = current_metrics.get("horario_pico") or {}
    peak_compared = compared_metrics.get("horario_pico") or {}

    return {
        "current_shift_id": current_shift_id,
        "compared_shift": {
            "id": compared_shift["id"],
            "opened_at": compared_shift["opened_at"],
            "closed_at": compared_shift["closed_at"],
            "duration": compared_shift["duration"],
        },
        "summary_text": summarize_shift_comparison(
            current_metrics,
            compared_metrics,
            revenue_comparison,
            build_text_metric_comparison(
                "Horario de pico",
                peak_current.get("label", "Sem dados"),
                peak_compared.get("label", "Sem dados"),
            ),
        ),
        "metrics": [
            revenue_comparison,
            total_orders_comparison,
            ticket_comparison,
            cost_comparison,
            profit_comparison,
            peak_orders_comparison,
            peak_revenue_comparison,
        ],
        "beverage_leader": {
            "label": "Bebida mais vendida",
            "current": leader_current.get("name", "Sem dados"),
            "compared": leader_compared.get("name", "Sem dados"),
            "current_quantity": leader_current.get("quantity", 0),
            "compared_quantity": leader_compared.get("quantity", 0),
            "indicator": "sem mudanca relevante"
            if leader_current.get("name") == leader_compared.get("name")
            else "mudou",
            "tone": "neutral",
        },
        "peak_window": {
            "label": "Horario de pico",
            "current": peak_current.get("label", "Sem dados"),
            "compared": peak_compared.get("label", "Sem dados"),
            "current_orders": peak_current.get("order_count", 0),
            "compared_orders": peak_compared.get("order_count", 0),
            "current_revenue": peak_current.get("revenue", 0),
            "compared_revenue": peak_compared.get("revenue", 0),
            "indicator": "sem mudanca relevante"
            if peak_current.get("label") == peak_compared.get("label")
            else "mudou",
            "tone": "neutral",
        },
    }


def build_sales_report(shift_id: int | None = None) -> dict:
    current_shift_id = shift_id or get_current_shift_id()
    metrics = build_shift_metrics(current_shift_id)
    return {
        "total_vendido": metrics["total_vendido"],
        "total_pedidos": metrics["total_pedidos"],
        "quantidade_por_bebida": metrics["quantidade_por_bebida"],
        "bebida_mais_pedida": metrics["bebida_mais_pedida"],
        "pico_atendimento": metrics["pico_atendimento"],
    }


def build_order_summary(shift_id: int | None = None) -> dict:
    current_shift_id = shift_id or get_current_shift_id()
    report = build_sales_report(current_shift_id)
    top_tables = get_db().execute(
        """
        SELECT
            COALESCE(NULLIF(TRIM(table_label), ''), 'Retirada') AS table_label,
            COUNT(*) AS total
        FROM pedidos
        WHERE turno_id = ?
        GROUP BY COALESCE(NULLIF(TRIM(table_label), ''), 'Retirada')
        ORDER BY total DESC, table_label ASC
        LIMIT 4
        """,
        (current_shift_id,),
    ).fetchall()
    pending = get_db().execute(
        "SELECT COUNT(*) AS total FROM pedidos WHERE status IN (?, ?) AND turno_id = ?",
        (*ACTIVE_ORDER_STATUSES, current_shift_id),
    ).fetchone()["total"]
    completed = get_db().execute(
        "SELECT COUNT(*) AS total FROM pedidos WHERE status = 'completed' AND turno_id = ?",
        (current_shift_id,),
    ).fetchone()["total"]
    average_ticket = 0.0
    if report["total_pedidos"]:
        average_ticket = round(report["total_vendido"] / report["total_pedidos"], 2)
    return {
        "pending_count": pending or 0,
        "completed_count": completed or 0,
        "total_count": report["total_pedidos"],
        "revenue": report["total_vendido"],
        "average_ticket": average_ticket,
        "top_items": report["quantidade_por_bebida"][:4],
        "top_tables": [
            {"table_label": row["table_label"], "total": row["total"]}
            for row in top_tables
        ],
    }


def build_closeout_report(shift_id: int | None = None) -> dict:
    current_shift_id = shift_id or get_current_shift_id()
    metrics = build_shift_metrics(current_shift_id)
    return metrics


def archive_current_shift_and_open_next() -> dict:
    db = get_db()
    current_shift_id = get_current_shift_id()
    report = build_closeout_report(current_shift_id)

    db.execute(
        """
        UPDATE turnos
        SET status = 'closed', fechado_em = ?, resumo_fechamento = ?
        WHERE id = ?
        """,
        (utc_now_iso(), json.dumps(report), current_shift_id),
    )
    new_shift_id = open_new_shift()
    return {
        "closed_shift_id": current_shift_id,
        "new_shift_id": new_shift_id,
        "report": report,
        "summary": build_order_summary(new_shift_id),
        "pending": fetch_orders(status="pending", shift_id=new_shift_id),
        "completed": fetch_orders(status="completed", limit=20, shift_id=new_shift_id),
        "logistics": fetch_logistics_snapshot(),
        "shifts": fetch_shift_history(),
        "generated_at": display_datetime(utc_now_iso()),
    }


def fetch_shift_history(limit: int = 10) -> list[dict]:
    rows = get_db().execute(
        """
        SELECT id, aberto_em, fechado_em
        FROM turnos
        WHERE status = 'closed'
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    shifts = []
    for row in rows:
        metrics = build_shift_metrics(row["id"])
        shifts.append(
            {
                "id": row["id"],
                "opened_at": display_datetime(row["aberto_em"]),
                "closed_at": display_datetime(row["fechado_em"]),
                "duration": duration_label(row["aberto_em"], row["fechado_em"]),
                "summary": metrics,
                "observations": metrics["observacoes"],
            }
        )
    return shifts


def fetch_order_row_by_code(code: str, shift_id: int | None = None) -> sqlite3.Row | None:
    query = """
        SELECT
            id,
            codigo_retirada,
            order_number,
            horario_pedido,
            status,
            valor_total,
            customer_name,
            table_label,
            source,
            completed_at,
            payment_method,
            payment_status,
            payment_provider,
            payment_provider_id,
            paid_at,
            pix_qr_code,
            pix_copy_paste
        FROM pedidos
        WHERE codigo_retirada = ?
    """
    params: list = [code]
    if shift_id is not None:
        query += " AND turno_id = ?"
        params.append(shift_id)
    return get_db().execute(query, params).fetchone()


def mark_as_paid(code: str, shift_id: int | None = None) -> dict:
    db = get_db()
    order_row = fetch_order_row_by_code(code, shift_id)
    if not order_row:
        raise LookupError("Pedido nao encontrado.")

    if order_row["payment_status"] != "paid":
        db.execute(
            """
            UPDATE pedidos
            SET payment_status = 'paid', paid_at = ?
            WHERE codigo_retirada = ?
            """,
            (utc_now_iso(), code),
        )
        db.commit()
        order_row = fetch_order_row_by_code(code, shift_id)

    return serialize_order(order_row)


def build_ticket(order: dict) -> dict:
    return {
        "order_number": order["order_number"],
        "pickup_code": order["code"],
        "created_at": order["created_at"],
        "customer_name": order["customer_name"],
        "table_label": order["table_label"],
        "payment_method": order["payment_method"],
        "payment_method_label": order["payment_method_label"],
        "payment_status": order["payment_status"],
        "payment_status_label": order["payment_status_label"],
        "paid_at": order["paid_at"],
        "items": order["items"],
        "total": order["total"],
    }


def fetch_shift_details(
    shift_id: int,
    comparison_shift_id: int | None = None,
    *,
    include_comparison: bool = True,
) -> dict:
    row = get_db().execute(
        """
        SELECT id, aberto_em, fechado_em, status
        FROM turnos
        WHERE id = ?
        """,
        (shift_id,),
    ).fetchone()
    if not row or row["status"] != "closed":
        raise LookupError("Turno nao encontrado.")

    metrics = build_shift_metrics(shift_id)
    orders = [serialize_order(order_row) for order_row in fetch_shift_orders_rows(shift_id)]
    comparison = build_shift_comparison(shift_id, comparison_shift_id) if include_comparison else None
    comparable_choices = fetch_comparable_shift_choices(shift_id)

    return {
        "id": row["id"],
        "opened_at": display_datetime(row["aberto_em"]),
        "closed_at": display_datetime(row["fechado_em"]),
        "duration": duration_label(row["aberto_em"], row["fechado_em"]),
        "summary": metrics,
        "orders": orders,
        "items": metrics["ranking_bebidas"],
        "ranking": metrics["ranking_bebidas"],
        "observations": metrics["observacoes"],
        "comparison": comparison,
        "comparison_choices": comparable_choices,
        "selected_comparison_id": comparison["compared_shift"]["id"] if comparison else None,
    }


def build_shift_export_csv(shift_id: int) -> str:
    db = get_db()
    shift_row = db.execute(
        """
        SELECT id, aberto_em, fechado_em, resumo_fechamento
        FROM turnos
        WHERE id = ? AND status = 'closed'
        """,
        (shift_id,),
    ).fetchone()
    if not shift_row:
        raise LookupError("Turno nao encontrado.")

    summary = build_shift_metrics(shift_id)
    orders = db.execute(
        """
        SELECT codigo_retirada, horario_pedido, status, valor_total, customer_name, table_label
        FROM pedidos
        WHERE turno_id = ?
        ORDER BY horario_pedido ASC
        """,
        (shift_id,),
    ).fetchall()

    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["BarOS", f"Turno {shift_id}"])
    writer.writerow(["Aberto em", display_datetime(shift_row["aberto_em"])])
    writer.writerow(["Fechado em", display_datetime(shift_row["fechado_em"])])
    writer.writerow(["Duracao", duration_label(shift_row["aberto_em"], shift_row["fechado_em"])])
    writer.writerow([])
    writer.writerow(["Resumo", "Valor"])
    writer.writerow(["Total vendido", summary.get("total_vendido", 0)])
    writer.writerow(["Total de pedidos", summary.get("total_pedidos", 0)])
    writer.writerow(["Ticket medio", summary.get("ticket_medio", 0)])
    writer.writerow(["Itens vendidos", summary.get("total_itens_vendidos", 0)])
    writer.writerow(["Custo estimado", summary.get("custo_estimado", 0)])
    writer.writerow(["Lucro estimado", summary.get("lucro_estimado", 0)])
    if summary.get("bebida_mais_pedida"):
        writer.writerow(
            [
                "Bebida mais pedida",
                f'{summary["bebida_mais_pedida"]["name"]} ({summary["bebida_mais_pedida"]["quantity"]}x)',
            ]
        )
    if summary.get("pico_atendimento"):
        writer.writerow(
            [
                "Pico de atendimento",
                f'{summary["pico_atendimento"]["hour"]} ({summary["pico_atendimento"]["orders"]} pedidos)',
            ]
        )
        writer.writerow(["Valor vendido no pico", summary["pico_atendimento"].get("revenue", 0)])

    writer.writerow([])
    writer.writerow(["Bebida", "Quantidade"])
    for item in summary.get("quantidade_por_bebida", []):
        writer.writerow([item["name"], item["quantity"]])

    writer.writerow([])
    writer.writerow(["Codigo", "Horario", "Cliente", "Mesa", "Status", "Total"])
    for row in orders:
        writer.writerow(
            [
                row["codigo_retirada"],
                display_datetime(row["horario_pedido"]),
                row["customer_name"] or "Cliente",
                row["table_label"] or "Retirada",
                row["status"],
                row["valor_total"],
            ]
        )

    return buffer.getvalue()


def build_shift_export_pdf(shift_id: int) -> bytes:
    shift = fetch_shift_details(shift_id, include_comparison=False)
    summary = shift["summary"]
    orders = shift["orders"]
    observations = shift["observations"]

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=18 * mm,
        leftMargin=18 * mm,
        topMargin=18 * mm,
        bottomMargin=16 * mm,
        title=f"BarOS - Turno {shift_id}",
        author="BarOS",
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "BarOSTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=20,
        leading=24,
        textColor=colors.HexColor("#15202d"),
        spaceAfter=10,
    )
    section_style = ParagraphStyle(
        "BarOSSection",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=15,
        textColor=colors.HexColor("#223247"),
        spaceBefore=8,
        spaceAfter=8,
    )
    body_style = ParagraphStyle(
        "BarOSBody",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=13,
        textColor=colors.HexColor("#334155"),
    )
    small_style = ParagraphStyle(
        "BarOSSmall",
        parent=body_style,
        fontSize=8.5,
        leading=12,
        textColor=colors.HexColor("#64748b"),
    )

    story = [
        Paragraph("BarOS", title_style),
        Paragraph(f"Relatorio de turno encerrado #{shift['id']}", styles["Heading3"]),
        Paragraph(
            f"Abertura: {shift['opened_at']}<br/>Fechamento: {shift['closed_at']}<br/>Duracao: {shift['duration']}",
            body_style,
        ),
        Spacer(1, 10),
        Paragraph("Resumo executivo", section_style),
    ]

    summary_table_data = [
        ["Total vendido", currency_brl(summary["total_vendido"]), "Total de pedidos", str(summary["total_pedidos"])],
        ["Ticket medio", currency_brl(summary["ticket_medio"]), "Custo estimado", currency_brl(summary["custo_estimado"])],
        ["Lucro estimado", currency_brl(summary["lucro_estimado"]), "Bebida mais vendida", escape((summary.get("bebida_mais_vendida") or {}).get("name", "Sem dados"))],
        ["Horario de pico", escape((summary.get("horario_pico") or {}).get("label", "Sem dados")), "Pedidos no pico", str(summary.get("quantidade_pedidos_pico", 0))],
        ["Valor vendido no pico", currency_brl(summary.get("valor_vendido_pico", 0)), "Itens vendidos", str(summary.get("total_itens_vendidos", 0))],
    ]
    summary_table = Table(summary_table_data, colWidths=[38 * mm, 42 * mm, 38 * mm, 52 * mm])
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
                ("TEXTCOLOR", (0, 0), (-1, -1), colors.HexColor("#0f172a")),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTNAME", (2, 0), (2, -1), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("PADDING", (0, 0), (-1, -1), 8),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.extend([summary_table, Spacer(1, 12), Paragraph("Top 5 bebidas", section_style)])

    top_items = summary.get("top_5_bebidas") or []
    if top_items:
        top_items_data = [["Bebida", "Quantidade", "Faturamento", "Custo"]]
        for item in top_items:
            top_items_data.append(
                [
                    escape(item["name"]),
                    str(item["quantity"]),
                    currency_brl(item["revenue"]),
                    currency_brl(item["cost"]),
                ]
            )
    else:
        top_items_data = [["Bebida", "Quantidade", "Faturamento", "Custo"], ["Sem vendas registradas", "-", "-", "-"]]
    top_items_table = Table(top_items_data, colWidths=[78 * mm, 26 * mm, 34 * mm, 34 * mm])
    top_items_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e293b")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cbd5e1")),
                ("PADDING", (0, 0), (-1, -1), 7),
            ]
        )
    )
    story.extend([top_items_table, Spacer(1, 12), Paragraph("Pedidos do turno", section_style)])

    if orders:
        order_rows = [["Codigo", "Horario", "Cliente", "Mesa", "Status", "Total"]]
        for order in orders:
            order_rows.append(
                [
                    order["code"],
                    order["created_at"],
                    escape(order["customer_name"]),
                    escape(order["table_label"]),
                    escape(order["status"]),
                    currency_brl(order["total"]),
                ]
            )
    else:
        order_rows = [["Codigo", "Horario", "Cliente", "Mesa", "Status", "Total"], ["Sem pedidos", "-", "-", "-", "-", "-"]]

    orders_table = Table(order_rows, colWidths=[22 * mm, 28 * mm, 40 * mm, 28 * mm, 24 * mm, 24 * mm], repeatRows=1)
    orders_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")),
                ("PADDING", (0, 0), (-1, -1), 6),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(orders_table)

    story.extend([Spacer(1, 12), Paragraph("Observacoes do turno", section_style)])
    if observations:
        for observation in observations:
            story.append(Paragraph(f"• {escape(observation)}", body_style))
            story.append(Spacer(1, 4))
    else:
        story.append(Paragraph("Sem observacoes registradas para este turno.", small_style))

    def draw_page(canvas, document):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#64748b"))
        canvas.drawString(document.leftMargin, 10 * mm, f"BarOS · Turno {shift['id']}")
        canvas.drawRightString(A4[0] - document.rightMargin, 10 * mm, f"Pagina {canvas.getPageNumber()}")
        canvas.restoreState()

    doc.build(story, onFirstPage=draw_page, onLaterPages=draw_page)
    return buffer.getvalue()


def upsert_product_from_form(product_id: int | None = None) -> str:
    db = get_db()
    name = sanitize_text(request.form.get("name"), "", limit=80)
    if not name:
        raise ValueError("Nome do produto e obrigatorio.")

    price = parse_decimal_input(request.form.get("price"), "Preco de venda", minimum=0.01)
    cost = parse_decimal_input(request.form.get("cost"), "Custo estimado", minimum=0.0)
    description = sanitize_optional_text(request.form.get("description"), limit=280)
    image_url = normalize_product_image(request.form.get("image_url"))
    is_active = 1 if checkbox_to_bool(request.form.get("is_active")) else 0

    if product_id:
        row = db.execute(
            """
            SELECT id, categoria, tempo_preparo
            FROM bebidas
            WHERE id = ? AND is_combo = 0
            """,
            (product_id,),
        ).fetchone()
        if not row:
            raise ValueError("Produto nao encontrado.")
        db.execute(
            """
            UPDATE bebidas
            SET nome = ?, preco_venda = ?, custo_estimado = ?, descricao = ?, imagem_url = ?, is_active = ?
            WHERE id = ?
            """,
            (name, price, cost, description, image_url, is_active, product_id),
        )
        refresh_combo_costs_for_component(product_id)
        return "Produto atualizado com sucesso."

    db.execute(
        """
        INSERT INTO bebidas (
            nome,
            preco_venda,
            custo_estimado,
            categoria,
            descricao,
            tempo_preparo,
            imagem_url,
            is_active,
            is_combo
        )
        VALUES (?, ?, ?, 'Bebida', ?, '3 min', ?, ?, 0)
        """,
        (name, price, cost, description, image_url, is_active),
    )
    return "Produto criado com sucesso."


def upsert_combo_from_form(combo_id: int | None = None) -> str:
    db = get_db()
    name = sanitize_text(request.form.get("name"), "", limit=80)
    if not name:
        raise ValueError("Nome do combo e obrigatorio.")

    price = parse_decimal_input(request.form.get("price"), "Preco do combo", minimum=0.01)
    description = sanitize_optional_text(request.form.get("description"), limit=280)
    image_url = normalize_product_image(request.form.get("image_url"))
    is_active = 1 if checkbox_to_bool(request.form.get("is_active")) else 0
    components = parse_combo_components_from_form(request.form.getlist("component_ids"))
    catalog = fetch_beverage_map(include_inactive=True)
    for component in components:
        component_row = catalog.get(component["component_beverage_id"])
        if not component_row or component_row["is_combo"]:
            raise ValueError("Combos so podem ser formados por produtos simples.")

    estimated_cost = calculate_combo_cost_estimate(components, catalog)

    if combo_id:
        row = db.execute(
            "SELECT id FROM bebidas WHERE id = ? AND is_combo = 1",
            (combo_id,),
        ).fetchone()
        if not row:
            raise ValueError("Combo nao encontrado.")
        db.execute(
            """
            UPDATE bebidas
            SET nome = ?, preco_venda = ?, custo_estimado = ?, descricao = ?, imagem_url = ?, is_active = ?
            WHERE id = ?
            """,
            (name, price, estimated_cost, description, image_url, is_active, combo_id),
        )
        db.execute("DELETE FROM combo_items WHERE combo_beverage_id = ?", (combo_id,))
        target_combo_id = combo_id
        success_message = "Combo atualizado com sucesso."
    else:
        cursor = db.execute(
            """
            INSERT INTO bebidas (
                nome,
                preco_venda,
                custo_estimado,
                categoria,
                descricao,
                tempo_preparo,
                imagem_url,
                is_active,
                is_combo
            )
            VALUES (?, ?, ?, 'Combo', ?, '4 min', ?, ?, 1)
            """,
            (name, price, estimated_cost, description, image_url, is_active),
        )
        target_combo_id = cursor.lastrowid
        success_message = "Combo criado com sucesso."

    db.executemany(
        """
        INSERT INTO combo_items (combo_beverage_id, component_beverage_id, quantity)
        VALUES (?, ?, ?)
        """,
        [
            (target_combo_id, component["component_beverage_id"], component["quantity"])
            for component in components
        ],
    )
    return success_message


@app.get("/")
def index():
    return render_template(
        "index.html",
        menu=fetch_menu(),
        current_time=display_datetime(utc_now_iso()),
        staff_access_url=url_for("staff_access"),
        payment_methods=[
            {"value": value, "label": label}
            for value, label in PAYMENT_METHOD_LABELS.items()
        ],
    )


def handle_staff_login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = get_db().execute(
            """
            SELECT id, username, password_hash, role, display_name, is_active
            FROM staff_users
            WHERE username = ?
            """,
            (username,),
        ).fetchone()
        if user and user["is_active"] and check_password_hash(user["password_hash"], password):
            session["bar_authenticated"] = True
            session["bar_user_id"] = user["id"]
            session["bar_role"] = user["role"]
            session["bar_display_name"] = user["display_name"]
            return redirect(url_for("dashboard"))
        error = "Usuario ou senha invalidos."
    return render_template(
        "login.html",
        error=error,
        internal_access_path=INTERNAL_ACCESS_PATH,
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return redirect(url_for("staff_access"))
    return handle_staff_login()


def staff_access():
    return handle_staff_login()


app.add_url_rule(f"/{INTERNAL_ACCESS_PATH}", "staff_access", staff_access, methods=["GET", "POST"])


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


@app.get("/painel")
@login_required
def dashboard():
    return render_template(
        "dashboard.html",
        summary=build_order_summary(),
        logistics=fetch_logistics_snapshot(),
        shifts=fetch_shift_history(limit=5),
        current_user=get_current_user(),
    )


@app.get("/painel/produtos")
@login_required
@role_required("admin")
def products_page():
    snapshot = fetch_products_management_snapshot()
    return render_template(
        "products.html",
        products=snapshot["products"],
        combos=snapshot["combos"],
        component_options=snapshot["component_options"],
        current_user=get_current_user(),
        message=request.args.get("message"),
        error=request.args.get("error"),
    )


@app.post("/painel/produtos/salvar")
@login_required
@role_required("admin")
def save_product():
    raw_product_id = request.form.get("product_id", "").strip()
    product_id = int(raw_product_id) if raw_product_id.isdigit() else None
    try:
        message = upsert_product_from_form(product_id)
        get_db().commit()
        return build_products_redirect(message=message)
    except sqlite3.IntegrityError:
        get_db().rollback()
        return build_products_redirect(error="Ja existe um produto com esse nome.")
    except ValueError as error:
        get_db().rollback()
        return build_products_redirect(error=str(error))


@app.post("/painel/produtos/combos/salvar")
@login_required
@role_required("admin")
def save_combo():
    raw_combo_id = request.form.get("combo_id", "").strip()
    combo_id = int(raw_combo_id) if raw_combo_id.isdigit() else None
    try:
        message = upsert_combo_from_form(combo_id)
        get_db().commit()
        return build_products_redirect(message=message)
    except sqlite3.IntegrityError:
        get_db().rollback()
        return build_products_redirect(error="Ja existe um item com esse nome.")
    except ValueError as error:
        get_db().rollback()
        return build_products_redirect(error=str(error))


@app.get("/historico-turnos")
@login_required
def shift_history_page():
    return render_template(
        "shift_history.html",
        shifts=fetch_shift_history(limit=30),
        current_user=get_current_user(),
    )


@app.get("/historico-turnos/<int:shift_id>")
@login_required
def shift_detail_page(shift_id: int):
    compare_to_raw = request.args.get("compare_to", "").strip()
    try:
        compare_to = int(compare_to_raw) if compare_to_raw else None
    except ValueError:
        compare_to = None
    try:
        shift = fetch_shift_details(shift_id, compare_to)
    except LookupError:
        return redirect(url_for("shift_history_page"))

    return render_template(
        "shift_detail.html",
        shift=shift,
        current_user=get_current_user(),
    )


@app.get("/api/orders")
@login_required
def get_orders():
    return jsonify(
        {
            "summary": build_order_summary(),
            "pending": fetch_orders(status="pending"),
            "completed": fetch_orders(status="completed", limit=20),
            "logistics": fetch_logistics_snapshot(),
            "shifts": fetch_shift_history(),
            "generated_at": display_datetime(utc_now_iso()),
        }
    )


@app.get("/pedidos/<code>/imprimir")
@login_required
def print_order_ticket(code: str):
    order_row = fetch_order_row_by_code(code)
    if not order_row:
        return redirect(url_for("dashboard"))
    ticket = build_ticket(serialize_order(order_row))
    return render_template(
        "ticket_print.html",
        ticket=ticket,
        current_user=get_current_user(),
    )


@app.get("/api/reports/shifts")
@login_required
def list_closed_shifts():
    return jsonify({"shifts": fetch_shift_history(limit=30)})


@app.get("/api/reports/shifts/<int:shift_id>")
@login_required
def get_shift_details(shift_id: int):
    compare_to_raw = request.args.get("compare_to", "").strip()
    try:
        compare_to = int(compare_to_raw) if compare_to_raw else None
    except ValueError:
        compare_to = None
    try:
        shift = fetch_shift_details(shift_id, compare_to)
    except LookupError:
        return jsonify({"error": "Turno nao encontrado."}), 404
    return jsonify({"shift": shift})


@app.get("/api/reports/shifts/<int:shift_id>/compare")
@login_required
def get_shift_comparison(shift_id: int):
    compare_to_raw = request.args.get("against", "").strip()
    try:
        compare_to = int(compare_to_raw) if compare_to_raw else None
    except ValueError:
        compare_to = None

    row = get_db().execute(
        "SELECT id FROM turnos WHERE id = ? AND status = 'closed'",
        (shift_id,),
    ).fetchone()
    if not row:
        return jsonify({"error": "Turno nao encontrado."}), 404

    comparison = build_shift_comparison(shift_id, compare_to)
    return jsonify({"comparison": comparison})


@app.get("/api/reports/shifts/<int:shift_id>/export")
@login_required
@role_required("admin")
def export_shift_report(shift_id: int):
    try:
        csv_content = build_shift_export_csv(shift_id)
    except LookupError:
        return jsonify({"error": "Turno nao encontrado."}), 404

    return Response(
        csv_content,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="baros-turno-{shift_id}.csv"'},
    )


@app.get("/api/reports/shifts/<int:shift_id>/export.pdf")
@login_required
@role_required("admin")
def export_shift_pdf(shift_id: int):
    try:
        pdf_bytes = build_shift_export_pdf(shift_id)
    except LookupError:
        return jsonify({"error": "Turno nao encontrado."}), 404

    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="baros-turno-{shift_id}.pdf"'},
    )


@app.post("/api/orders")
def create_order():
    payload = request.get_json(silent=True) or {}
    selected_items, validation_error = validate_order_payload(payload.get("items") or [])
    if validation_error:
        return jsonify({"error": validation_error}), 400

    db = get_db()
    customer_name = sanitize_text(payload.get("customer_name"), "Cliente", limit=48)
    table_label = sanitize_text(payload.get("table_label"), "Retirada", limit=32)
    source = sanitize_text(payload.get("source"), DEFAULT_ORDER_SOURCE, limit=24)
    payment_method = normalize_payment_method(payload.get("payment_method"))
    payment_status = "pending"
    payment_provider = sanitize_optional_text(payload.get("payment_provider"), limit=64) or None
    payment_provider_id = sanitize_optional_text(payload.get("payment_provider_id"), limit=128) or None
    pix_qr_code = sanitize_optional_text(payload.get("pix_qr_code"), limit=4000) or None
    pix_copy_paste = sanitize_optional_text(payload.get("pix_copy_paste"), limit=4000) or None
    if payment_method != "pix":
        payment_provider = None
        payment_provider_id = None
        pix_qr_code = None
        pix_copy_paste = None
    bebidas_por_id = fetch_beverage_map(include_inactive=True)
    combo_components_map = fetch_combo_components_map()
    inventory_by_name = get_inventory_by_name()

    itens = []
    valor_total = 0.0
    for entry in selected_items:
        bebida_id = entry["id"]
        quantidade = entry["quantity"]
        bebida = bebidas_por_id.get(bebida_id)
        if not bebida or quantidade <= 0 or not bebida["is_active"]:
            continue
        is_available, availability_note = beverage_availability(
            bebida,
            inventory_by_name,
            bebidas_por_id,
            combo_components_map,
        )
        if not is_available:
            return jsonify({"error": availability_note or f"{bebida['nome']} indisponivel."}), 400
        subtotal = round(bebida["preco_venda"] * quantidade, 2)
        valor_total += subtotal
        itens.append(
            {
                "bebida_id": bebida_id,
                "name": bebida["nome"],
                "quantity": quantidade,
                "price": bebida["preco_venda"],
                "cost": bebida["custo_estimado"],
                "subtotal": subtotal,
                "item_type": "combo" if bebida["is_combo"] else "product",
            }
        )

    if not itens:
        return jsonify({"error": "Itens invalidos."}), 400

    shortages = check_stock_availability(itens, bebidas_por_id, combo_components_map)
    if shortages:
        readable = ", ".join(
            f'{entry["item"]} ({entry["available"]}/{entry["needed"]} {entry["unit"]})'.strip()
            for entry in shortages
        )
        return jsonify(
            {
                "error": f"Estoque insuficiente para concluir o pedido: {readable}",
                "shortages": shortages,
            }
        ), 400

    codigo = generate_order_code()
    horario = utc_now_iso()
    turno_id = get_current_shift_id()
    cursor = db.execute(
        """
        INSERT INTO pedidos (
            codigo_retirada,
            horario_pedido,
            status,
            valor_total,
            turno_id,
            customer_name,
            table_label,
            source,
            payment_method,
            payment_status,
            payment_provider,
            payment_provider_id,
            pix_qr_code,
            pix_copy_paste
        )
        VALUES (?, ?, 'new', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            codigo,
            horario,
            round(valor_total, 2),
            turno_id,
            customer_name,
            table_label,
            source,
            payment_method,
            payment_status,
            payment_provider,
            payment_provider_id,
            pix_qr_code,
            pix_copy_paste,
        ),
    )
    pedido_id = cursor.lastrowid
    order_number = generate_order_number(pedido_id, turno_id)
    db.execute(
        "UPDATE pedidos SET order_number = ? WHERE id = ?",
        (order_number, pedido_id),
    )

    db.executemany(
        """
        INSERT INTO itens_pedido (
            pedido_id,
            bebida_id,
            quantidade,
            subtotal,
            item_name_snapshot,
            item_type_snapshot,
            unit_price_snapshot,
            unit_cost_snapshot
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                pedido_id,
                item["bebida_id"],
                item["quantity"],
                item["subtotal"],
                item["name"],
                item["item_type"],
                item["price"],
                item["cost"],
            )
            for item in itens
        ],
    )
    apply_stock_deductions(itens, bebidas_por_id, combo_components_map)
    db.commit()

    row = db.execute(
        """
        SELECT
            id,
            codigo_retirada,
            horario_pedido,
            status,
            valor_total,
            customer_name,
            table_label,
            source,
            completed_at,
            order_number,
            payment_method,
            payment_status,
            payment_provider,
            payment_provider_id,
            paid_at,
            pix_qr_code,
            pix_copy_paste
        FROM pedidos
        WHERE id = ?
        """,
        (pedido_id,),
    ).fetchone()
    return jsonify({"order": serialize_order(row), "summary": build_order_summary()}), 201


@app.post("/api/orders/<code>/pay")
@login_required
def pay_order(code: str):
    try:
        order = mark_as_paid(code)
    except LookupError:
        return jsonify({"error": "Pedido nao encontrado."}), 404
    return jsonify({"order": order, "summary": build_order_summary()})


@app.post("/api/orders/<code>/complete")
@login_required
def complete_order(code: str):
    db = get_db()
    current_shift_id = get_current_shift_id()
    row = fetch_order_row_by_code(code, current_shift_id)
    if not row:
        return jsonify({"error": "Pedido nao encontrado."}), 404
    if row["status"] == "completed":
        return jsonify({"order": serialize_order(row), "summary": build_order_summary()})

    if row["payment_method"] == "pix" and normalize_payment_status(row["payment_status"], "pending") != "paid":
        return jsonify({"error": "Pedidos com Pix so podem ser concluidos depois da confirmacao de pagamento."}), 400

    db.execute(
        """
        UPDATE pedidos
        SET status = 'completed', completed_at = ?, completed_by_user_id = ?
        WHERE codigo_retirada = ? AND turno_id = ?
        """,
        (utc_now_iso(), session.get("bar_user_id"), code, current_shift_id),
    )
    db.commit()

    updated = fetch_order_row_by_code(code, current_shift_id)
    return jsonify({"order": serialize_order(updated), "summary": build_order_summary()})


@app.post("/api/reports/closeout")
@login_required
@role_required("admin")
def closeout_report():
    return jsonify(archive_current_shift_and_open_next())


@app.post("/api/reports/reset")
@login_required
@role_required("admin")
def reset_data():
    archived = archive_current_shift_and_open_next()
    archived["ok"] = True
    return jsonify(archived)


@app.post("/api/logistics/inventory/<int:item_id>")
@login_required
@role_required("admin")
def update_inventory(item_id: int):
    payload = request.get_json(silent=True) or {}
    status = payload.get("status")
    stock_action = payload.get("stock_action")
    amount = payload.get("amount")
    par_level = payload.get("par_level")
    db = get_db()
    row = db.execute(
        "SELECT id, stock_level, par_level FROM inventory_items WHERE id = ?",
        (item_id,),
    ).fetchone()
    if not row:
        return jsonify({"error": "Item nao encontrado."}), 404

    next_stock = row["stock_level"]
    next_par = row["par_level"]

    if stock_action:
        try:
            amount_value = float(amount)
        except (TypeError, ValueError):
            return jsonify({"error": "Quantidade invalida."}), 400
        if stock_action == "add":
            if amount_value <= 0:
                return jsonify({"error": "Informe uma quantidade positiva para reabastecer."}), 400
            next_stock = row["stock_level"] + amount_value
        elif stock_action == "set":
            if amount_value < 0:
                return jsonify({"error": "O estoque nao pode ficar negativo."}), 400
            next_stock = amount_value
        else:
            return jsonify({"error": "Acao de estoque invalida."}), 400

    if par_level not in (None, ""):
        try:
            next_par = float(par_level)
        except (TypeError, ValueError):
            return jsonify({"error": "Nivel minimo invalido."}), 400
        if next_par <= 0:
            return jsonify({"error": "O nivel minimo deve ser maior que zero."}), 400

    if stock_action or par_level not in (None, ""):
        next_status = calculate_inventory_status(next_stock, next_par)
    else:
        if status not in {"ok", "attention", "critical"}:
            return jsonify({"error": "Status invalido."}), 400
        next_status = status

    db.execute(
        """
        UPDATE inventory_items
        SET stock_level = ?, par_level = ?, status = ?, updated_at = ?
        WHERE id = ?
        """,
        (next_stock, next_par, next_status, utc_now_iso(), item_id),
    )
    db.commit()
    return jsonify(fetch_logistics_snapshot())


@app.post("/api/logistics/notes/<int:note_id>/close")
@login_required
def close_shift_note(note_id: int):
    db = get_db()
    row = db.execute("SELECT id FROM shift_notes WHERE id = ?", (note_id,)).fetchone()
    if not row:
        return jsonify({"error": "Nota nao encontrada."}), 404
    db.execute("UPDATE shift_notes SET status = 'done' WHERE id = ?", (note_id,))
    db.commit()
    return jsonify(fetch_logistics_snapshot())


init_db()


if __name__ == "__main__":
    host = os.getenv("BAROS_HOST", "127.0.0.1")
    port = int(os.getenv("PORT", os.getenv("BAROS_PORT", "5000")))
    debug = os.getenv("BAROS_DEBUG", "true").lower() == "true"
    app.run(debug=debug, host=host, port=port)
