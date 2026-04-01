from __future__ import annotations
import csv
import json
import io
import os
import re
import secrets
import sqlite3  # legacy import only; runtime storage uses PostgreSQL
import time
from threading import Lock
from datetime import datetime, timedelta, timezone
from functools import wraps
from html import escape
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import sentry_sdk
from dotenv import load_dotenv
from psycopg import IntegrityError, connect
from psycopg.rows import dict_row
from flask import Flask, Response, g, jsonify, redirect, render_template, request, session, url_for
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
from werkzeug.security import check_password_hash, generate_password_hash


load_dotenv(override=False)

BASE_DIR = Path(__file__).resolve().parent

INSTANCE_DIR = BASE_DIR / "instance"
INSTANCE_DIR.mkdir(exist_ok=True)

DATABASE_URL = (os.getenv("DATABASE_URL") or "").strip()
LEGACY_SQLITE_IMPORT_PATH_RAW = (
    os.getenv("BAROS_SQLITE_IMPORT_PATH")
    or os.getenv("DATABASE_PATH")
    or os.getenv("BAROS_DB_PATH")
    or ""
).strip()
LEGACY_SQLITE_IMPORT_PATH = (
    Path(LEGACY_SQLITE_IMPORT_PATH_RAW).expanduser() if LEGACY_SQLITE_IMPORT_PATH_RAW else None
)
BAROS_ENV = (os.getenv("BAROS_ENV") or os.getenv("FLASK_ENV") or "development").strip().lower()
IS_LOCAL_ENV = BAROS_ENV in {"development", "dev", "local"}
IS_PRODUCTION = BAROS_ENV in {"production", "staging"}
BAROS_SECRET_KEY = (os.getenv("BAROS_SECRET_KEY") or "").strip()
BAROS_COOKIE_SECURE = os.getenv("BAROS_COOKIE_SECURE")
SENTRY_DSN = (os.getenv("SENTRY_DSN") or "").strip()
SENTRY_RELEASE = (os.getenv("BAROS_RELEASE") or os.getenv("RENDER_GIT_COMMIT") or "").strip() or None
ALLOW_DEFAULT_SEED_USERS = (
    os.getenv("BAROS_ALLOW_DEFAULT_SEED_USERS", "true" if IS_LOCAL_ENV else "false").strip().lower()
    == "true"
)
LEGACY_SEED_ADMIN_USERNAME = (os.getenv("BAROS_USERNAME") or "").strip()
LEGACY_SEED_ADMIN_PASSWORD = os.getenv("BAROS_PASSWORD") or ""
LEGACY_SEED_OPERATOR_USERNAME = (os.getenv("BAROS_OPERATOR_USERNAME") or "").strip()
LEGACY_SEED_OPERATOR_PASSWORD = os.getenv("BAROS_OPERATOR_PASSWORD") or ""
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
ORDER_TYPE_LABELS = {
    "pista": "Pista",
    "camarote": "Camarote",
}
ORDER_STATUS_LABELS = {
    "new": "Liberado ao bar",
    "pending": "Liberado ao bar",
    "pending_payment": "Aguardando Pix",
    "completed": "Concluido",
}
PRODUCT_CATEGORY_OPTIONS = [
    "Bebida",
    "Bistrô",
    "Fogos",
    "Letreiro",
    "Autoral",
    "Classico",
    "Assinatura",
    "Premium",
    "Leve",
    "Chopp",
]
ACTIVE_ORDER_STATUSES = ("new", "pending")
AWAITING_PAYMENT_STATUS = "pending_payment"
PREORDER_ACTIVE_STATUSES = ("new", "preparing")
try:
    LOCAL_TIMEZONE = ZoneInfo(os.getenv("BAROS_TIMEZONE", "America/Sao_Paulo"))
except ZoneInfoNotFoundError:
    LOCAL_TIMEZONE = timezone(timedelta(hours=-3))

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
SQL_NAMED_PARAM_PATTERN = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")
DbRow = dict[str, Any]
SNAPSHOT_CACHE_TTLS = {
    "menu": 2.0,
    "logistics": 3.0,
    "order_summary": 2.0,
    "shift_history": 15.0,
}
_SNAPSHOT_CACHE: dict[str, tuple[float, Any]] = {}
_SNAPSHOT_CACHE_LOCK = Lock()


def normalize_internal_access_path(raw_value: str) -> str:
    cleaned = re.sub(r"[^a-z0-9-]+", "-", (raw_value or "").strip().lower()).strip("-")
    return cleaned or "backstage"


INTERNAL_ACCESS_PATH = normalize_internal_access_path(os.getenv("BAROS_INTERNAL_ACCESS_PATH", "backstage"))


def build_runtime_secret_key() -> str:
    if BAROS_SECRET_KEY:
        return BAROS_SECRET_KEY
    if IS_PRODUCTION:
        raise RuntimeError("BAROS_SECRET_KEY obrigatorio quando BAROS_ENV=production.")
    return secrets.token_hex(32)


def validate_runtime_security() -> None:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL obrigatorio para iniciar o BarOS.")
    if IS_PRODUCTION and len(BAROS_SECRET_KEY) < 32:
        raise RuntimeError("BAROS_SECRET_KEY precisa ter pelo menos 32 caracteres em producao.")
    if IS_PRODUCTION and ALLOW_DEFAULT_SEED_USERS:
        raise RuntimeError("BAROS_ALLOW_DEFAULT_SEED_USERS deve ficar desativado em producao.")


validate_runtime_security()


if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        environment=BAROS_ENV,
        release=SENTRY_RELEASE,
        traces_sample_rate=0.2,
        send_default_pii=False,
    )


app = Flask(__name__)
app.config["SECRET_KEY"] = build_runtime_secret_key()
app.config["MAX_CONTENT_LENGTH"] = 1024 * 1024
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = (
    BAROS_COOKIE_SECURE.strip().lower() == "true"
    if BAROS_COOKIE_SECURE is not None
    else IS_PRODUCTION
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def local_now() -> datetime:
    return datetime.now(ZoneInfo("America/Sao_Paulo"))


def display_datetime(value: str | None) -> str:
    if not value:
        return "-"
    return datetime.fromisoformat(value).astimezone(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M")


def hour_bucket_label(value: str | None) -> str:
    if not value:
        return "-"
    return datetime.fromisoformat(value).astimezone(ZoneInfo("America/Sao_Paulo")).strftime("%Hh")


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


def read_snapshot_cache(cache_key: str) -> Any | None:
    with _SNAPSHOT_CACHE_LOCK:
        entry = _SNAPSHOT_CACHE.get(cache_key)
        if not entry:
            return None
        expires_at, value = entry
        if expires_at <= time.monotonic():
            _SNAPSHOT_CACHE.pop(cache_key, None)
            return None
        return value


def write_snapshot_cache(cache_key: str, ttl_seconds: float, value: Any) -> Any:
    with _SNAPSHOT_CACHE_LOCK:
        _SNAPSHOT_CACHE[cache_key] = (time.monotonic() + ttl_seconds, value)
    return value


def invalidate_snapshot_cache(*prefixes: str) -> None:
    with _SNAPSHOT_CACHE_LOCK:
        if not prefixes:
            _SNAPSHOT_CACHE.clear()
            return
        stale_keys = [key for key in _SNAPSHOT_CACHE if any(key.startswith(prefix) for prefix in prefixes)]
        for key in stale_keys:
            _SNAPSHOT_CACHE.pop(key, None)


def sanitize_optional_text(value: str | None, limit: int = 255) -> str:
    return (value or "").strip()[:limit]


def normalize_product_category(raw_value: str | None) -> str:
    return sanitize_text(raw_value, "Bebida", limit=32)


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


def parse_optional_integer_input(raw_value: str | None, label: str, minimum: int = 1) -> int | None:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return None
    value = parse_integer_input(cleaned, label, minimum=minimum)
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


def normalize_query_params(params: Any = None):
    if params is None:
        return None
    if isinstance(params, dict):
        return params
    if isinstance(params, (list, tuple)):
        return tuple(params)
    return (params,)


def adapt_sql_query(query: str, params: Any = None) -> tuple[str, Any]:
    if params is None:
        return query, None
    if isinstance(params, dict):
        return SQL_NAMED_PARAM_PATTERN.sub(lambda match: f"%({match.group(1)})s", query), params
    return query.replace("?", "%s"), normalize_query_params(params)


class DatabaseConnection:
    def __init__(self, raw_connection) -> None:
        self._raw_connection = raw_connection

    def execute(self, query: str, params: Any = None):
        sql, values = adapt_sql_query(query, params)
        cursor = self._raw_connection.cursor(row_factory=dict_row)
        cursor.execute(sql, values)
        return cursor

    def executemany(self, query: str, params_seq) -> Any:
        sql, _ = adapt_sql_query(query, ())
        normalized_params = [normalize_query_params(item) for item in params_seq]
        cursor = self._raw_connection.cursor(row_factory=dict_row)
        cursor.executemany(sql, normalized_params)
        return cursor

    def commit(self) -> None:
        self._raw_connection.commit()

    def rollback(self) -> None:
        self._raw_connection.rollback()

    def close(self) -> None:
        self._raw_connection.close()


def open_db_connection() -> DatabaseConnection:
    raw_connection = connect(DATABASE_URL, row_factory=dict_row, autocommit=False)
    return DatabaseConnection(raw_connection)


def get_db() -> DatabaseConnection:
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


def get_table_columns(db: DatabaseConnection, table_name: str) -> list[str]:
    rows = db.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = ?
        ORDER BY ordinal_position
        """,
        (table_name,),
    ).fetchall()
    return [row["column_name"] for row in rows]


def table_row_count(db: DatabaseConnection, table_name: str) -> int:
    row = db.execute(f"SELECT COUNT(*) AS total FROM {table_name}").fetchone()
    return int(row["total"] or 0)


def reset_table_sequence(db: DatabaseConnection, table_name: str) -> None:
    sequence_row = db.execute(
        "SELECT pg_get_serial_sequence(?, 'id') AS sequence_name",
        (table_name,),
    ).fetchone()
    sequence_name = sequence_row["sequence_name"] if sequence_row else None
    if not sequence_name:
        return
    db.execute(
        f"""
        SELECT setval(
            ?,
            COALESCE((SELECT MAX(id) FROM {table_name}), 1),
            EXISTS(SELECT 1 FROM {table_name})
        )
        """,
        (sequence_name,),
    )


def safe_rollback(db: DatabaseConnection) -> None:
    try:
        db.rollback()
    except Exception:
        pass


def sqlite_table_exists(db: sqlite3.Connection, table_name: str) -> bool:
    row = db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def sqlite_table_columns(db: sqlite3.Connection, table_name: str) -> list[str]:
    return [row["name"] for row in db.execute(f"PRAGMA table_info({table_name})").fetchall()]


def import_table_from_sqlite(
    postgres_db: DatabaseConnection,
    sqlite_db: sqlite3.Connection,
    table_name: str,
    postgres_columns: list[str],
) -> None:
    if not sqlite_table_exists(sqlite_db, table_name):
        return

    source_columns = sqlite_table_columns(sqlite_db, table_name)
    shared_columns = [column for column in postgres_columns if column in source_columns]
    if not shared_columns:
        return

    select_sql = f"SELECT {', '.join(shared_columns)} FROM {table_name} ORDER BY id ASC"
    rows = sqlite_db.execute(select_sql).fetchall()
    if not rows:
        return

    placeholders = ", ".join(["?"] * len(shared_columns))
    insert_sql = (
        f"INSERT INTO {table_name} ({', '.join(shared_columns)}) "
        f"VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
    )
    postgres_db.executemany(
        insert_sql,
        [tuple(row[column] for column in shared_columns) for row in rows],
    )
    reset_table_sequence(postgres_db, table_name)


def import_legacy_sqlite_if_needed(db: DatabaseConnection) -> None:
    if not LEGACY_SQLITE_IMPORT_PATH or not LEGACY_SQLITE_IMPORT_PATH.exists():
        return

    tables = [
        "turnos",
        "bebidas",
        "staff_users",
        "preorder_settings",
        "inventory_items",
        "shift_notes",
        "pedidos",
        "itens_pedido",
        "combo_items",
    ]
    if any(table_row_count(db, table_name) for table_name in tables):
        return

    sqlite_db = sqlite3.connect(LEGACY_SQLITE_IMPORT_PATH)
    sqlite_db.row_factory = sqlite3.Row
    try:
        for table_name in tables:
            import_table_from_sqlite(db, sqlite_db, table_name, get_table_columns(db, table_name))
    finally:
        sqlite_db.close()


def build_seed_staff_accounts() -> list[dict]:
    accounts = []

    admin_username = (os.getenv("BAROS_SEED_ADMIN_USERNAME") or LEGACY_SEED_ADMIN_USERNAME).strip()
    admin_password = os.getenv("BAROS_SEED_ADMIN_PASSWORD") or LEGACY_SEED_ADMIN_PASSWORD
    if admin_username and admin_password:
        accounts.append(
            {
                "username": admin_username,
                "password": admin_password,
                "role": "admin",
                "display_name": "Administrador",
            }
        )

    operator_username = (os.getenv("BAROS_SEED_OPERATOR_USERNAME") or LEGACY_SEED_OPERATOR_USERNAME).strip()
    operator_password = os.getenv("BAROS_SEED_OPERATOR_PASSWORD") or LEGACY_SEED_OPERATOR_PASSWORD
    if operator_username and operator_password:
        accounts.append(
            {
                "username": operator_username,
                "password": operator_password,
                "role": "operator",
                "display_name": "Operacao",
            }
        )

    if accounts:
        return accounts

    # Credenciais previsiveis so existem como atalho local, nunca como padrao
    # implícito em staging/producao.
    if not ALLOW_DEFAULT_SEED_USERS or not IS_LOCAL_ENV:
        return []

    return [
        {
            "username": "admin",
            "password": "bar123",
            "role": "admin",
            "display_name": "Administrador",
        },
        {
            "username": "operacao",
            "password": "bar123",
            "role": "operator",
            "display_name": "Operacao",
        },
    ]


def seed_staff_user_if_missing(db: DatabaseConnection, username: str, password: str, role: str, display_name: str) -> None:
    if not username or not password:
        return

    existing = db.execute(
        "SELECT id FROM staff_users WHERE username = ?",
        (username,),
    ).fetchone()
    if existing:
        return

    db.execute(
        """
        INSERT INTO staff_users (username, password_hash, role, display_name, is_active, created_at, updated_at)
        VALUES (?, ?, ?, ?, 1, ?, ?)
        """,
        (username, generate_password_hash(password), role, display_name, utc_now_iso(), utc_now_iso()),
    )


def validate_staff_bootstrap(db: DatabaseConnection) -> None:
    admin_total = db.execute(
        "SELECT COUNT(*) AS total FROM staff_users WHERE role = 'admin' AND is_active = 1"
    ).fetchone()["total"]
    if IS_PRODUCTION and not admin_total:
        raise RuntimeError(
            "Nenhum usuario admin ativo foi encontrado. Configure BAROS_SEED_ADMIN_USERNAME e "
            "BAROS_SEED_ADMIN_PASSWORD no primeiro deploy ou cadastre um admin no banco."
        )


def create_core_tables(db: DatabaseConnection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS bebidas (
            id BIGSERIAL PRIMARY KEY,
            nome TEXT NOT NULL UNIQUE,
            preco_venda DOUBLE PRECISION NOT NULL,
            custo_estimado DOUBLE PRECISION NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS pedidos (
            id BIGSERIAL PRIMARY KEY,
            codigo_retirada TEXT NOT NULL UNIQUE,
            horario_pedido TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            valor_total DOUBLE PRECISION NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS turnos (
            id BIGSERIAL PRIMARY KEY,
            aberto_em TEXT NOT NULL,
            fechado_em TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            resumo_fechamento TEXT
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS itens_pedido (
            id BIGSERIAL PRIMARY KEY,
            pedido_id BIGINT NOT NULL REFERENCES pedidos(id) ON DELETE CASCADE,
            bebida_id BIGINT NOT NULL REFERENCES bebidas(id),
            quantidade INTEGER NOT NULL,
            subtotal DOUBLE PRECISION NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS combo_items (
            id BIGSERIAL PRIMARY KEY,
            combo_beverage_id BIGINT NOT NULL REFERENCES bebidas(id) ON DELETE CASCADE,
            component_beverage_id BIGINT NOT NULL REFERENCES bebidas(id),
            quantity INTEGER NOT NULL DEFAULT 1,
            UNIQUE(combo_beverage_id, component_beverage_id)
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_items (
            id BIGSERIAL PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            unit TEXT NOT NULL,
            stock_level DOUBLE PRECISION NOT NULL,
            par_level DOUBLE PRECISION NOT NULL,
            status TEXT NOT NULL DEFAULT 'ok',
            updated_at TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS shift_notes (
            id BIGSERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            priority TEXT NOT NULL DEFAULT 'media',
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS staff_users (
            id BIGSERIAL PRIMARY KEY,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'operator',
            display_name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS preorder_settings (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            preorder_start_time TEXT,
            preorder_end_time TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )


def apply_schema_updates(db: DatabaseConnection) -> None:
    db.execute("ALTER TABLE bebidas ADD COLUMN IF NOT EXISTS categoria TEXT")
    db.execute("ALTER TABLE bebidas ADD COLUMN IF NOT EXISTS descricao TEXT")
    db.execute("ALTER TABLE bebidas ADD COLUMN IF NOT EXISTS tempo_preparo TEXT")
    db.execute("ALTER TABLE bebidas ADD COLUMN IF NOT EXISTS imagem_url TEXT")
    db.execute("ALTER TABLE bebidas ADD COLUMN IF NOT EXISTS is_active INTEGER NOT NULL DEFAULT 1")
    db.execute("ALTER TABLE bebidas ADD COLUMN IF NOT EXISTS is_combo INTEGER NOT NULL DEFAULT 0")
    db.execute("ALTER TABLE bebidas ADD COLUMN IF NOT EXISTS max_active_orders INTEGER")

    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS turno_id BIGINT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS customer_name TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS table_label TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS source TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS completed_at TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS completed_by_user_id BIGINT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS order_number TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS payment_method TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS payment_status TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS payment_provider TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS payment_provider_id TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS paid_at TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS pix_qr_code TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS pix_copy_paste TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS request_id TEXT")
    db.execute("ALTER TABLE pedidos ADD COLUMN IF NOT EXISTS order_type TEXT")
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_pedidos_request_id ON pedidos(request_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pedidos_turno_status_horario ON pedidos(turno_id, status, horario_pedido DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pedidos_turno_horario ON pedidos(turno_id, horario_pedido DESC)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_pedidos_codigo_turno ON pedidos(codigo_retirada, turno_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_turnos_status_id ON turnos(status, id DESC)")

    db.execute("ALTER TABLE itens_pedido ADD COLUMN IF NOT EXISTS item_name_snapshot TEXT")
    db.execute("ALTER TABLE itens_pedido ADD COLUMN IF NOT EXISTS item_type_snapshot TEXT")
    db.execute("ALTER TABLE itens_pedido ADD COLUMN IF NOT EXISTS unit_price_snapshot DOUBLE PRECISION")
    db.execute("ALTER TABLE itens_pedido ADD COLUMN IF NOT EXISTS unit_cost_snapshot DOUBLE PRECISION")
    db.execute("CREATE INDEX IF NOT EXISTS idx_itens_pedido_pedido_id ON itens_pedido(pedido_id)")


def seed_bootstrap_data(db: DatabaseConnection) -> None:
    beverages_total = db.execute("SELECT COUNT(*) AS total FROM bebidas").fetchone()["total"]
    inventory_total = db.execute("SELECT COUNT(*) AS total FROM inventory_items").fetchone()["total"]
    notes_total = db.execute("SELECT COUNT(*) AS total FROM shift_notes").fetchone()["total"]

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

    for account in build_seed_staff_accounts():
        seed_staff_user_if_missing(
            db,
            account["username"],
            account["password"],
            account["role"],
            account["display_name"],
        )


def ensure_open_shift(db: DatabaseConnection) -> int:
    open_shift = db.execute("SELECT id FROM turnos WHERE status = 'open' ORDER BY id DESC LIMIT 1").fetchone()
    if open_shift:
        return open_shift["id"]

    created_shift = db.execute(
        """
        INSERT INTO turnos (aberto_em, status)
        VALUES (?, 'open')
        RETURNING id
        """,
        (utc_now_iso(),),
    ).fetchone()
    return created_shift["id"]


def backfill_existing_rows(db: DatabaseConnection, open_shift_id: int) -> None:
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
        "UPDATE pedidos SET order_type = 'pista' WHERE order_type IS NULL OR TRIM(order_type) = '' OR order_type = 'bistro'"
    )
    db.execute(
        f"""
        UPDATE pedidos
        SET status = '{AWAITING_PAYMENT_STATUS}'
        WHERE payment_method = 'pix'
          AND COALESCE(payment_status, 'pending') != 'paid'
          AND status IN ('new', 'pending')
        """
    )
    db.execute(
        """
        UPDATE pedidos
        SET order_number = codigo_retirada
        WHERE order_number IS NULL
           OR TRIM(order_number) = ''
           OR order_number != codigo_retirada
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
            CAST((subtotal / CASE WHEN quantidade > 0 THEN quantidade ELSE 1 END) AS numeric),
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


def init_db() -> None:
    db = open_db_connection()
    try:
        create_core_tables(db)
        apply_schema_updates(db)
        import_legacy_sqlite_if_needed(db)
        seed_bootstrap_data(db)
        open_shift_id = ensure_open_shift(db)
        backfill_existing_rows(db, open_shift_id)
        validate_staff_bootstrap(db)
        db.commit()
    except Exception:
        safe_rollback(db)
        raise
    finally:
        db.close()


def is_api_request() -> bool:
    return request.path.startswith("/api/")


def is_staff_authenticated() -> bool:
    return bool(session.get("bar_authenticated") and session.get("bar_user_id"))


def authentication_required_response():
    if is_api_request():
        return jsonify({"error": "Faca login para continuar."}), 401
    return redirect(url_for("staff_access"))


def permission_denied_response():
    if is_api_request():
        return jsonify({"error": "Voce nao tem permissao para esta acao."}), 403
    return redirect(url_for("dashboard"))


def fetch_staff_user_by_id(user_id: int | None, db: DatabaseConnection | None = None) -> DbRow | None:
    if not user_id:
        return None
    connection = db or get_db()
    return connection.execute(
        """
        SELECT id, username, password_hash, role, display_name, is_active, created_at, updated_at
        FROM staff_users
        WHERE id = ?
        """,
        (user_id,),
    ).fetchone()


def fetch_staff_user_by_username(username: str, db: DatabaseConnection | None = None) -> DbRow | None:
    normalized_username = (username or "").strip()
    if not normalized_username:
        return None
    connection = db or get_db()
    return connection.execute(
        """
        SELECT id, username, password_hash, role, display_name, is_active, created_at, updated_at
        FROM staff_users
        WHERE username = ?
        """,
        (normalized_username,),
    ).fetchone()


def authenticate_staff_user(username: str, password: str) -> DbRow | None:
    user = fetch_staff_user_by_username(username)
    if not user or not user["is_active"]:
        return None
    if not check_password_hash(user["password_hash"], password):
        return None
    return user


def begin_staff_session(user: DbRow) -> None:
    session.clear()
    session["bar_authenticated"] = True
    session["bar_user_id"] = user["id"]
    session["bar_role"] = user["role"]
    session["bar_display_name"] = user["display_name"]
    session["bar_username"] = user["username"]


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not is_staff_authenticated():
            return authentication_required_response()
        return view(*args, **kwargs)

    return wrapped_view


def role_required(*allowed_roles: str):
    def decorator(view):
        @wraps(view)
        def wrapped_view(*args, **kwargs):
            if not is_staff_authenticated():
                return authentication_required_response()
            if session.get("bar_role") not in allowed_roles:
                return permission_denied_response()
            return view(*args, **kwargs)

        return wrapped_view

    return decorator


def get_current_user() -> dict:
    role = session.get("bar_role", "operator")
    return {
        "id": session.get("bar_user_id"),
        "username": session.get("bar_username"),
        "display_name": session.get("bar_display_name", "Equipe"),
        "role": role,
        "role_label": ROLE_LABELS.get(role, role.title()),
        "can_manage_bar": role == "admin",
    }


def validate_staff_username(username: str, label: str = "Usuario") -> str:
    normalized = (username or "").strip()
    if len(normalized) < 3:
        raise ValueError(f"{label} precisa ter pelo menos 3 caracteres.")
    if len(normalized) > 40:
        raise ValueError(f"{label} nao pode passar de 40 caracteres.")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", normalized):
        raise ValueError(f"{label} so pode usar letras, numeros, ponto, traco e underscore.")
    return normalized


def validate_staff_password(password: str, *, required: bool = True, label: str = "Senha") -> str:
    normalized = password or ""
    if not normalized:
        if required:
            raise ValueError(f"{label} e obrigatoria.")
        return ""
    if len(normalized) < 8:
        raise ValueError(f"{label} precisa ter pelo menos 8 caracteres.")
    return normalized


def fetch_staff_settings_snapshot() -> dict:
    db = get_db()
    current_admin = fetch_staff_user_by_id(session.get("bar_user_id"), db=db)
    operator_user = db.execute(
        """
        SELECT id, username, role, display_name, is_active
        FROM staff_users
        WHERE role = 'operator'
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    return {
        "admin": {
            "id": current_admin["id"] if current_admin else None,
            "username": current_admin["username"] if current_admin else "",
            "display_name": current_admin["display_name"] if current_admin else "Administrador",
        },
        "operator": {
            "id": operator_user["id"] if operator_user else None,
            "username": operator_user["username"] if operator_user else "",
            "display_name": operator_user["display_name"] if operator_user else "Operacao",
            "is_active": bool(operator_user["is_active"]) if operator_user else True,
        },
    }


def update_admin_credentials(user_id: int, form_data) -> str:
    db = get_db()
    current_user = fetch_staff_user_by_id(user_id, db=db)
    if not current_user:
        raise ValueError("Usuario admin nao encontrado.")

    next_username = validate_staff_username(form_data.get("username", ""), "Usuario admin")
    next_password = validate_staff_password(
        form_data.get("new_password", ""),
        required=False,
        label="Nova senha do admin",
    )
    password_confirmation = form_data.get("confirm_password", "")
    if next_password and next_password != password_confirmation:
        raise ValueError("A confirmacao da nova senha do admin nao confere.")

    duplicate = db.execute(
        "SELECT id FROM staff_users WHERE username = ? AND id != ?",
        (next_username, user_id),
    ).fetchone()
    if duplicate:
        raise ValueError("Esse usuario admin ja esta em uso.")

    db.execute(
        """
        UPDATE staff_users
        SET username = ?, password_hash = ?, updated_at = ?
        WHERE id = ?
        """,
        (
            next_username,
            generate_password_hash(next_password) if next_password else current_user["password_hash"],
            utc_now_iso(),
            user_id,
        ),
    )

    refreshed_user = fetch_staff_user_by_id(user_id, db=db)
    if refreshed_user:
        begin_staff_session(refreshed_user)
    return "Credenciais do admin atualizadas com sucesso."


def upsert_operator_account(form_data) -> str:
    db = get_db()
    username = validate_staff_username(form_data.get("operator_username", ""), "Usuario do operador")
    display_name = sanitize_text(form_data.get("operator_display_name"), "Operacao", limit=48)
    password = validate_staff_password(
        form_data.get("operator_password", ""),
        required=False,
        label="Senha do operador",
    )
    is_active = 1 if checkbox_to_bool(form_data.get("operator_is_active")) else 0

    operator_user = db.execute(
        """
        SELECT id, password_hash
        FROM staff_users
        WHERE role = 'operator'
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    duplicate = db.execute(
        """
        SELECT id
        FROM staff_users
        WHERE username = ?
          AND (? IS NULL OR id != ?)
        """,
        (username, operator_user["id"] if operator_user else None, operator_user["id"] if operator_user else None),
    ).fetchone()
    if duplicate:
        raise ValueError("Esse usuario ja esta em uso por outro membro da equipe.")

    if operator_user:
        if not password:
            password_hash = operator_user["password_hash"]
        else:
            password_hash = generate_password_hash(password)
        db.execute(
            """
            UPDATE staff_users
            SET username = ?, display_name = ?, password_hash = ?, is_active = ?, updated_at = ?
            WHERE id = ?
            """,
            (username, display_name, password_hash, is_active, utc_now_iso(), operator_user["id"]),
        )
        return "Conta do operador atualizada com sucesso."

    if not password:
        raise ValueError("Informe uma senha para criar a conta do operador.")

    db.execute(
        """
        INSERT INTO staff_users (username, password_hash, role, display_name, is_active, created_at, updated_at)
        VALUES (?, ?, 'operator', ?, ?, ?, ?)
        """,
        (username, generate_password_hash(password), display_name, is_active, utc_now_iso(), utc_now_iso()),
    )
    return "Conta do operador criada com sucesso."


def generate_order_code() -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    db = get_db()
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(5))
        exists = db.execute(
            "SELECT 1 FROM pedidos WHERE codigo_retirada = ?",
            (code,),
        ).fetchone()
        if not exists:
            return code


def generate_order_number(code: str) -> str:
    return code


def build_fake_pix_payload(order_code: str, total: float) -> dict:
    provider_id = f"PIX-{secrets.token_hex(4).upper()}"
    amount = f"{float(total or 0):.2f}"
    copy_paste = (
        f"00020126580014BR.GOV.BCB.PIX0136BAROS-SIMULADO-{order_code}"
        f"520400005303986540{amount}5802BR5913BAROS6009SAO PAULO62070503***6304{order_code}"
    )
    qr_svg = f"""
    <svg xmlns="http://www.w3.org/2000/svg" width="280" height="280" viewBox="0 0 280 280">
      <rect width="280" height="280" rx="24" fill="#ffffff"/>
      <rect x="18" y="18" width="244" height="244" rx="18" fill="#101722" stroke="#f2b35d" stroke-width="6"/>
      <rect x="38" y="38" width="58" height="58" fill="#f2b35d"/>
      <rect x="184" y="38" width="58" height="58" fill="#f2b35d"/>
      <rect x="38" y="184" width="58" height="58" fill="#f2b35d"/>
      <rect x="118" y="58" width="18" height="18" fill="#f5f7fb"/>
      <rect x="154" y="58" width="18" height="18" fill="#f5f7fb"/>
      <rect x="118" y="94" width="18" height="18" fill="#f5f7fb"/>
      <rect x="154" y="94" width="18" height="18" fill="#f5f7fb"/>
      <rect x="118" y="130" width="18" height="18" fill="#f5f7fb"/>
      <rect x="154" y="130" width="18" height="18" fill="#f5f7fb"/>
      <rect x="94" y="154" width="92" height="18" fill="#6ad7a0"/>
      <text x="140" y="214" text-anchor="middle" fill="#f5f7fb" font-size="28" font-family="Arial, sans-serif" font-weight="700">PIX</text>
      <text x="140" y="242" text-anchor="middle" fill="#9ca8bb" font-size="14" font-family="Arial, sans-serif">{order_code}</text>
    </svg>
    """.strip()
    return {
        "payment_provider": "baros-pix-simulado",
        "payment_provider_id": provider_id,
        "pix_copy_paste": copy_paste,
        "pix_qr_code": f"data:image/svg+xml;charset=utf-8,{quote(qr_svg)}",
    }


def normalize_payment_method(raw_value: str | None) -> str | None:
    value = sanitize_optional_text(raw_value, limit=16).lower()
    if not value:
        return None
    return value if value in PAYMENT_METHOD_LABELS else None


def normalize_payment_status(raw_value: str | None, fallback: str = "pending") -> str:
    value = sanitize_text(raw_value, fallback, limit=16).lower()
    return value if value in PAYMENT_STATUS_LABELS else fallback


def get_payment_method_label(method: str | None) -> str:
    return PAYMENT_METHOD_LABELS.get(method or "", "Pagar no balcao")


def get_payment_status_label(status: str | None) -> str:
    return PAYMENT_STATUS_LABELS.get(status or "", "Pendente")


def get_order_status_label(status: str | None) -> str:
    return ORDER_STATUS_LABELS.get(status or "", "Pedido criado")


def normalize_request_id(raw_value: str | None) -> str | None:
    value = sanitize_optional_text(raw_value, limit=128)
    return value or None


def normalize_order_type(raw_value: str | None, fallback: str = "pista") -> str | None:
    value = sanitize_optional_text(raw_value, limit=24).lower()
    if not value:
        return fallback
    return value if value in ORDER_TYPE_LABELS else None


def get_order_type_label(order_type: str | None) -> str:
    return ORDER_TYPE_LABELS.get(order_type or "", ORDER_TYPE_LABELS["pista"])


def attach_order_context(
    *,
    order_code: str | None = None,
    order_type: str | None = None,
    payment_method: str | None = None,
    shift_id: int | None = None,
) -> None:
    if not SENTRY_DSN:
        return

    context: dict[str, str | int] = {}
    if order_code:
        context["order_code"] = order_code
        sentry_sdk.set_tag("order_code", order_code)
    if order_type:
        context["order_type"] = order_type
        sentry_sdk.set_tag("order_type", order_type)
    if payment_method:
        context["payment_method"] = payment_method
        sentry_sdk.set_tag("payment_method", payment_method)
    if shift_id is not None:
        context["shift_id"] = shift_id
        sentry_sdk.set_tag("shift_id", str(shift_id))

    if context:
        sentry_sdk.set_context("order", context)


def fetch_order_row_by_code(code: str, shift_id: int | None = None) -> sqlite3.Row | None:
    query = """
        SELECT
            id,
            codigo_retirada,
            turno_id,
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
            order_type,
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


def fetch_order_row_by_request_id(request_id: str) -> sqlite3.Row | None:
    return get_db().execute(
        """
        SELECT
            id,
            codigo_retirada,
            turno_id,
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
            order_type,
            payment_provider,
            payment_provider_id,
            paid_at,
            pix_qr_code,
            pix_copy_paste
        FROM pedidos
        WHERE request_id = ?
        """,
        (request_id,),
    ).fetchone()


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
                WHEN payment_method = 'pix' AND status = ? THEN 'new'
                ELSE status
            END
        WHERE id = ?
        """,
        (paid_at, AWAITING_PAYMENT_STATUS, row["id"]),
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
    cached_shift_id = getattr(g, "current_shift_id", None)
    if cached_shift_id is not None:
        return cached_shift_id

    db = get_db()
    row = db.execute(
        "SELECT id FROM turnos WHERE status = 'open' ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row:
        g.current_shift_id = row["id"]
        return row["id"]

    row = db.execute(
        """
        INSERT INTO turnos (aberto_em, status)
        VALUES (?, 'open')
        RETURNING id
        """,
        (utc_now_iso(),),
    ).fetchone()
    db.commit()
    g.current_shift_id = row["id"]
    return row["id"]


def open_new_shift(*, commit: bool = True) -> int:
    db = get_db()
    row = db.execute(
        """
        INSERT INTO turnos (aberto_em, status)
        VALUES (?, 'open')
        RETURNING id
        """,
        (utc_now_iso(),),
    ).fetchone()
    db.execute("UPDATE shift_notes SET status = 'open'")
    if commit:
        db.commit()
    return row["id"]


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
            is_combo,
            max_active_orders
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


def parse_preorder_time_value(raw_value: str | None) -> str | None:
    cleaned = (raw_value or "").strip()
    if not cleaned:
        return None
    try:
        datetime.strptime(cleaned, "%H:%M")
    except ValueError:
        raise ValueError("Horario de pre-order invalido. Use HH:MM.")
    return cleaned


def fetch_preorder_settings() -> dict:
    row = get_db().execute(
        """
        SELECT preorder_start_time, preorder_end_time, updated_at
        FROM preorder_settings
        WHERE id = 1
        """
    ).fetchone()
    start_time = row["preorder_start_time"] if row else None
    end_time = row["preorder_end_time"] if row else None
    status = resolve_preorder_window_status(start_time, end_time)
    return {
        "start_time": start_time or "",
        "end_time": end_time or "",
        "updated_at": display_datetime(row["updated_at"]) if row and row["updated_at"] else "-",
        **status,
    }


def resolve_preorder_window_status(start_time: str | None, end_time: str | None) -> dict:
    if not start_time or not end_time:
        return {
            "is_configured": False,
            "status": "disabled",
            "status_label": "Sem configuracao",
            "status_note": "Sem janela configurada, o sistema segue normal.",
        }

    current_value = local_now().strftime("%H:%M")
    if current_value < start_time:
        return {
            "is_configured": True,
            "status": "not_started",
            "status_label": "Ainda nao comecou",
            "status_note": f"Pre-order abre as {start_time}.",
        }
    if current_value > end_time:
        return {
            "is_configured": True,
            "status": "closed",
            "status_label": "Fechado",
            "status_note": f"Pre-order encerrou as {end_time}.",
        }
    return {
        "is_configured": True,
        "status": "open",
        "status_label": "Aberto",
        "status_note": f"Recebendo pre-orders ate {end_time}.",
    }


def fetch_active_preorder_counts() -> dict[int, int]:
    placeholders = ",".join("?" for _ in PREORDER_ACTIVE_STATUSES)
    rows = get_db().execute(
        f"""
        SELECT
            ip.bebida_id,
            COALESCE(SUM(ip.quantidade), 0) AS total
        FROM itens_pedido ip
        JOIN pedidos p ON p.id = ip.pedido_id
        WHERE p.status IN ({placeholders})
        GROUP BY ip.bebida_id
        """,
        PREORDER_ACTIVE_STATUSES,
    ).fetchall()
    return {row["bebida_id"]: int(row["total"] or 0) for row in rows}


def preorder_availability(
    beverage_row: sqlite3.Row | dict,
    *,
    requested_quantity: int = 1,
    active_counts: dict[int, int] | None = None,
    preorder_settings: dict | None = None,
) -> tuple[bool, str, str | None]:
    settings = preorder_settings or fetch_preorder_settings()
    if not settings.get("is_configured"):
        return True, "available", None

    status = settings.get("status")
    if status == "not_started":
        return False, "available_soon", settings.get("status_note")
    if status != "open":
        return False, "unavailable_now", settings.get("status_note")

    row_data = dict(beverage_row)
    max_active_orders = row_data.get("max_active_orders")
    if not max_active_orders:
        return True, "available", settings.get("status_note")

    counts = active_counts or fetch_active_preorder_counts()
    current_active = int(counts.get(int(row_data["id"]), 0))
    if current_active + requested_quantity > int(max_active_orders):
        return (
            False,
            "unavailable_now",
            f"Capacidade de pre-order atingida ({current_active}/{int(max_active_orders)} em preparo).",
        )

    return True, "available", f"{current_active}/{int(max_active_orders)} ativos no momento."


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
        "max_active_orders": int(row_data["max_active_orders"]) if row_data.get("max_active_orders") else None,
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
    *,
    requested_quantity: int = 1,
    active_counts: dict[int, int] | None = None,
    preorder_settings: dict | None = None,
) -> tuple[bool, str | None, str]:
    if not beverage_row["is_active"]:
        return False, "Item inativo no cardapio.", "unavailable_now"

    preorder_allowed, preorder_state, preorder_note = preorder_availability(
        beverage_row,
        requested_quantity=requested_quantity,
        active_counts=active_counts,
        preorder_settings=preorder_settings,
    )
    if not preorder_allowed:
        return False, preorder_note or "Pre-order indisponivel no momento.", preorder_state

    required, error = build_item_requirements(
        beverage_row,
        requested_quantity,
        beverages_by_id,
        combo_components_map,
    )
    if error:
        return False, f"Indisponivel: {error}", "unavailable_now"
    if not required:
        return True, preorder_note, preorder_state

    for ingredient_name, amount in required.items():
        inventory_item = inventory_by_name.get(ingredient_name)
        if not inventory_item:
            return False, f"Indisponivel: {ingredient_name} nao cadastrado.", "unavailable_now"
        if inventory_item["stock_level"] < amount:
            return False, f"Indisponivel por estoque de {ingredient_name}.", "unavailable_now"

    return True, preorder_note, preorder_state


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


def reserve_stock_deductions(
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
        "SELECT id, name, stock_level, par_level, unit FROM inventory_items WHERE name IN ({})".format(
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
        if float(row["stock_level"] or 0) < needed:
            shortages.append(
                {
                    "item": ingredient_name,
                    "needed": needed,
                    "available": row["stock_level"],
                    "unit": row["unit"],
                }
            )

    if shortages:
        return shortages

    updated_at = utc_now_iso()
    for ingredient_name, needed in required.items():
        row = by_name[ingredient_name]
        next_stock = round(float(row["stock_level"] or 0) - needed, 4)
        next_status = calculate_inventory_status(next_stock, row["par_level"])
        result = db.execute(
            """
            UPDATE inventory_items
            SET stock_level = ?, status = ?, updated_at = ?
            WHERE id = ? AND stock_level >= ?
            """,
            (next_stock, next_status, updated_at, row["id"], needed),
        )
        if result.rowcount != 1:
            current_row = db.execute(
                "SELECT stock_level, unit FROM inventory_items WHERE id = ?",
                (row["id"],),
            ).fetchone()
            return [
                {
                    "item": ingredient_name,
                    "needed": needed,
                    "available": current_row["stock_level"] if current_row else 0,
                    "unit": current_row["unit"] if current_row else row["unit"],
                }
            ]

    return []


def build_menu_snapshot() -> list[dict]:
    inventory_by_name = get_inventory_by_name()
    rows = fetch_beverage_rows(include_inactive=False)
    beverages_by_id = fetch_beverage_map(include_inactive=True)
    combo_components_map = fetch_combo_components_map([row["id"] for row in rows if row["is_combo"]])
    preorder_settings = fetch_preorder_settings()
    active_counts = fetch_active_preorder_counts()
    menu = []
    for row in rows:
        menu_item = get_beverage_display_data(row)
        is_available, availability_note, availability_state = beverage_availability(
            row,
            inventory_by_name,
            beverages_by_id,
            combo_components_map,
            requested_quantity=1,
            active_counts=active_counts,
            preorder_settings=preorder_settings,
        )
        menu_item.update(
            {
                "is_available": is_available,
                "availability_note": availability_note,
                "availability_state": availability_state,
                "availability_state_label": {
                    "available": "Disponivel",
                    "available_soon": "Disponivel em breve",
                    "unavailable_now": "Indisponivel no momento",
                }.get(availability_state, "Disponivel"),
            }
        )
        menu.append(menu_item)
    return menu


def fetch_menu() -> list[dict]:
    cache_key = "menu"
    cached_value = read_snapshot_cache(cache_key)
    if cached_value is not None:
        return cached_value
    return write_snapshot_cache(cache_key, SNAPSHOT_CACHE_TTLS["menu"], build_menu_snapshot())


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
    active_counts = fetch_active_preorder_counts()
    products = []
    combos = []
    base_products = []

    for row in rows:
        product = get_beverage_display_data(row)
        product["raw_description"] = row["descricao"] or ""
        product["raw_image_url"] = row["imagem_url"] or ""
        product["raw_max_active_orders"] = row["max_active_orders"] or ""
        product["active_order_count"] = int(active_counts.get(row["id"], 0))
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


def build_preorder_redirect(message: str | None = None, error: str | None = None):
    params = {}
    if message:
        params["message"] = message
    if error:
        params["error"] = error
    return redirect(url_for("preorder_page", **params))


def fetch_preorder_dashboard_snapshot() -> dict:
    settings = fetch_preorder_settings()
    active_counts = fetch_active_preorder_counts()
    rows = fetch_beverage_rows(include_inactive=True)
    tracked_products = []
    for row in rows:
        product = get_beverage_display_data(row)
        product["active_order_count"] = int(active_counts.get(row["id"], 0))
        if product["max_active_orders"] or product["active_order_count"]:
            tracked_products.append(product)

    tracked_products.sort(
        key=lambda item: (
            0 if item["max_active_orders"] else 1,
            item["name"].lower(),
        )
    )

    return {
        "settings": settings,
        "tracked_products": tracked_products,
        "active_total": sum(active_counts.values()),
    }


def save_preorder_settings_from_form() -> str:
    db = get_db()
    start_time = parse_preorder_time_value(request.form.get("preorder_start_time"))
    end_time = parse_preorder_time_value(request.form.get("preorder_end_time"))

    if bool(start_time) != bool(end_time):
        raise ValueError("Preencha inicio e fim do pre-order para ativar a janela.")
    if start_time and end_time and start_time >= end_time:
        raise ValueError("O horario final precisa ser depois do horario inicial.")

    db.execute(
        """
        INSERT INTO preorder_settings (id, preorder_start_time, preorder_end_time, updated_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            preorder_start_time = excluded.preorder_start_time,
            preorder_end_time = excluded.preorder_end_time,
            updated_at = excluded.updated_at
        """,
        (start_time, end_time, utc_now_iso()),
    )
    return "Configuracao de pre-order salva com sucesso."


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


def fetch_order_items_map(order_ids: list[int]) -> dict[int, list[DbRow]]:
    if not order_ids:
        return {}

    placeholders = ",".join("?" for _ in order_ids)
    rows = get_db().execute(
        """
        SELECT
            ip.pedido_id,
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
        WHERE ip.pedido_id IN (""" + placeholders + """)
        ORDER BY ip.pedido_id ASC, ip.id ASC
        """,
        order_ids,
    ).fetchall()
    items_by_order_id: dict[int, list[DbRow]] = {}
    for item_row in rows:
        items_by_order_id.setdefault(item_row["pedido_id"], []).append(item_row)
    return items_by_order_id


def serialize_order(row: sqlite3.Row, items: list[DbRow] | None = None) -> dict:
    if items is None:
        items = fetch_order_items_map([row["id"]]).get(row["id"], [])

    return {
        "id": row["id"],
        "code": row["codigo_retirada"],
        "order_number": row["order_number"] if "order_number" in row.keys() and row["order_number"] else row["codigo_retirada"],
        "status": row["status"],
        "status_label": get_order_status_label(row["status"] if "status" in row.keys() else None),
        "released_to_bar": (row["status"] if "status" in row.keys() else "") in (*ACTIVE_ORDER_STATUSES, "completed"),
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
        "order_type": normalize_order_type(row["order_type"] if "order_type" in row.keys() else "pista") or "pista",
        "order_type_label": get_order_type_label(
            normalize_order_type(row["order_type"] if "order_type" in row.keys() else "pista") or "pista"
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
            order_type,
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
        elif status == "awaiting_payment":
            conditions.append("status = ?")
            params.append(AWAITING_PAYMENT_STATUS)
        else:
            conditions.append("status = ?")
            params.append(status)
    query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY horario_pedido DESC"
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    rows = get_db().execute(query, params).fetchall()
    if not rows:
        return []

    items_by_order_id = fetch_order_items_map([row["id"] for row in rows])
    return [serialize_order(row, items_by_order_id.get(row["id"], [])) for row in rows]


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


def build_logistics_snapshot() -> dict:
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


def fetch_logistics_snapshot() -> dict:
    cache_key = "logistics"
    cached_value = read_snapshot_cache(cache_key)
    if cached_value is not None:
        return cached_value
    return write_snapshot_cache(cache_key, SNAPSHOT_CACHE_TTLS["logistics"], build_logistics_snapshot())


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
        local_dt = datetime.fromisoformat(row["horario_pedido"]).astimezone(ZoneInfo("America/Sao_Paulo"))
        bucket_start = local_dt.replace(minute=0, second=0, microsecond=0)
        bucket = buckets.setdefault(bucket_start, {"orders": 0, "revenue": 0.0})
        bucket["orders"] += 1
        if normalize_payment_status(row["payment_status"], "pending") == "paid":
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
            COALESCE(SUM(CASE WHEN payment_status = 'paid' THEN valor_total ELSE 0 END), 0) AS total_recebido,
            COALESCE(SUM(CASE WHEN payment_status = 'paid' THEN 1 ELSE 0 END), 0) AS total_pedidos_pagos
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
            COALESCE(SUM(CASE WHEN p.payment_status = 'paid' THEN ip.subtotal ELSE 0 END), 0) AS total_recebido,
            COALESCE(
                SUM(
                    CASE
                        WHEN p.payment_status = 'paid' THEN COALESCE(ip.unit_cost_snapshot, b.custo_estimado) * ip.quantidade
                        ELSE 0
                    END
                ),
                0
            ) AS custo_estimado
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

    total_recebido = round(float(totals["total_recebido"] or 0), 2)
    total_pedidos = int(totals["total_pedidos"] or 0)
    total_pedidos_pagos = int(totals["total_pedidos_pagos"] or 0)
    custo_estimado = round(
        sum(float(row["custo_estimado"] or 0) for row in ranking_rows),
        2,
    )
    ticket_medio = round(total_recebido / total_pedidos_pagos, 2) if total_pedidos_pagos else 0.0

    ranking_bebidas = [
        {
            "id": row["beverage_id"],
            "name": row["nome"],
            "quantity": int(row["quantidade"] or 0),
            "revenue": round(float(row["total_recebido"] or 0), 2),
            "cost": round(float(row["custo_estimado"] or 0), 2),
        }
        for row in ranking_rows
    ]
    bebida_mais_vendida = ranking_bebidas[0] if ranking_bebidas else None

    return {
        "total_vendido": total_recebido,
        "total_recebido": total_recebido,
        "total_pedidos": total_pedidos,
        "total_pedidos_pagos": total_pedidos_pagos,
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
        "lucro_estimado": round(total_recebido - custo_estimado, 2),
        "observacoes": parse_shift_observations(stored_summary),
    }


def get_shift_closeout_blockers(shift_id: int) -> dict:
    row = get_db().execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN status IN (?, ?) THEN 1 ELSE 0 END), 0) AS pending_orders,
            COALESCE(SUM(CASE WHEN COALESCE(payment_status, 'pending') != 'paid' THEN 1 ELSE 0 END), 0) AS unpaid_orders,
            COALESCE(
                SUM(
                    CASE
                        WHEN payment_method = 'pix' AND COALESCE(payment_status, 'pending') != 'paid' THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS unpaid_pix_orders
        FROM pedidos
        WHERE turno_id = ?
        """,
        (*ACTIVE_ORDER_STATUSES, shift_id),
    ).fetchone()
    return {
        "pending_orders": int(row["pending_orders"] or 0),
        "unpaid_orders": int(row["unpaid_orders"] or 0),
        "unpaid_pix_orders": int(row["unpaid_pix_orders"] or 0),
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
            "Este turno manteve o mesmo total recebido do comparado."
            if revenue_comparison["indicator"] == "sem mudanca relevante"
            else f'Este turno {revenue_comparison["indicator"]} no total recebido em relacao ao comparado.'
        )
    else:
        revenue_part = f'Este turno recebeu {abs(revenue_pct):.1f}% {"a mais" if revenue_pct > 0 else "a menos"} que o comparado.'

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
        "Total recebido",
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
        "Valor recebido no pico",
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
        "total_recebido": metrics["total_recebido"],
        "total_pedidos": metrics["total_pedidos"],
        "total_pedidos_pagos": metrics["total_pedidos_pagos"],
        "quantidade_por_bebida": metrics["quantidade_por_bebida"],
        "bebida_mais_pedida": metrics["bebida_mais_pedida"],
        "pico_atendimento": metrics["pico_atendimento"],
    }


def build_order_summary_payload(shift_id: int) -> dict:
    report = build_sales_report(shift_id)
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
        (shift_id,),
    ).fetchall()
    pending = get_db().execute(
        "SELECT COUNT(*) AS total FROM pedidos WHERE status IN (?, ?) AND turno_id = ?",
        (*ACTIVE_ORDER_STATUSES, shift_id),
    ).fetchone()["total"]
    completed = get_db().execute(
        "SELECT COUNT(*) AS total FROM pedidos WHERE status = 'completed' AND turno_id = ?",
        (shift_id,),
    ).fetchone()["total"]
    awaiting_payment = get_db().execute(
        "SELECT COUNT(*) AS total FROM pedidos WHERE status = ? AND turno_id = ?",
        (AWAITING_PAYMENT_STATUS, shift_id),
    ).fetchone()["total"]
    average_ticket = 0.0
    if report["total_pedidos_pagos"]:
        average_ticket = round(report["total_recebido"] / report["total_pedidos_pagos"], 2)
    return {
        "pending_count": pending or 0,
        "completed_count": completed or 0,
        "awaiting_payment_count": awaiting_payment or 0,
        "total_count": report["total_pedidos"],
        "revenue": report["total_recebido"],
        "average_ticket": average_ticket,
        "top_items": report["quantidade_por_bebida"][:4],
        "peak_time_label": report["pico_atendimento"]["label"] if report["pico_atendimento"] else "Sem dados",
        "peak_order_count": report["pico_atendimento"]["orders"] if report["pico_atendimento"] else 0,
        "top_tables": [
            {"table_label": row["table_label"], "total": row["total"]}
            for row in top_tables
        ],
    }


def build_order_summary(shift_id: int | None = None) -> dict:
    current_shift_id = shift_id or get_current_shift_id()
    cache_key = f"order-summary:{current_shift_id}"
    cached_value = read_snapshot_cache(cache_key)
    if cached_value is not None:
        return cached_value
    return write_snapshot_cache(
        cache_key,
        SNAPSHOT_CACHE_TTLS["order_summary"],
        build_order_summary_payload(current_shift_id),
    )


def build_closeout_report(shift_id: int | None = None) -> dict:
    current_shift_id = shift_id or get_current_shift_id()
    metrics = build_shift_metrics(current_shift_id)
    return metrics


def archive_current_shift_and_open_next(expected_shift_id: int | None = None) -> dict:
    db = get_db()
    try:
        current_shift_id = get_current_shift_id()
        if expected_shift_id is not None and expected_shift_id != current_shift_id:
            safe_rollback(db)
            raise ValueError("O turno ativo mudou. Atualize o painel antes de tentar fechar novamente.")

        blockers = get_shift_closeout_blockers(current_shift_id)
        if blockers["pending_orders"]:
            safe_rollback(db)
            raise ValueError(
                f"Nao e possivel fechar o turno: existem {blockers['pending_orders']} pedidos pendentes."
            )
        if blockers["unpaid_orders"]:
            safe_rollback(db)
            if blockers["unpaid_pix_orders"]:
                raise ValueError(
                    f"Nao e possivel fechar o turno: existem {blockers['unpaid_pix_orders']} pedidos Pix sem confirmacao."
                )
            raise ValueError(
                f"Nao e possivel fechar o turno: existem {blockers['unpaid_orders']} pedidos sem pagamento confirmado."
            )

        report = build_closeout_report(current_shift_id)
        closed_at = utc_now_iso()
        result = db.execute(
            """
            UPDATE turnos
            SET status = 'closed', fechado_em = ?, resumo_fechamento = ?
            WHERE id = ? AND status = 'open'
            """,
            (closed_at, json.dumps(report), current_shift_id),
        )
        if result.rowcount != 1:
            safe_rollback(db)
            raise ValueError("O turno nao pode ser fechado novamente. Atualize o painel.")

        new_shift_id = open_new_shift(commit=False)
        db.commit()
    except Exception:
        safe_rollback(db)
        raise

    return {
        "closed_shift_id": current_shift_id,
        "new_shift_id": new_shift_id,
        "current_shift_id": new_shift_id,
        "report": report,
        "summary": build_order_summary(new_shift_id),
        "awaiting_payment": fetch_orders(status="awaiting_payment", shift_id=new_shift_id),
        "pending": fetch_orders(status="pending", shift_id=new_shift_id),
        "completed": fetch_orders(status="completed", limit=20, shift_id=new_shift_id),
        "logistics": fetch_logistics_snapshot(),
        "shifts": fetch_shift_history(),
        "generated_at": display_datetime(utc_now_iso()),
    }


def build_shift_history_payload(limit: int) -> list[dict]:
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


def fetch_shift_history(limit: int = 10) -> list[dict]:
    cache_key = f"shift-history:{limit}"
    cached_value = read_snapshot_cache(cache_key)
    if cached_value is not None:
        return cached_value
    return write_snapshot_cache(
        cache_key,
        SNAPSHOT_CACHE_TTLS["shift_history"],
        build_shift_history_payload(limit),
    )


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
    writer.writerow(["Total recebido", summary.get("total_recebido", summary.get("total_vendido", 0))])
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
        writer.writerow(["Valor recebido no pico", summary["pico_atendimento"].get("revenue", 0)])

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
        ["Total recebido", currency_brl(summary.get("total_recebido", summary["total_vendido"])), "Total de pedidos", str(summary["total_pedidos"])],
        ["Ticket medio", currency_brl(summary["ticket_medio"]), "Custo estimado", currency_brl(summary["custo_estimado"])],
        ["Lucro estimado", currency_brl(summary["lucro_estimado"]), "Bebida mais vendida", escape((summary.get("bebida_mais_vendida") or {}).get("name", "Sem dados"))],
        ["Horario de pico", escape((summary.get("horario_pico") or {}).get("label", "Sem dados")), "Pedidos no pico", str(summary.get("quantidade_pedidos_pico", 0))],
        ["Valor recebido no pico", currency_brl(summary.get("valor_vendido_pico", 0)), "Itens vendidos", str(summary.get("total_itens_vendidos", 0))],
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
        top_items_data = [["Bebida", "Quantidade", "Total recebido", "Custo"]]
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
        top_items_data = [["Bebida", "Quantidade", "Total recebido", "Custo"], ["Sem vendas registradas", "-", "-", "-"]]
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
    category = normalize_product_category(request.form.get("category"))
    description = sanitize_optional_text(request.form.get("description"), limit=280)
    image_url = normalize_product_image(request.form.get("image_url"))
    is_active = 1 if checkbox_to_bool(request.form.get("is_active")) else 0
    max_active_orders = parse_optional_integer_input(
        request.form.get("max_active_orders"),
        "Capacidade maxima",
        minimum=1,
    )

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
            SET nome = ?, preco_venda = ?, custo_estimado = ?, categoria = ?, descricao = ?, imagem_url = ?, is_active = ?, max_active_orders = ?
            WHERE id = ?
            """,
            (name, price, cost, category, description, image_url, is_active, max_active_orders, product_id),
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
            is_combo,
            max_active_orders
        )
        VALUES (?, ?, ?, ?, ?, '3 min', ?, ?, 0, ?)
        """,
        (name, price, cost, category, description, image_url, is_active, max_active_orders),
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
    max_active_orders = parse_optional_integer_input(
        request.form.get("max_active_orders"),
        "Capacidade maxima",
        minimum=1,
    )
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
            SET nome = ?, preco_venda = ?, custo_estimado = ?, descricao = ?, imagem_url = ?, is_active = ?, max_active_orders = ?
            WHERE id = ?
            """,
            (name, price, estimated_cost, description, image_url, is_active, max_active_orders, combo_id),
        )
        db.execute("DELETE FROM combo_items WHERE combo_beverage_id = ?", (combo_id,))
        target_combo_id = combo_id
        success_message = "Combo atualizado com sucesso."
    else:
        row = db.execute(
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
                is_combo,
                max_active_orders
            )
            VALUES (?, ?, ?, 'Combo', ?, '4 min', ?, ?, 1, ?)
            RETURNING id
            """,
            (name, price, estimated_cost, description, image_url, is_active, max_active_orders),
        ).fetchone()
        target_combo_id = row["id"]
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
        order_types=[
            {"value": value, "label": label}
            for value, label in ORDER_TYPE_LABELS.items()
        ],
        payment_methods=[
            {"value": value, "label": label}
            for value, label in PAYMENT_METHOD_LABELS.items()
        ],
    )


def build_dashboard_redirect(message: str | None = None, error: str | None = None):
    params = {}
    if message:
        params["auth_message"] = message
    if error:
        params["auth_error"] = error
    return redirect(url_for("dashboard", **params))


def handle_staff_login():
    if request.method == "GET" and is_staff_authenticated():
        return redirect(url_for("dashboard"))

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = authenticate_staff_user(username, password)
        if user:
            begin_staff_session(user)
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
    current_user = get_current_user()
    return render_template(
        "dashboard.html",
        summary=build_order_summary(),
        logistics=fetch_logistics_snapshot(),
        shifts=fetch_shift_history(limit=5),
        preorder=fetch_preorder_settings(),
        current_shift_id=get_current_shift_id(),
        current_user=current_user,
        staff_settings=fetch_staff_settings_snapshot() if current_user["can_manage_bar"] else None,
        auth_message=request.args.get("auth_message"),
        auth_error=request.args.get("auth_error"),
    )


@app.post("/painel/equipe/credenciais")
@login_required
@role_required("admin")
def save_staff_credentials():
    action = request.form.get("account_action", "").strip()
    try:
        if action == "admin_credentials":
            message = update_admin_credentials(session.get("bar_user_id"), request.form)
        elif action == "operator_account":
            message = upsert_operator_account(request.form)
        else:
            raise ValueError("Acao de credenciais invalida.")
        get_db().commit()
        return build_dashboard_redirect(message=message)
    except ValueError as error:
        get_db().rollback()
        return build_dashboard_redirect(error=str(error))


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
        product_category_options=PRODUCT_CATEGORY_OPTIONS,
        current_user=get_current_user(),
        message=request.args.get("message"),
        error=request.args.get("error"),
    )


@app.get("/painel/pre-order")
@login_required
@role_required("admin")
def preorder_page():
    snapshot = fetch_preorder_dashboard_snapshot()
    return render_template(
        "preorder.html",
        preorder=snapshot["settings"],
        tracked_products=snapshot["tracked_products"],
        active_total=snapshot["active_total"],
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
    except IntegrityError:
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
    except IntegrityError:
        get_db().rollback()
        return build_products_redirect(error="Ja existe um item com esse nome.")
    except ValueError as error:
        get_db().rollback()
        return build_products_redirect(error=str(error))


@app.post("/painel/pre-order/salvar")
@login_required
@role_required("admin")
def save_preorder():
    try:
        message = save_preorder_settings_from_form()
        get_db().commit()
        return build_preorder_redirect(message=message)
    except ValueError as error:
        get_db().rollback()
        return build_preorder_redirect(error=str(error))


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
    current_shift_id = get_current_shift_id()
    return jsonify(
        {
            "current_shift_id": current_shift_id,
            "summary": build_order_summary(current_shift_id),
            "awaiting_payment": fetch_orders(status="awaiting_payment", shift_id=current_shift_id),
            "pending": fetch_orders(status="pending", shift_id=current_shift_id),
            "completed": fetch_orders(status="completed", limit=20, shift_id=current_shift_id),
            "logistics": fetch_logistics_snapshot(),
            "shifts": fetch_shift_history(limit=5),
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
    request_id = normalize_request_id(request.headers.get("Idempotency-Key") or payload.get("request_id"))
    if not request_id:
        return jsonify({"error": "request_id obrigatorio para criar o pedido."}), 400

    selected_items, validation_error = validate_order_payload(payload.get("items") or [])
    if validation_error:
        return jsonify({"error": validation_error}), 400

    db = get_db()
    customer_name = sanitize_text(payload.get("customer_name"), "Cliente", limit=48)
    table_label = sanitize_text(payload.get("table_label"), "Retirada", limit=32)
    source = sanitize_text(payload.get("source"), DEFAULT_ORDER_SOURCE, limit=24)
    order_type = normalize_order_type(payload.get("order_type"))
    raw_payment_method = payload.get("payment_method")
    payment_method = normalize_payment_method(raw_payment_method)
    app.logger.info(
        "create_order raw_payment_method=%s normalized_payment_method=%s source=%s customer=%s",
        raw_payment_method,
        payment_method,
        source,
        customer_name,
    )
    if not payment_method:
        return jsonify({"error": "Escolha uma forma de pagamento valida antes de enviar o pedido."}), 400
    if not order_type:
        return jsonify({"error": "Escolha o tipo do pedido antes de enviar."}), 400

    payment_status = "pending"
    order_status = AWAITING_PAYMENT_STATUS if payment_method == "pix" else "new"
    payment_provider = sanitize_optional_text(payload.get("payment_provider"), limit=64) or None
    payment_provider_id = sanitize_optional_text(payload.get("payment_provider_id"), limit=128) or None
    pix_qr_code = sanitize_optional_text(payload.get("pix_qr_code"), limit=4000) or None
    pix_copy_paste = sanitize_optional_text(payload.get("pix_copy_paste"), limit=4000) or None
    if payment_method != "pix":
        payment_provider = None
        payment_provider_id = None
        pix_qr_code = None
        pix_copy_paste = None
    try:
        turno_id = get_current_shift_id()
        attach_order_context(
            order_type=order_type,
            payment_method=payment_method,
            shift_id=turno_id,
        )
        existing_order = fetch_order_row_by_request_id(request_id)
        if existing_order:
            db.rollback()
            return (
                jsonify(
                    {
                        "order": serialize_order(existing_order),
                        "summary": build_order_summary(existing_order["turno_id"]),
                    }
                ),
                200,
            )

        bebidas_por_id = fetch_beverage_map(include_inactive=True)
        combo_components_map = fetch_combo_components_map()
        inventory_by_name = get_inventory_by_name()
        preorder_settings = fetch_preorder_settings()
        active_preorder_counts = fetch_active_preorder_counts()

        itens = []
        valor_total = 0.0
        for entry in selected_items:
            bebida_id = entry["id"]
            quantidade = entry["quantity"]
            bebida = bebidas_por_id.get(bebida_id)
            if not bebida or quantidade <= 0 or not bebida["is_active"]:
                continue
            is_available, availability_note, _ = beverage_availability(
                bebida,
                inventory_by_name,
                bebidas_por_id,
                combo_components_map,
                requested_quantity=quantidade,
                active_counts=active_preorder_counts,
                preorder_settings=preorder_settings,
            )
            if not is_available:
                db.rollback()
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
            db.rollback()
            return jsonify({"error": "Itens invalidos."}), 400

        shortages = reserve_stock_deductions(itens, bebidas_por_id, combo_components_map)
        if shortages:
            readable = ", ".join(
                f'{entry["item"]} ({entry["available"]}/{entry["needed"]} {entry["unit"]})'.strip()
                for entry in shortages
            )
            db.rollback()
            return jsonify(
                {
                    "error": f"Estoque insuficiente para concluir o pedido: {readable}",
                    "shortages": shortages,
                }
            ), 400

        codigo = generate_order_code()
        order_number = generate_order_number(codigo)
        if payment_method == "pix":
            pix_payload = build_fake_pix_payload(codigo, valor_total)
            payment_provider = pix_payload["payment_provider"]
            payment_provider_id = pix_payload["payment_provider_id"]
            pix_qr_code = pix_payload["pix_qr_code"]
            pix_copy_paste = pix_payload["pix_copy_paste"]
            app.logger.info(
                "create_order pix_branch code=%s provider=%s provider_id=%s copy_len=%s has_qr=%s",
                codigo,
                payment_provider,
                payment_provider_id,
                len(pix_copy_paste or ""),
                bool(pix_qr_code),
            )
        else:
            app.logger.info("create_order counter_branch code=%s", codigo)
        horario = utc_now_iso()
        attach_order_context(
            order_code=codigo,
            order_type=order_type,
            payment_method=payment_method,
            shift_id=turno_id,
        )

        pedido_row = db.execute(
            """
            INSERT INTO pedidos (
                codigo_retirada,
                order_number,
                horario_pedido,
                status,
                valor_total,
                turno_id,
                customer_name,
                table_label,
                source,
                payment_method,
                payment_status,
                order_type,
                payment_provider,
                payment_provider_id,
                pix_qr_code,
                pix_copy_paste,
                request_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (
                codigo,
                order_number,
                horario,
                order_status,
                round(valor_total, 2),
                turno_id,
                customer_name,
                table_label,
                source,
                payment_method,
                payment_status,
                order_type,
                payment_provider,
                payment_provider_id,
                pix_qr_code,
                pix_copy_paste,
                request_id,
            ),
        ).fetchone()
        pedido_id = pedido_row["id"]

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
        db.commit()
        invalidate_snapshot_cache("menu", "logistics", "order-summary")
    except IntegrityError as exc:
        safe_rollback(db)
        existing_order = fetch_order_row_by_request_id(request_id)
        if existing_order:
            return (
                jsonify(
                    {
                        "order": serialize_order(existing_order),
                        "summary": build_order_summary(existing_order["turno_id"]),
                    }
                ),
                200,
            )
        sentry_sdk.capture_exception(exc)
        return jsonify({"error": "Nao foi possivel registrar o pedido agora. Tente novamente."}), 500
    except Exception as exc:
        safe_rollback(db)
        sentry_sdk.capture_exception(exc)
        return jsonify({"error": "Nao foi possivel criar o pedido agora. Tente novamente."}), 500

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
            order_type,
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
        current_shift_id = get_current_shift_id()
        attach_order_context(order_code=code, shift_id=current_shift_id)
        row = fetch_order_row_by_code(code, current_shift_id)
        if not row:
            return jsonify({"error": "Pedido nao encontrado."}), 404
        attach_order_context(
            order_code=code,
            order_type=normalize_order_type(row["order_type"]) or "pista",
            payment_method=normalize_payment_method(row["payment_method"]) or "counter",
            shift_id=current_shift_id,
        )
        order = mark_as_paid(code, current_shift_id)
        invalidate_snapshot_cache("order-summary")
    except LookupError:
        return jsonify({"error": "Pedido nao encontrado."}), 404
    except Exception as exc:
        sentry_sdk.capture_exception(exc)
        return jsonify({"error": "Nao foi possivel confirmar o pagamento agora. Tente novamente."}), 500
    return jsonify({"order": order, "summary": build_order_summary()})


@app.post("/api/orders/<code>/complete")
@login_required
def complete_order(code: str):
    db = get_db()
    try:
        current_shift_id = get_current_shift_id()
        attach_order_context(order_code=code, shift_id=current_shift_id)
        row = fetch_order_row_by_code(code, current_shift_id)
        if not row:
            return jsonify({"error": "Pedido nao encontrado."}), 404
        attach_order_context(
            order_code=code,
            order_type=normalize_order_type(row["order_type"]) or "pista",
            payment_method=normalize_payment_method(row["payment_method"]) or "counter",
            shift_id=current_shift_id,
        )
        if row["status"] == "completed":
            return jsonify({"order": serialize_order(row), "summary": build_order_summary()})
        if row["status"] == AWAITING_PAYMENT_STATUS:
            return jsonify({"error": "Esse pedido ainda aguarda confirmacao do Pix antes de ser liberado para o bar."}), 400

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
        invalidate_snapshot_cache("order-summary")

        updated = fetch_order_row_by_code(code, current_shift_id)
        return jsonify({"order": serialize_order(updated), "summary": build_order_summary()})
    except Exception as exc:
        safe_rollback(db)
        sentry_sdk.capture_exception(exc)
        return jsonify({"error": "Nao foi possivel concluir o pedido agora. Tente novamente."}), 500


@app.post("/api/reports/closeout")
@login_required
@role_required("admin")
def closeout_report():
    payload = request.get_json(silent=True) or {}
    raw_expected_shift_id = payload.get("expected_shift_id")
    expected_shift_id = None
    if raw_expected_shift_id not in (None, ""):
        try:
            expected_shift_id = int(raw_expected_shift_id)
        except (TypeError, ValueError):
            return jsonify({"error": "Turno esperado invalido."}), 400
    try:
        payload = archive_current_shift_and_open_next(expected_shift_id)
        invalidate_snapshot_cache("order-summary", "shift-history", "logistics", "menu")
        return jsonify(payload)
    except ValueError as error:
        return jsonify({"error": str(error)}), 409


@app.post("/api/reports/reset")
@login_required
@role_required("admin")
def reset_data():
    payload = request.get_json(silent=True) or {}
    raw_expected_shift_id = payload.get("expected_shift_id")
    expected_shift_id = None
    if raw_expected_shift_id not in (None, ""):
        try:
            expected_shift_id = int(raw_expected_shift_id)
        except (TypeError, ValueError):
            return jsonify({"error": "Turno esperado invalido."}), 400
    try:
        archived = archive_current_shift_and_open_next(expected_shift_id)
    except ValueError as error:
        return jsonify({"error": str(error)}), 409
    invalidate_snapshot_cache("order-summary", "shift-history", "logistics", "menu")
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
    invalidate_snapshot_cache("logistics", "menu")
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
    invalidate_snapshot_cache("logistics")
    return jsonify(fetch_logistics_snapshot())


@app.get("/test-error")
def test_error():
    if BAROS_ENV == "production":
        return "", 404
    raise RuntimeError("BarOS test error for Sentry validation.")


@app.errorhandler(500)
def handle_internal_server_error(error):
    if is_api_request():
        return jsonify({"error": "Ocorreu um erro interno. Tente novamente."}), 500
    return "Ocorreu um erro interno. Tente novamente.", 500


init_db()


if __name__ == "__main__":
    host = os.getenv("BAROS_HOST", "127.0.0.1")
    port = int(os.getenv("PORT", os.getenv("BAROS_PORT", "5000")))
    debug = IS_LOCAL_ENV and os.getenv("BAROS_DEBUG", "true").lower() == "true"
    app.run(debug=debug, host=host, port=port)

