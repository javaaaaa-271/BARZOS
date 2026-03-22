from __future__ import annotations

import os
import secrets
import sqlite3
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

from flask import Flask, g, jsonify, redirect, render_template, request, session, url_for


BASE_DIR = Path(__file__).resolve().parent
INSTANCE_DIR = BASE_DIR / "instance"
INSTANCE_DIR.mkdir(exist_ok=True)

DATABASE_PATH = Path(os.getenv("BAROS_DB_PATH", INSTANCE_DIR / "baros.db"))
BAR_USERNAME = os.getenv("BAROS_USERNAME", "admin")
BAR_PASSWORD = os.getenv("BAROS_PASSWORD", "bar123")
DEFAULT_SECRET = "troque-esta-chave-em-producao"

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


app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("BAROS_SECRET_KEY", DEFAULT_SECRET)


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


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        connection = sqlite3.connect(DATABASE_PATH)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        g.db = connection
    return g.db


@app.teardown_appcontext
def close_db(error=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = sqlite3.connect(DATABASE_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
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

        CREATE TABLE IF NOT EXISTS itens_pedido (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pedido_id INTEGER NOT NULL,
            bebida_id INTEGER NOT NULL,
            quantidade INTEGER NOT NULL,
            subtotal REAL NOT NULL,
            FOREIGN KEY(pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE,
            FOREIGN KEY(bebida_id) REFERENCES bebidas(id)
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
        """
    )

    for beverage in BEVERAGE_SEED:
        exists = db.execute("SELECT 1 FROM bebidas WHERE nome = ?", (beverage["nome"],)).fetchone()
        if not exists:
            db.execute(
                """
                INSERT INTO bebidas (nome, preco_venda, custo_estimado)
                VALUES (:nome, :preco_venda, :custo_estimado)
                """,
                beverage,
            )

    for item in LOGISTICS_SEED:
        exists = db.execute("SELECT 1 FROM inventory_items WHERE name = ?", (item["name"],)).fetchone()
        if not exists:
            db.execute(
                """
                INSERT INTO inventory_items (name, category, unit, stock_level, par_level, status, updated_at)
                VALUES (:name, :category, :unit, :stock_level, :par_level, :status, :updated_at)
                """,
                {**item, "updated_at": utc_now_iso()},
            )

    for note in SHIFT_NOTES_SEED:
        exists = db.execute("SELECT 1 FROM shift_notes WHERE title = ? AND body = ?", (note["title"], note["body"])).fetchone()
        if not exists:
            db.execute(
                """
                INSERT INTO shift_notes (title, body, priority, status, created_at)
                VALUES (:title, :body, :priority, :status, :created_at)
                """,
                {**note, "created_at": utc_now_iso()},
            )

    db.commit()
    db.close()


def login_required(view):
    @wraps(view)
    def wrapped_view(*args, **kwargs):
        if not session.get("bar_authenticated"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped_view


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


def beverage_availability(beverage_name: str, inventory_by_name: dict[str, sqlite3.Row]) -> tuple[bool, str | None]:
    recipe = BEVERAGE_RECIPES.get(beverage_name, {})
    if not recipe:
        return True, None

    for ingredient_name, amount in recipe.items():
        inventory_item = inventory_by_name.get(ingredient_name)
        if not inventory_item:
            return False, f"Indisponivel: {ingredient_name} nao cadastrado."
        if inventory_item["stock_level"] < amount:
            return (
                False,
                f"Indisponivel por estoque de {ingredient_name}.",
            )

    return True, None


def apply_stock_deductions(items: list[dict]) -> None:
    db = get_db()
    recipes_to_apply: dict[str, float] = {}

    for item in items:
        recipe = BEVERAGE_RECIPES.get(item["name"], {})
        for ingredient_name, base_amount in recipe.items():
            recipes_to_apply[ingredient_name] = recipes_to_apply.get(ingredient_name, 0) + (
                base_amount * item["quantity"]
            )

    if not recipes_to_apply:
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


def check_stock_availability(items: list[dict]) -> list[dict]:
    db = get_db()
    required: dict[str, float] = {}

    for item in items:
        recipe = BEVERAGE_RECIPES.get(item["name"], {})
        for ingredient_name, amount in recipe.items():
            required[ingredient_name] = required.get(ingredient_name, 0) + (amount * item["quantity"])

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
    rows = get_db().execute(
        "SELECT id, nome, preco_venda, custo_estimado FROM bebidas ORDER BY nome ASC"
    ).fetchall()
    menu = []
    for row in rows:
        meta = BEVERAGE_META.get(row["nome"], {})
        is_available, availability_note = beverage_availability(row["nome"], inventory_by_name)
        menu.append(
            {
                "id": row["id"],
                "name": row["nome"],
                "price": row["preco_venda"],
                "cost": row["custo_estimado"],
                "category": meta.get("category", "Bebida"),
                "description": meta.get("description", "Bebida cadastrada no sistema."),
                "prep_time": meta.get("prep_time", "3 min"),
                "is_available": is_available,
                "availability_note": availability_note,
            }
        )
    return menu


def serialize_order(row: sqlite3.Row) -> dict:
    items = get_db().execute(
        """
        SELECT ip.id, ip.quantidade, ip.subtotal, b.id AS bebida_id, b.nome, b.preco_venda
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
        "status": row["status"],
        "created_at": display_datetime(row["horario_pedido"]),
        "completed_at": display_datetime(row["completed_at"]) if "completed_at" in row.keys() and row["completed_at"] else None,
        "total": row["valor_total"],
        "customer_name": "Cliente",
        "table_label": "Retirada",
        "source": "salon",
        "items": [
            {
                "id": item["bebida_id"],
                "name": item["nome"],
                "quantity": item["quantidade"],
                "price": item["preco_venda"],
                "subtotal": item["subtotal"],
            }
            for item in items
        ],
    }


def fetch_orders(status: str | None = None, limit: int | None = None) -> list[dict]:
    query = "SELECT id, codigo_retirada, horario_pedido, status, valor_total, NULL AS completed_at FROM pedidos"
    params: list = []
    if status:
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY horario_pedido DESC"
    if limit:
        query += " LIMIT ?"
        params.append(limit)
    rows = get_db().execute(query, params).fetchall()
    return [serialize_order(row) for row in rows]


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


def build_sales_report() -> dict:
    db = get_db()
    totals = db.execute(
        """
        SELECT
            COUNT(*) AS total_pedidos,
            COALESCE(SUM(valor_total), 0) AS total_vendido
        FROM pedidos
        """
    ).fetchone()

    by_beverage = db.execute(
        """
        SELECT b.nome, SUM(ip.quantidade) AS quantidade
        FROM itens_pedido ip
        JOIN bebidas b ON b.id = ip.bebida_id
        GROUP BY b.id, b.nome
        ORDER BY quantidade DESC, b.nome ASC
        """
    ).fetchall()

    peak = db.execute(
        """
        SELECT strftime('%H', horario_pedido) AS hora, COUNT(*) AS total
        FROM pedidos
        GROUP BY hora
        ORDER BY total DESC, hora ASC
        LIMIT 1
        """
    ).fetchone()

    top_item = by_beverage[0] if by_beverage else None

    return {
        "total_vendido": round(totals["total_vendido"] or 0, 2),
        "total_pedidos": totals["total_pedidos"] or 0,
        "quantidade_por_bebida": [
            {"name": row["nome"], "quantity": row["quantidade"]}
            for row in by_beverage
        ],
        "bebida_mais_pedida": {
            "name": top_item["nome"],
            "quantity": top_item["quantidade"],
        }
        if top_item
        else None,
        "pico_atendimento": {
            "hour": f'{peak["hora"]}h',
            "orders": peak["total"],
        }
        if peak and peak["hora"] is not None
        else None,
    }


def build_order_summary() -> dict:
    report = build_sales_report()
    pending = get_db().execute(
        "SELECT COUNT(*) AS total FROM pedidos WHERE status = 'pending'"
    ).fetchone()["total"]
    completed = get_db().execute(
        "SELECT COUNT(*) AS total FROM pedidos WHERE status = 'completed'"
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
        "top_tables": [],
    }


def build_closeout_report() -> dict:
    db = get_db()
    sales = build_sales_report()
    totals = db.execute(
        """
        SELECT
            COALESCE(SUM(ip.quantidade), 0) AS total_itens,
            COALESCE(SUM(b.custo_estimado * ip.quantidade), 0) AS custo_total
        FROM itens_pedido ip
        JOIN bebidas b ON b.id = ip.bebida_id
        """
    ).fetchone()

    return {
        "total_vendido": sales["total_vendido"],
        "total_pedidos": sales["total_pedidos"],
        "total_itens_vendidos": totals["total_itens"] or 0,
        "bebida_mais_pedida": sales["bebida_mais_pedida"],
        "pico_atendimento": sales["pico_atendimento"],
        "custo_total": round(totals["custo_total"] or 0, 2),
        "lucro_estimado": round(sales["total_vendido"] - (totals["custo_total"] or 0), 2),
        "quantidade_por_bebida": sales["quantidade_por_bebida"],
    }


@app.get("/")
def index():
    return render_template("index.html", menu=fetch_menu(), current_time=display_datetime(utc_now_iso()))


@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username == BAR_USERNAME and password == BAR_PASSWORD:
            session["bar_authenticated"] = True
            return redirect(url_for("dashboard"))
        error = "Usuario ou senha invalidos."
    return render_template("login.html", error=error)


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
            "generated_at": display_datetime(utc_now_iso()),
        }
    )


@app.post("/api/orders")
def create_order():
    payload = request.get_json(silent=True) or {}
    selected_items = payload.get("items") or []
    if not selected_items:
        return jsonify({"error": "Nenhum item enviado."}), 400

    db = get_db()
    bebidas_por_id = {
        row["id"]: row
        for row in db.execute("SELECT id, nome, preco_venda, custo_estimado FROM bebidas").fetchall()
    }

    itens = []
    valor_total = 0.0
    for entry in selected_items:
        bebida_id = int(entry.get("id", 0))
        quantidade = int(entry.get("quantity", 0))
        bebida = bebidas_por_id.get(bebida_id)
        if not bebida or quantidade <= 0:
            continue
        subtotal = round(bebida["preco_venda"] * quantidade, 2)
        valor_total += subtotal
        itens.append(
            {
                "bebida_id": bebida_id,
                "name": bebida["nome"],
                "quantity": quantidade,
                "price": bebida["preco_venda"],
                "subtotal": subtotal,
            }
        )

    if not itens:
        return jsonify({"error": "Itens invalidos."}), 400

    shortages = check_stock_availability(itens)
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
    cursor = db.execute(
        """
        INSERT INTO pedidos (codigo_retirada, horario_pedido, status, valor_total)
        VALUES (?, ?, 'pending', ?)
        """,
        (codigo, horario, round(valor_total, 2)),
    )
    pedido_id = cursor.lastrowid

    db.executemany(
        """
        INSERT INTO itens_pedido (pedido_id, bebida_id, quantidade, subtotal)
        VALUES (?, ?, ?, ?)
        """,
        [
            (pedido_id, item["bebida_id"], item["quantity"], item["subtotal"])
            for item in itens
        ],
    )
    apply_stock_deductions(itens)
    db.commit()

    row = db.execute(
        "SELECT id, codigo_retirada, horario_pedido, status, valor_total, NULL AS completed_at FROM pedidos WHERE id = ?",
        (pedido_id,),
    ).fetchone()
    return jsonify({"order": serialize_order(row), "summary": build_order_summary()}), 201


@app.post("/api/orders/<code>/complete")
@login_required
def complete_order(code: str):
    db = get_db()
    row = db.execute(
        "SELECT id FROM pedidos WHERE codigo_retirada = ?",
        (code,),
    ).fetchone()
    if not row:
        return jsonify({"error": "Pedido nao encontrado."}), 404

    db.execute(
        "UPDATE pedidos SET status = 'completed' WHERE codigo_retirada = ?",
        (code,),
    )
    db.commit()

    updated = db.execute(
        "SELECT id, codigo_retirada, horario_pedido, status, valor_total, NULL AS completed_at FROM pedidos WHERE codigo_retirada = ?",
        (code,),
    ).fetchone()
    return jsonify({"order": serialize_order(updated), "summary": build_order_summary()})


@app.get("/api/reports/closeout")
@login_required
def closeout_report():
    return jsonify(build_closeout_report())


@app.post("/api/reports/reset")
@login_required
def reset_data():
    db = get_db()
    db.execute("DELETE FROM itens_pedido")
    db.execute("DELETE FROM pedidos")
    db.commit()
    return jsonify({"ok": True, "summary": build_order_summary()})


@app.post("/api/logistics/inventory/<int:item_id>")
@login_required
def update_inventory(item_id: int):
    payload = request.get_json(silent=True) or {}
    status = payload.get("status")
    if status not in {"ok", "attention", "critical"}:
        return jsonify({"error": "Status invalido."}), 400
    db = get_db()
    row = db.execute("SELECT id, stock_level FROM inventory_items WHERE id = ?", (item_id,)).fetchone()
    if not row:
        return jsonify({"error": "Item nao encontrado."}), 404
    db.execute(
        """
        UPDATE inventory_items
        SET status = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, utc_now_iso(), item_id),
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
