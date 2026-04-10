from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path
from urllib.parse import quote, unquote, urlparse
from uuid import uuid4

from supabase import Client, create_client
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename


logger = logging.getLogger(__name__)

ALLOWED_PRODUCT_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
ALLOWED_PRODUCT_IMAGE_MIMETYPES = {"image/png", "image/jpeg", "image/webp"}
MAX_PRODUCT_IMAGE_SIZE_BYTES = 5 * 1024 * 1024
CONTENT_TYPE_BY_EXTENSION = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
}
DEFAULT_PRODUCT_IMAGE_URL = "data:image/svg+xml;charset=UTF-8," + quote(
    (
        "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 1200 900'>"
        "<defs>"
        "<linearGradient id='g' x1='0' x2='1' y1='0' y2='1'>"
        "<stop stop-color='#111827' offset='0'/>"
        "<stop stop-color='#1f2937' offset='1'/>"
        "</linearGradient>"
        "</defs>"
        "<rect width='1200' height='900' fill='url(#g)' rx='48'/>"
        "<circle cx='940' cy='180' r='120' fill='#f59e0b' fill-opacity='0.18'/>"
        "<circle cx='260' cy='720' r='160' fill='#fb7185' fill-opacity='0.14'/>"
        "<text x='50%' y='46%' fill='#f9fafb' font-family='Arial, sans-serif' font-size='92' "
        "font-weight='700' text-anchor='middle'>BarOS</text>"
        "<text x='50%' y='56%' fill='#d1d5db' font-family='Arial, sans-serif' font-size='36' "
        "text-anchor='middle'>Imagem do produto</text>"
        "</svg>"
    )
)


@lru_cache(maxsize=1)
def get_supabase_client() -> Client:
    supabase_url = (os.getenv("SUPABASE_URL") or "").strip()
    supabase_key = (os.getenv("SUPABASE_KEY") or "").strip()

    if not supabase_url or not supabase_key:
        raise OSError("Configuracao de armazenamento indisponivel no momento.")

    return create_client(supabase_url, supabase_key)


def _get_bucket_name() -> str:
    bucket_name = (os.getenv("SUPABASE_BUCKET") or "products").strip()
    if not bucket_name:
        raise OSError("Bucket de armazenamento nao configurado.")
    return bucket_name


def _validate_product_image(file: FileStorage) -> tuple[str, str, str]:
    raw_filename = (file.filename or "").strip()
    if not raw_filename:
        raise ValueError("Selecione uma imagem PNG, JPG, JPEG ou WEBP.")

    safe_name = secure_filename(raw_filename)
    extension = Path(safe_name).suffix.lower()

    if not safe_name or extension not in ALLOWED_PRODUCT_IMAGE_EXTENSIONS:
        raise ValueError("Formato invalido. Use PNG, JPG, JPEG ou WEBP.")

    if file.mimetype and file.mimetype not in ALLOWED_PRODUCT_IMAGE_MIMETYPES:
        raise ValueError("Arquivo invalido. Envie uma imagem PNG, JPG, JPEG ou WEBP.")

    content_type = file.mimetype or CONTENT_TYPE_BY_EXTENSION[extension]
    return safe_name, extension, content_type


def _read_product_image_bytes(file: FileStorage) -> bytes:
    if file.content_length and file.content_length > MAX_PRODUCT_IMAGE_SIZE_BYTES:
        raise ValueError("Imagem muito grande. Envie um arquivo de ate 5 MB.")

    try:
        file.stream.seek(0)
        payload = file.stream.read(MAX_PRODUCT_IMAGE_SIZE_BYTES + 1)
        file.stream.seek(0)
    except Exception as error:
        raise OSError("Nao foi possivel ler a imagem enviada.") from error

    if not payload:
        raise ValueError("Arquivo invalido. Envie uma imagem PNG, JPG, JPEG ou WEBP.")

    if len(payload) > MAX_PRODUCT_IMAGE_SIZE_BYTES:
        raise ValueError("Imagem muito grande. Envie um arquivo de ate 5 MB.")

    return payload


def _extract_storage_path(public_url: str, bucket_name: str) -> str | None:
    cleaned_url = (public_url or "").strip()
    if not cleaned_url or cleaned_url == DEFAULT_PRODUCT_IMAGE_URL or cleaned_url.startswith("data:image/"):
        return None

    parsed_url = urlparse(cleaned_url)
    if not parsed_url.scheme or not parsed_url.netloc:
        return None

    public_prefix = f"/storage/v1/object/public/{bucket_name}/"
    if not parsed_url.path.startswith(public_prefix):
        return None

    storage_path = unquote(parsed_url.path[len(public_prefix) :]).strip("/")
    return storage_path or None


def upload_product_image(file: FileStorage) -> str:
    if file is None:
        raise ValueError("Nenhum arquivo enviado.")

    _, extension, content_type = _validate_product_image(file)
    payload = _read_product_image_bytes(file)
    bucket_name = _get_bucket_name()
    storage_path = f"products/{uuid4().hex}{extension}"
    supabase = get_supabase_client()

    try:
        supabase.storage.from_(bucket_name).upload(
            storage_path,
            payload,
            {"content-type": content_type, "x-upsert": "false"},
        )
    except ValueError:
        raise
    except Exception as error:
        raise OSError("Nao foi possivel enviar a imagem agora.") from error

    return supabase.storage.from_(bucket_name).get_public_url(storage_path)


def delete_product_image(public_url: str) -> bool:
    try:
        bucket_name = _get_bucket_name()
        storage_path = _extract_storage_path(public_url, bucket_name)
        if not storage_path:
            return False

        supabase = get_supabase_client()
        supabase.storage.from_(bucket_name).remove([storage_path])
        return True
    except Exception as error:
        logger.warning("Falha ao remover imagem antiga do produto: %s", error)
        return False
