import os
import re
import io
import json
import hashlib
from datetime import datetime, timezone
from uuid import uuid4

from flask import Flask, request
from google.cloud import storage
from pypdf import PdfReader

app = Flask(__name__)
storage_client = storage.Client()

LANDING_BUCKET = os.environ["LANDING_BUCKET"]
RAW_BUCKET = os.environ["RAW_BUCKET"]

# landing: lotes/YYYY-MM-DD/archivo.pdf   (o lotes/YYYY-MM-DD-NN/archivo.pdf)
LANDING_LOTS_PREFIX = os.environ.get("LANDING_LOTS_PREFIX", "lotes")

# RAW roots
RAW_PDF_ROOT = os.environ.get("RAW_PDF_ROOT", "clientes-pdf")
RAW_META_ROOT = os.environ.get("RAW_META_ROOT", "clientes-meta")


# ------------------------
# Utilidades
# ------------------------
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def _is_pdf(name, content_type):
    return name.lower().endswith(".pdf") or (content_type and "pdf" in content_type.lower())

def _sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()

def _normalize_digits(s):
    return re.sub(r"\D+", "", s or "")

def _sanitize_filename(s):
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_\-\.]+", "", s)
    s = re.sub(r"_+", "_", s)
    return s[:120] if s else "SIN_DATO"

def _extract_text_from_pdf(pdf_bytes):
    reader = PdfReader(io.BytesIO(pdf_bytes))
    texts = []
    for page in reader.pages:
        t = page.extract_text() or ""
        if t.strip():
            texts.append(t)
    return "\n".join(texts)

def _parse_lot_id_from_object(name):
    """
    Acepta:
      lotes/YYYY-MM-DD/archivo.pdf        -> lot_id = YYYY-MM-DD-01
      lotes/YYYY-MM-DD-NN/archivo.pdf     -> lot_id = YYYY-MM-DD-NN
    """
    prefix = f"{LANDING_LOTS_PREFIX}/"
    if not name.startswith(prefix):
        return None

    rest = name[len(prefix):]
    parts = rest.split("/", 1)
    if not parts or len(parts[0]) < 10:
        return None

    folder = parts[0].strip()

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", folder):
        return f"{folder}-01"

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}-\d{2}", folder):
        return folder

    return None


# ------------------------
# Extracción por secciones
# ------------------------
def _section(text, header, next_headers):
    m = re.search(re.escape(header), text, flags=re.IGNORECASE)
    if not m:
        return ""
    start = m.end()
    end = len(text)
    for nh in next_headers:
        m2 = re.search(re.escape(nh), text[start:], flags=re.IGNORECASE)
        if m2:
            end = min(end, start + m2.start())
    return text[start:end]

def _extract_cufe_and_invoice_number(doc_section):
    s = doc_section

    cufe = None
    m_cufe = re.search(r"CUFE\s*[:#]?\s*([A-Za-z0-9\-\._]{20,})", s, flags=re.IGNORECASE)
    if m_cufe:
        cufe = m_cufe.group(1).strip()

    cut = s
    m_fp = re.search(r"Forma\s+de\s+pago", s, flags=re.IGNORECASE)
    if m_fp:
        cut = s[:m_fp.start()]

    invoice_number = None
    patterns = [
        r"N[uú]mero\s+de\s+factura\s*[:#]?\s*([A-Za-z0-9\-]+)",
        r"Factura\s*(Electr[oó]nica)?\s*(No\.?|N[oº])?\s*[:#]?\s*([A-Za-z0-9\-]+)",
        r"No\.?\s*Factura\s*[:#]?\s*([A-Za-z0-9\-]+)",
        r"Factura\s*[:#]?\s*([A-Za-z0-9\-]+)",
    ]
    for p in patterns:
        m = re.search(p, cut, flags=re.IGNORECASE)
        if m:
            groups = [g for g in m.groups() if g]
            if groups:
                invoice_number = groups[-1].strip()
                break

    return cufe, invoice_number


def _extract_nit_emisor(emisor_section):
    """
    REFINADO (según tu regla):
    En 'Datos del Emisor / Vendedor':
      - El NIT está DESPUÉS de 'Nombre comercial' y ANTES de 'País'
    Dentro de esa ventana:
      1) buscar NIT explícito
      2) si no existe, capturar primera secuencia numérica larga (con puntos/guiones) y normalizar
    """
    s = emisor_section

    # tolerancia a variaciones: "Nombre comercial", "Nombre Comercial", etc.
    m_nc = re.search(r"Nombre\s+comercial", s, flags=re.IGNORECASE)
    m_pais = re.search(r"Pa[ií]s", s, flags=re.IGNORECASE)

    window = s
    if m_nc and m_pais and m_pais.start() > m_nc.end():
        window = s[m_nc.end():m_pais.start()]

    # 1) NIT explícito
    m_nit = re.search(r"(NIT|Nit|nit)\s*[:#]?\s*([0-9\.\-]{7,18})", window)
    if m_nit:
        nit = _normalize_digits(m_nit.group(2))
        return nit if len(nit) >= 8 else None

    # 2) Número largo "suelto" (con separadores)
    # Ejemplos: 900.123.456-7 / 900123456-7 / 9001234567
    m_digits = re.search(r"([0-9][0-9\.\-]{6,17}[0-9])", window)
    if m_digits:
        nit = _normalize_digits(m_digits.group(1))
        # a veces incluye DV; si te interesa guardarlo aparte, lo hacemos luego
        return nit if len(nit) >= 8 else None

    # 3) Fallback: buscar NIT en toda la sección (por si el PDF cambia ubicación)
    m_nit2 = re.search(r"(NIT|Nit|nit)\s*[:#]?\s*([0-9\.\-]{7,18})", s)
    if m_nit2:
        nit = _normalize_digits(m_nit2.group(2))
        return nit if len(nit) >= 8 else None

    return None


def _extract_nit_receptor(adq_section):
    # NIT receptor: entre "País" y "Departamento"
    s = adq_section

    m_pais = re.search(r"Pa[ií]s", s, flags=re.IGNORECASE)
    m_depto = re.search(r"Departamento", s, flags=re.IGNORECASE)

    window = s
    if m_pais and m_depto and m_depto.start() > m_pais.end():
        window = s[m_pais.end():m_depto.start()]

    m_doc = re.search(
        r"(N[uú]mero\s+de\s+documento|Documento|NIT|Nit|nit)\s*[:#]?\s*([0-9\.\-]{7,18})",
        window,
        flags=re.IGNORECASE
    )
    if m_doc:
        nit = _normalize_digits(m_doc.group(2))
        return nit if len(nit) >= 8 else None

    # fallback: número largo suelto
    m_digits = re.search(r"([0-9][0-9\.\-]{6,17}[0-9])", window)
    if m_digits:
        nit = _normalize_digits(m_digits.group(1))
        return nit if len(nit) >= 8 else None

    return None


def _extract_invoice_date_anywhere(text):
    m1 = re.search(r"Fecha[\s\S]{0,60}?(\d{4}-\d{2}-\d{2})", text, flags=re.IGNORECASE)
    if m1:
        return m1.group(1)
    m2 = re.search(r"Fecha[\s\S]{0,60}?(\d{2}/\d{2}/\d{4})", text, flags=re.IGNORECASE)
    if m2:
        d, m, y = m2.group(1).split("/")
        return f"{y}-{m}-{d}"
    return None


def _extract_by_sections(text):
    doc = _section(
        text,
        "Datos del Documento",
        ["Datos del Emisor / Vendedor", "Datos del Adquiriente / Comprador"]
    )
    emisor = _section(
        text,
        "Datos del Emisor / Vendedor",
        ["Datos del Adquiriente / Comprador", "Datos del Documento"]
    )
    adq = _section(
        text,
        "Datos del Adquiriente / Comprador",
        ["Datos del Emisor / Vendedor", "Datos del Documento"]
    )

    cufe, inv_num = _extract_cufe_and_invoice_number(doc) if doc else (None, None)
    nit_emisor = _extract_nit_emisor(emisor) if emisor else None
    nit_receptor = _extract_nit_receptor(adq) if adq else None

    return {
        "cufe": cufe,
        "invoice_number": inv_num,
        "nit_emisor": nit_emisor,
        "nit_receptor": nit_receptor,
    }


# ------------------------
# Handler
# ------------------------
@app.post("/")
def handle_event():
    body = request.get_json(silent=True) or {}
    data = body.get("data") or body

    bucket = data.get("bucket") or data.get("bucketId")
    name = data.get("name") or data.get("objectId")
    content_type = data.get("contentType")

    if not bucket or not name:
        return ("Missing bucket/name", 400)

    if bucket != LANDING_BUCKET:
        return ("Ignored bucket", 204)

    lot_id = _parse_lot_id_from_object(name)
    if not lot_id:
        return ("Ignored folder", 204)

    if not _is_pdf(name, content_type):
        return ("Ignored non-pdf", 204)

    # Descargar PDF
    landing_bkt = storage_client.bucket(LANDING_BUCKET)
    pdf_bytes = landing_bkt.blob(name).download_as_bytes()

    invoice_id = str(uuid4())
    pdf_hash = _sha256_bytes(pdf_bytes)

    # Extraer texto y campos
    try:
        text = _extract_text_from_pdf(pdf_bytes)
    except Exception:
        text = ""

    extracted = _extract_by_sections(text)
    nit_emisor = _normalize_digits(extracted.get("nit_emisor"))
    nit_receptor = _normalize_digits(extracted.get("nit_receptor"))
    invoice_number = extracted.get("invoice_number")
    cufe = extracted.get("cufe")
    invoice_date = _extract_invoice_date_anywhere(text)

    client_folder = nit_receptor if nit_receptor else "unassigned"

    num = _sanitize_filename(invoice_number or invoice_id)
    emisor_part = nit_emisor if nit_emisor else "SIN_NIT_EMISOR"
    cufe_suffix = (cufe[-6:] if cufe else invoice_id[-6:])

    base_name = f"{emisor_part}-{num}-{cufe_suffix}"

    raw_pdf_path = f"{RAW_PDF_ROOT}/{client_folder}/{lot_id}/{base_name}.pdf"
    raw_meta_path = f"{RAW_META_ROOT}/{client_folder}/{lot_id}/{base_name}.json"

    raw_bkt = storage_client.bucket(RAW_BUCKET)

    raw_bkt.blob(raw_pdf_path).upload_from_string(pdf_bytes, content_type="application/pdf")

    meta = {
        "invoice_id": invoice_id,
        "sha256": pdf_hash,

        "lot_id": lot_id,
        "lot_date": lot_id[:10],

        "invoice_date": invoice_date,
        "invoice_number": invoice_number,
        "cufe": cufe,

        "nit_emisor": nit_emisor or None,
        "nit_receptor": nit_receptor or None,

        "source_bucket": LANDING_BUCKET,
        "source_object": name,

        "raw_pdf_path": raw_pdf_path,
        "raw_meta_path": raw_meta_path,

        "ingested_at": _now_iso(),
        "content_type": content_type,
        "pipeline_step": "landing_lotes_to_raw_same_lot_folder_refined_emisor"
    }

    raw_bkt.blob(raw_meta_path).upload_from_string(
        json.dumps(meta, ensure_ascii=False, indent=2),
        content_type="application/json"
    )

    return ("OK", 200)
