from flask import Flask, Response, render_template_string, request, jsonify
import requests
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone
from dateutil import parser
import csv
from pathlib import Path
import math
import json
import random
import threading
import time
import os
from typing import Optional, Any, Dict
from flask import session, redirect, url_for, g
from functools import wraps
import requests
from oauthlib.oauth2 import WebApplicationClient
import json

app = Flask(__name__)

# Credenciales y endpoints VerifyFaces
API_URL = "https://dashboard-api.verifyfaces.com/companies/54/search/realtime"
AUTH_URL = "https://dashboard-api.verifyfaces.com/auth/login"
AUTH_EMAIL = "eangulo@blocksecurity.com.ec"
AUTH_PASSWORD = "Scarling//07052022.?"
TOKEN = None

# Configuración de Autenticación
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET")

if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
    raise ValueError("Las variables de entorno GOOGLE_CLIENT_ID y GOOGLE_CLIENT_SECRET deben estar configuradas.")


GOOGLE_DISCOVERY_URL = "https://accounts.google.com/.well-known/openid-configuration"
app.secret_key = os.environ.get("SECRET_KEY") or "your-super-secret-key-123"

# Lista de usuarios autorizados
AUTHORIZED_USERS = [
    "killthmxall@gmail.com",
    "jronquillolugo@gmail.com",
    "paul.hernandez@arrayanes.com",
    "coord.seguridad@arrayanes.com",
    "johana.jaramillo@arrayanes.com",
    "atencionalsocio@arrayanes.com"
]

# Configuración del cliente OAuth 2
client = WebApplicationClient(GOOGLE_CLIENT_ID)

# IDs de galería
EMP_GALLERY_ID = 531
SOC_GALLERY_ID = 546
PROV_GALLERY_ID = 548
GALLERY_IDS = [EMP_GALLERY_ID, SOC_GALLERY_ID, PROV_GALLERY_ID]

# CSVs por galería
CSV_FILES = {
    EMP_GALLERY_ID: Path("detecciones_empleados.csv"),
    SOC_GALLERY_ID: Path("detecciones_socios.csv"),
    PROV_GALLERY_ID: Path("detecciones_proveedores.csv"),
}

# Caches por galería
GALLERY_CACHE = {gid: {} for gid in GALLERY_IDS}
PERSON_IMG_MAP_BY = {gid: {} for gid in GALLERY_IDS}

# Hash por galería para evitar escrituras innecesarias
_last_fetch_hash_by = {gid: None for gid in GALLERY_IDS}

# Paginación / límites
PER_PAGE = 100
TOTAL_RECORDS_NEEDED = 1000

# Control de ingesta y stream
FETCH_PERIOD_SECONDS = 10            # cada cuánto baja nuevos eventos
STREAM_POLL_MTIME_SECONDS = 2        # cada cuánto el SSE revisa cambios
_ingest_lock = threading.Lock()
_stop_event = threading.Event()

_worker_started = False
_worker_lock = threading.Lock()


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_email" not in session or session["user_email"] not in AUTHORIZED_USERS:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated_function

def get_google_provider_cfg():
    return requests.get(GOOGLE_DISCOVERY_URL).json()

def _ensure_background_started():
    global _worker_started
    if _worker_started:
        return
    with _worker_lock:
        if _worker_started:
            return
        t = threading.Thread(target=background_worker, daemon=True)
        t.start()
        _worker_started = True


# =========================
# Utilidades
# =========================

def _extract_image_url(image_data: Dict[str, Any]) -> Optional[str]:
    if not isinstance(image_data, dict):
        return None
    for k in ("thumbnailUrl", "url", "publicUrl"):
        v = image_data.get(k)
        if isinstance(v, str) and v:
            return v
    img = image_data.get("image")
    if isinstance(img, dict):
        v = img.get("url")
        if isinstance(v, str) and v:
            return v
    f = image_data.get("file")
    if isinstance(f, dict):
        v = f.get("url")
        if isinstance(v, str) and v:
            return v
    return None


def obtener_nuevo_token():
    global TOKEN
    try:
        auth_data = {"email": AUTH_EMAIL, "password": AUTH_PASSWORD}
        response = requests.post(AUTH_URL, json=auth_data, timeout=10)
        response.raise_for_status()
        new_token = response.json().get("token")
        if new_token:
            TOKEN = new_token
            print("Token actualizado correctamente.")
            return True
        return False
    except requests.exceptions.RequestException as e:
        print(f"Error al obtener el token: {e}")
        return False


def cargar_cache_galeria(gallery_id: int) -> bool:
    """
    Carga en memoria el cache de la galería indicada (531 empleados, 546 socios, 548 proveedores).
    Llena GALLERY_CACHE[gallery_id] y PERSON_IMG_MAP_BY[gallery_id].
    """
    global TOKEN, GALLERY_CACHE, PERSON_IMG_MAP_BY

    BASE_URL = f"https://dashboard-api.verifyfaces.com/companies/54/galleries/{gallery_id}"
    PER_PAGE_LOCAL = 100

    MAX_PAGE_RETRIES = 3
    REQUEST_TIMEOUT  = 30
    BACKOFF_BASE     = 1.6
    BACKOFF_JITTER   = (0.15, 0.65)

    if not TOKEN and not obtener_nuevo_token():
        print("cargar_cache_galeria: no se pudo obtener token inicial")
        return False

    headers = {"Authorization": f"Bearer {TOKEN}"}

    def _sleep_backoff(attempt_idx: int):
        base = (BACKOFF_BASE ** attempt_idx)
        jitter = random.uniform(*BACKOFF_JITTER)
        time.sleep(base + jitter)

    temp_gallery_cache = {}
    temp_person_img_map = {}

    page = 1
    total_cargados = 0

    while True:
        last_exc = None
        for attempt in range(MAX_PAGE_RETRIES):
            try:
                params = {"perPage": PER_PAGE_LOCAL, "page": page}
                resp = requests.get(BASE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

                if resp.status_code == 401:
                    if not obtener_nuevo_token():
                        last_exc = RuntimeError("401 y no se pudo renovar token")
                        break
                    headers["Authorization"] = f"Bearer {TOKEN}"
                    resp = requests.get(BASE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

                resp.raise_for_status()
                data = resp.json()
                images = data.get("images", []) or []

                if not images:
                    GALLERY_CACHE[gallery_id] = temp_gallery_cache
                    PERSON_IMG_MAP_BY[gallery_id] = temp_person_img_map
                    print(f"Galería {gallery_id}: fin de páginas (page={page-1}, total={total_cargados})")
                    return True

                for image_data in images:
                    original_filename = image_data.get("originalFilename")
                    metadata = image_data.get("metadata")
                    img_url = _extract_image_url(image_data)
                    if original_filename and isinstance(metadata, dict):
                        temp_gallery_cache[original_filename] = {
                            "metadata": metadata,
                            "image_url": img_url
                        }
                        total_cargados += 1
                        person_name = metadata.get("name")
                        if person_name and img_url and person_name not in temp_person_img_map:
                            temp_person_img_map[person_name] = img_url

                page += 1
                if len(images) < PER_PAGE_LOCAL:
                    GALLERY_CACHE[gallery_id] = temp_gallery_cache
                    PERSON_IMG_MAP_BY[gallery_id] = temp_person_img_map
                    print(f"Galería {gallery_id} cargada: {total_cargados} imágenes en caché.")
                    return True

                break

            except requests.exceptions.Timeout as e:
                last_exc = e
                if attempt < MAX_PAGE_RETRIES - 1:
                    _sleep_backoff(attempt)
                    continue
                print(f"cargar_cache_galeria({gallery_id}): timeout persistente en page={page}: {e}")

            except requests.exceptions.RequestException as e:
                last_exc = e
                status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
                if status and 400 <= status < 500 and status != 401:
                    print(f"cargar_cache_galeria({gallery_id}): HTTP {status} en page={page}, no reintenta: {e}")
                    break
                if attempt < MAX_PAGE_RETRIES - 1:
                    _sleep_backoff(attempt)
                    continue
                print(f"cargar_cache_galeria({gallery_id}): error de red/API en page={page}: {e}")

            except ValueError as e:
                last_exc = e
                print(f"cargar_cache_galeria({gallery_id}): JSON inválido en page={page}: {e}")
                break

            except Exception as e:
                last_exc = e
                print(f"cargar_cache_galeria({gallery_id}): error inesperado en page={page}: {e}")
                break

        if last_exc is not None:
            print(f"cargar_cache_galeria({gallery_id}): abortando carga por error en page={page}: {last_exc}")
            return False


def leer_csv(ruta: Path):
    registros = []
    agg = defaultdict(int)
    agg_hora_latest = {}
    fechas = set()
    personas_total = Counter()
    if not ruta.exists():
        return [], {}, {}, [], [], {}

    def _is_time_b_greater(a: str, b: str) -> bool:
        return (b or "") > (a or "")

    with ruta.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fecha = str(row.get("fecha", "")).strip()
            hora = str(row.get("hora", "")).strip()
            person_id = str(row.get("nombre_persona", "")).strip()
            try:
                conteo = int(row.get("conteo", "1"))
            except ValueError:
                conteo = 1
            if not fecha or not person_id:
                continue

            try:
                fecha_dt = datetime.fromisoformat(str(fecha).split(" ")[0])
                fecha = fecha_dt.date().isoformat()
            except Exception:
                pass

            camara = str(row.get("camara", "")).strip()
            search_id = str(row.get("search_id", "")).strip()
            camera_id = str(row.get("camera_id", "")).strip()
            ts_utc    = str(row.get("ts_utc", "")).strip()

            registros.append({
                "fecha": fecha,
                "hora": hora,
                "person_id": person_id,
                "conteo": conteo,
                "camara": camara,
                "search_id": search_id,
                "camera_id": camera_id,
                "ts_utc": ts_utc,
            })

            agg[(fecha, person_id)] += conteo
            fechas.add(fecha)
            personas_total[person_id] += conteo
            key = (fecha, person_id)
            if key not in agg_hora_latest or _is_time_b_greater(agg_hora_latest.get(key, ""), hora):
                agg_hora_latest[key] = hora

    fechas_ordenadas = sorted(fechas)
    personas_ordenadas = [pid for pid, _ in sorted(personas_total.items(), key=lambda x: (-x[1], x[0]))]
    return registros, agg, agg_hora_latest, fechas_ordenadas, personas_ordenadas, personas_total


def html_escape(s: str) -> str:
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


# =========================
# Construcción de UI
# =========================

def construir_filas_html(agg, agg_hora_latest, fechas, personas_total, person_img_map):
    filas = []
    def initials(name: str) -> str:
        parts = [p for p in (name or "").split() if p]
        if not parts:
            return "?"
        if len(parts) == 1:
            return parts[0][:2].upper()
        return (parts[0][:1] + parts[-1][:1]).upper()

    for (fecha, person_id), suma in sorted(
        agg.items(),
        key=lambda item: (item[0][0], agg_hora_latest.get((item[0][0], item[0][1]), "00:00:00"), item[1]),
        reverse=True
    ):
        hora = agg_hora_latest.get((fecha, person_id), "")
        key = f"{fecha}||{person_id}"
        img_url = person_img_map.get(person_id)
        if img_url:
            avatar_html = (
                f"<span class='avatar'>"
                f"<img src='{html_escape(img_url)}' "
                f"alt='{html_escape(person_id)}' "
                f"onerror='this.replaceWith(Object.assign(document.createElement(\"span\"),{{className:\"avatar\", innerText:\"{html_escape(initials(person_id))}\"}}));'>"
                f"</span>"
            )
        else:
            avatar_html = f"<span class='avatar'>{html_escape(initials(person_id))}</span>"

        filas.append(
            f"<tr data-fecha='{html_escape(fecha)}' data-hora='{html_escape(hora)}' data-key='{html_escape(key)}'>"
            f"<td class='td'>{html_escape(fecha)}</td>"
            f"<td class='td mono'>{html_escape(hora)}</td>"
            f"<td class='td name-cell'>{avatar_html}<button class='name-btn' type='button'>{html_escape(person_id)}</button></td>"
            f"<td class='td num'>{suma}</td>"
            f"</tr>"
        )
    return "".join(filas)


def construir_times_by(registros):
    times_by = defaultdict(list)
    for r in registros:
        f = r["fecha"]; p = r["person_id"]; h = r["hora"]
        c = r.get("camara", "")
        sid = r.get("search_id", "")
        cid = r.get("camera_id", "")
        ts  = r.get("ts_utc", "")
        if f and p and h:
            times_by[(f, p)].append({"h": h, "c": c, "sid": sid, "cid": cid, "ts": ts})

    out = {}
    for (f, p), items in times_by.items():
        seen = set()
        uniq = []
        for it in items:
            key = (it["h"], it.get("c", ""), it.get("sid",""), it.get("cid",""), it.get("ts",""))
            if key not in seen:
                seen.add(key)
                uniq.append(it)
        uniq.sort(key=lambda x: x["h"], reverse=True)
        out[f"{f}||{p}"] = uniq
    return out


def construir_estado_dashboard(registros, agg, agg_hora_latest, fechas, personas_ordenadas, personas_total, person_img_map):
    # KPIs
    total_registros = sum(r["conteo"] for r in registros) if registros else 0
    total_personas = len(set(r["person_id"] for r in registros)) if registros else 0
    hoy_iso = datetime.now().date().isoformat()
    ingresos_hoy = sum((r.get("conteo", 0) or 0) for r in registros if r.get("fecha") == hoy_iso)
    personas_unicas_hoy = len({r.get("person_id") for r in registros if r.get("fecha") == hoy_iso})

    # Cobertura galería (cruce por nombre exacto)
    gallery_names = set()
    try:
        for meta in (person_img_map or {}).keys():
            gallery_names.add(meta)
    except Exception:
        pass
    recognized_names = set(personas_ordenadas or [])
    recognized_in_gallery = len(recognized_names.intersection(gallery_names))
    total_gallery_persons = len(gallery_names)
    percent_gallery = (recognized_in_gallery / total_gallery_persons * 100.0) if total_gallery_persons else 0.0

    # Top 10
    top_personas = sorted(personas_total.items(), key=lambda x: -x[1])[:10]
    labels_personas = [pid for pid, _ in top_personas]
    data_personas = [cnt for _, cnt in top_personas]
    top_items_html = "".join(
        f"<li><span class='mono'>{html_escape(pid)}</span> · <strong>{cnt}</strong></li>"
        for pid, cnt in top_personas
    )

    # Series Top10 por fecha
    pivot = {pid: {f: 0 for f in fechas} for pid in personas_ordenadas}
    for (fecha, person_id), suma in agg.items():
        if person_id in pivot and fecha in pivot[person_id]:
            pivot[person_id][fecha] += suma

    datasets_top10 = []
    for person_id in [pid for pid, _ in top_personas]:
        person_data = [pivot.get(person_id, {}).get(fecha, 0) for fecha in fechas]
        datasets_top10.append({
            'label': person_id,
            'data': person_data,
            'backgroundColor': f'rgba({random.randint(0,255)},{random.randint(0,255)},{random.randint(0,255)},.7)',
            'stack': 'Stack 1'
        })

    # Esta semana (12 días atrás a hoy, filtrando existentes)
    hoy = datetime.now().date()
    ultimos = [(hoy - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7, -1, -1)]
    fechas_semana = [f for f in ultimos if f in fechas]

    datasets_all = []
    for person_id in personas_ordenadas:
        person_data = [pivot.get(person_id, {}).get(fecha, 0) for fecha in fechas_semana]
        if any(v > 0 for v in person_data):
            datasets_all.append({
                'label': html_escape(person_id),
                'data': person_data,
                'backgroundColor': f'rgba({random.randint(0,255)},{random.randint(0,255)},{random.randint(0,255)},.7)',
                'stack': 'Stack 1'
            })

    # Último timestamp
    last_ts = None
    for r in registros:
        f = r.get("fecha") or ""
        h = r.get("hora") or ""
        if f and h:
            try:
                dt = datetime.fromisoformat(f)
                hh, mm, ss = h.split(":")
                dt = dt.replace(hour=int(hh), minute=int(mm), second=int(ss))
                if (last_ts is None) or (dt > last_ts):
                    last_ts = dt
            except Exception:
                pass
    last_ts_iso = last_ts.isoformat() if last_ts else ""

    estado = {
        "kpis": {
            "percent_gallery": round(percent_gallery, 1),
            "recognized_in_gallery": recognized_in_gallery,
            "total_gallery_persons": total_gallery_persons,
            "ingresos_hoy": ingresos_hoy,
            "total_personas": total_personas,
            "personas_unicas_hoy": personas_unicas_hoy,
            "total_registros": total_registros,
            "hoy_iso": hoy_iso
        },
        "top_items_html": top_items_html,
        "personasChart": {
            "labels": labels_personas,
            "data": data_personas
        },
        "top10Chart": {
            "labels": fechas,
            "datasets": datasets_top10
        },
        "thisWeekChart": {
            "labels": fechas_semana,
            "datasets": datasets_all
        },
        "tbody_html": construir_filas_html(agg, agg_hora_latest, fechas, personas_total, person_img_map),
        "times_by": construir_times_by(registros),
        "last_ts_iso": last_ts_iso
    }
    return estado


def construir_html_dashboard_bootstrap(estado: dict, gallery_id: int, titulo: str = "Dashboard"):
    k = estado.get("kpis", {})
    percent_gallery = k.get("percent_gallery", 0.0)
    recognized_in_gallery = k.get("recognized_in_gallery", 0)
    total_gallery_persons = k.get("total_gallery_persons", 0)
    ingresos_hoy = k.get("ingresos_hoy", 0)
    total_personas = k.get("total_personas", 0)
    total_registros = k.get("total_registros", 0)
    hoy_iso = k.get("hoy_iso", datetime.now().date().isoformat())
    personas_unicas_hoy = k.get("personas_unicas_hoy", 0)

    top_items = estado.get("top_items_html", "")
    personasChart_labels = json.dumps(estado.get("personasChart", {}).get("labels", []))
    personasChart_data = json.dumps(estado.get("personasChart", {}).get("data", []))

    top10_labels = json.dumps(estado.get("top10Chart", {}).get("labels", []))
    top10_datasets = json.dumps(estado.get("top10Chart", {}).get("datasets", []))

    week_labels = json.dumps(estado.get("thisWeekChart", {}).get("labels", []))
    week_datasets = json.dumps(estado.get("thisWeekChart", {}).get("datasets", []))

    tbody_html = estado.get("tbody_html", "")
    times_by_json = json.dumps(estado.get("times_by", {}))
    js_last_ts = json.dumps(estado.get("last_ts_iso", ""))

    # rutas y estado activo
    ruta_empleados = "/"
    ruta_socios = "/socios"
    ruta_proveedores = "/proveedores"
    activo_empleados = "active" if gallery_id == EMP_GALLERY_ID else ""
    activo_socios = "active" if gallery_id == SOC_GALLERY_ID else ""
    activo_proveedores = "active" if gallery_id == PROV_GALLERY_ID else ""

    # etiqueta del botón según la galería actual
    if gallery_id == EMP_GALLERY_ID:
        etiqueta_actual = "Empleados"
    elif gallery_id == SOC_GALLERY_ID:
        etiqueta_actual = "Socios"
    elif gallery_id == PROV_GALLERY_ID:
        etiqueta_actual = "Proveedores"
    else:
        etiqueta_actual = "Galerías"

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>{html_escape(titulo)}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {{
    --bg: #0b1020;
    --card: #12172a;
    --muted: #8ea0c0;
    --text: #e8eefc;
    --accent: #6aa7ff;
    --grid: #1e2742;
    --border: #223052;
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; padding: 0; background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui; }}
  .wrap {{ max-width: 1200px; margin: 32px auto; padding: 0 16px; }}
  h1 {{ margin: 0; font-size: 28px; font-weight: 700; letter-spacing: .2px; }}
  .muted {{ color: var(--muted); }}
  .grid {{ display: grid; gap: 16px; }}
  .cards {{ grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-bottom: 20px; }}
  .card {{ background: linear-gradient(180deg, var(--card), #0f1528 80%); border: 1px solid var(--border); border-radius: 16px; padding: 16px 18px; }}
  .card h3 {{ margin: 0; font-size: 13px; font-weight: 600; color: var(--muted); }}
  .card .val {{ margin-top: 8px; font-size: 28px; font-weight: 800; color: var(--text); }}
  .section {{ background: rgba(18,23,42,.6); border: 1px solid var(--border); border-radius: 16px; padding: 16px; margin-bottom: 22px; }}
  .section h2 {{ margin: 0 0 12px; font-size: 18px; }}
  .toolbar {{ display: flex; flex-wrap: wrap; gap: 8px; align-items: center; margin-bottom: 12px; }}
  .input {{ background: #0b1226; border: 1px solid var(--border); color: var(--text); padding: 10px 12px; border-radius: 12px; outline: none; }}
  .input.w100 {{ width: 100%; }}
  .table-wrap {{ overflow: auto; border: 1px solid var(--border); border-radius: 12px; }}
  table {{ width: 100%; border-collapse: collapse; min-width: 640px; }}
  .th, .td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--grid); white-space: nowrap; }}
  thead .th {{ position: sticky; top: 0; background: #0d1330; z-index: 1; }}
  .num {{ text-align: right; }}
  .mono {{ font-family: ui-monospace, monospace; font-size: 12px; color: #c8d5f5; }}
  .strong {{ font-weight: 700; }}
  .pill {{ display: inline-block; padding: 4px 8px; border: 1px solid var(--border); border-radius: 999px; color: var(--muted); font-size: 12px; }}
  .list {{ display: grid; grid-template-columns: repeat(auto-fit,minmax(220px,1fr)); gap: 8px; margin: 0; padding: 0; list-style: none; }}
  .list li {{ background: #0b1226; border: 1px solid var(--border); border-radius: 12px; padding: 10px 12px; }}
  .hint {{ font-size: 12px; color: var(--muted); }}
  .right {{ text-align: right; }}
  .footer {{ margin: 16px 0 4px; color: var(--muted); font-size: 12px; }}

  /* ===== Dropdown Galerías ===== */
  .header-row {{ display:flex; align-items:center; gap:12px; margin: 0 0 16px; }}
  .header-row .spacer {{ flex: 1 1 auto; }}
  .dropdown {{ position: relative; }}
  .dropdown-btn {{
    display:flex; align-items:center; gap:8px;
    background:#0b1226; color:var(--text);
    border:1px solid var(--border); border-radius: 999px;
    padding: 10px 14px; cursor: pointer; font-weight: 600;
  }}
  .dropdown-btn .caret {{
    display:inline-block; width:0; height:0;
    border-left:5px solid transparent; border-right:5px solid transparent; border-top:6px solid var(--muted);
    transform: translateY(1px);
  }}
  .dropdown-menu {{
    position:absolute; right:0; top: calc(100% + 8px);
    min-width: 220px; display:none; z-index: 20;
    background: #0b1226; border:1px solid var(--border); border-radius: 12px; padding:6px;
    box-shadow: 0 12px 24px rgba(0,0,0,.35);
  }}
  .dropdown.open .dropdown-menu {{ display:block; }}
  .dropdown-menu a {{
    display:block; text-decoration:none; color:var(--text);
    padding:10px 12px; border-radius: 10px; font-weight: 600;
  }}
  .dropdown-menu a:hover {{ background:#10183a; }}
  .dropdown-menu a.active {{
    background:#12b981; color:#08111e; border:1px solid #0ea371;
  }}

  /* ===== Filtros tabla ===== */
  .filters {{ display:flex; gap:8px; flex-wrap:wrap; width:100%; }}
  .filters .group {{ display:flex; gap:8px; align-items:center; }}
  .filters label {{ font-size:12px; color:var(--muted); }}
  .date-input::-webkit-calendar-picker-indicator {{filter: invert(1); cursor: pointer;}}

  /* ===== Detalle lista ===== */
  .detail-col {{ display: flex; flex-direction: column; gap: 6px; padding: 6px 0; }}
  .detail-line {{ display: flex; padding: 6px 0; padding-left: 20px; font-size: 12px; color: #80FF82; }}
  .detail-line .mono {{ font-family: ui-monospace, monospace; }}

  @keyframes bg-flash {{
    0%   {{ background: #0b1020; }}
    20%  {{ background: #103b2b; }}
    50%  {{ background: #0b1020; }}
    70%  {{ background: #103b2b; }}
    100% {{ background: #0b1020; }}
  }}
  body.flash {{ animation: bg-flash 5s ease-in-out 1; }}

  #newAlert {{
    position: fixed; z-index: 9999; left: 50%; top: 16px; transform: translateX(-50%);
    background: #12b981; color: #08111e; border: 1px solid #0ea371;
    padding: 10px 14px; border-radius: 999px; font-weight: 700; box-shadow: 0 10px 20px rgba(0,0,0,.25);
    opacity: 0; pointer-events: none; transition: opacity .25s ease, transform .25s ease;
  }}
  #newAlert.show {{ opacity: 1; transform: translateX(-50%) translateY(2px); }}

  .tr-new {{
    animation: pulseRow 1.5s ease-in-out 1;
    background: rgba(18, 185, 129, .15);
  }}
  @keyframes pulseRow {{
    0% {{ background: rgba(18,185,129,.4); }}
    100% {{ background: rgba(18,185,129,.15); }}
  }}

  .name-btn {{ background: none; border: 0; color: #c8d5f5; cursor: pointer; text-decoration: underline; padding: 0; font: inherit; }}
  .detail-row td {{ background: #0b1226; border-top: 1px solid var(--border); }}
  .detail-wrap {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
  .chip {{ display:inline-block; padding:4px 8px; border:1px solid var(--border); border-radius:999px; font-family: ui-monospace, monospace; font-size:12px; color:#40B1FF; }}

  .name-cell {{ display:flex; align-items:center; gap:10px; }}
  .avatar {{ width: 28px; height: 28px; border-radius: 50%; background: #0b1226; border: 1px solid var(--border); display:flex; align-items:center; justify-content:center; font-size: 12px; color: #9fb4da; font-weight:700; overflow:hidden; }}
  .avatar img {{ width:100%; height:100%; object-fit:cover; display:block; }}
</style>
</head>
<body>

  <div id="newAlert">¡Nueva detección!</div>

  <div class="wrap">
    <span><img src="https://res.cloudinary.com/df5olfhrq/image/upload/v1756228647/logo_tpskcd.png" alt="BlockSecurity" style="height:80px; margin-bottom:16px;"></span>
    <span style="padding: 5px">
      <img src="https://arrayanes.com/wp-content/uploads/2025/05/LOGO-ARRAYANES-1024x653.webp" alt="Arrayanes" style="height:80px; margin-bottom:16px;">
    </span>

    <!-- Encabezado: título a la izquierda, dropdown a la derecha -->
    <div class="header-row">
      <h1>{html_escape(titulo)} Arrayanes Country Club</h1>
      <div class="spacer"></div>
      <div class="dropdown" id="galDropdown">
        <button class="dropdown-btn" aria-haspopup="true" aria-expanded="false">
          {html_escape(etiqueta_actual)}
          <span class="caret"></span>
        </button>
        <div class="dropdown-menu" role="menu" aria-label="Seleccionar galería">
          <a href="{ruta_empleados}" class="item {activo_empleados}" role="menuitem">Empleados</a>
          <a href="{ruta_socios}" class="item {activo_socios}" role="menuitem">Socios</a>
          <a href="{ruta_proveedores}" class="item {activo_proveedores}" role="menuitem">Proveedores</a>
        </div>
      </div>
    </div>
    
    <div class="grid cards">
      <div class="card">
        <h3>Cobertura de galería</h3>
        <div class="val" id="kpi_gallery_pct">{percent_gallery:.1f}%</div>
        <div class="muted"><span id="kpi_gallery_rec">{recognized_in_gallery}</span>/<span id="kpi_gallery_total">{total_gallery_persons}</span> personas en la galería</div>
      </div>

      <div class="card">
        <h3>Reconocimientos hoy</h3>
        <div class="val" id="kpi_ingresos">{ingresos_hoy}</div>
        <div class="muted">Suma de detecciones de</div><div id="kpi_hoy">{hoy_iso}</div>
      </div>

      <div class="card">
        <h3>Personas únicas hoy</h3>
        <div class="val" id="kpi_personas_hoy">{personas_unicas_hoy}</div>
        <div class="muted">Personas distintas detectadas el</div><div id="kpi_hoy">{hoy_iso}</div>
      </div>

      <div class="card" style="display:none">
        <h3>Registros</h3>
        <div class="val" id="kpi_registros">{total_registros}</div>
      </div>
    </div>

    <div class="section">
      <div class="toolbar"><h2 style="margin-right:auto">Top 10 personas detectadas</h2></div>
      <ul class="list" id="top_list">{top_items}</ul>
    </div>

    <div class="section">
      <div class="toolbar"><h2 style="margin-right:auto">Detecciones por nombre (Top 10 personas)</h2></div>
      <div class="chart-container">
        <canvas id="personasChart" height="100"></canvas>
      </div>
    </div>

    <div class="section">
      <div class="toolbar"><h2 style="margin-right:auto">Detecciones este mes</h2></div>
      <canvas id="top10Chart" height="100"></canvas>
    </div>

    <div class="section">
      <div class="toolbar"><h2 style="margin-right:auto">Detecciones esta semana</h2></div>
      <canvas id="thisWeekChart" height="100"></canvas>
    </div>

    <div class="section">
      <div class="toolbar" style="gap:12px;">
        <h2 style="margin-right:auto">Tabla de detecciones (fecha · nombre · conteo)</h2>
        <input class="input" id="filterAgg" placeholder="Filtra por texto (fecha o nombre)..." style="min-width:260px; display:none;">
      </div>

      <div class="filters" style="margin-bottom:8px;">
        <div class="group">
          <label for="dateStart" class="date-input">Desde</label>
          <input type="date" id="dateStart" class="input">
        </div>

        <div class="group">
          <label for="dateEnd">Hasta</label>
          <input type="date" id="dateEnd" class="input">
        </div>

        <div class="group" style="flex:1;">
          <label for="nameFilter">Persona</label>
          <input type="text" id="nameFilter" class="input w100" placeholder="Ej.: Juan Pérez">
        </div>

        <div class="group">
          <button id="clearFilters" class="input" style="cursor:pointer;">Limpiar</button>
        </div>
      </div>

      <div class="table-wrap">
        <table id="aggTable">
          <thead>
            <tr><th class="th">fecha</th><th class="th">hora (última)</th><th class="th">nombre</th><th class="th right">conteo</th></tr>
          </thead>
          <tbody id="tbodyAgg">{tbody_html}</tbody>
        </table>
      </div>
      <div class="hint">Se agrupan múltiples filas del CSV sumando su columna <code>conteo</code>. Los filtros de fecha y persona se aplican a las filas visibles.</div>
    </div>

    <div class="footer">Generado {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
  </div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
  let CURRENT_LAST_TS = {js_last_ts};
  let TIMES_BY = {times_by_json};

  // Dropdown accesible: abrir/cerrar y cerrar al clicar fuera o con ESC
  (function(){{
    const dd = document.getElementById('galDropdown');
    if (!dd) return;
    const btn = dd.querySelector('.dropdown-btn');
    const menu = dd.querySelector('.dropdown-menu');

    function close() {{
      dd.classList.remove('open');
      btn.setAttribute('aria-expanded', 'false');
    }}

    btn.addEventListener('click', (e) => {{
      e.stopPropagation();
      const nowOpen = !dd.classList.contains('open');
      if (nowOpen) {{
        dd.classList.add('open');
        btn.setAttribute('aria-expanded', 'true');
      }} else {{
        close();
      }}
    }});

    document.addEventListener('click', (e) => {{
      if (!dd.contains(e.target)) close();
    }});

    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape') close();
    }});
  }})();

  // Filtro rápido texto
  (function(){{
    const input = document.getElementById('filterAgg');
    const tbody = document.getElementById('tbodyAgg');
    input && input.addEventListener('input', function(){{
      const q = this.value.toLowerCase();
      for (const tr of tbody.rows) {{
        const txt = tr.innerText.toLowerCase();
        tr.style.display = txt.includes(q) ? '' : 'none';
      }}
      applySpecificFilters();
    }});
  }})();

  // Filtros avanzados
  (function(){{
    const tbody = document.getElementById('tbodyAgg');
    const dateStart = document.getElementById('dateStart');
    const dateEnd = document.getElementById('dateEnd');
    const nameFilter = document.getElementById('nameFilter');
    const clearBtn = document.getElementById('clearFilters');

    function normalizeDateStr(d) {{
      if (!d) return null;
      const parts = d.split('-');
      if (parts.length !== 3) return null;
      const year = parseInt(parts[0],10);
      const month = parseInt(parts[1],10)-1;
      const day = parseInt(parts[2],10);
      const dt = new Date(Date.UTC(year, month, day));
      return isNaN(dt.getTime()) ? null : dt;
    }}

    function parseCellDateStr(s) {{ return normalizeDateStr(s.trim()); }}

    window.applySpecificFilters = function applySpecificFilters() {{
      const start = normalizeDateStr(dateStart.value);
      const end = normalizeDateStr(dateEnd.value);
      const nameQ = (nameFilter.value || '').toLowerCase();

      for (const tr of tbody.rows) {{
        if (tr.classList.contains('detail-row')) {{
          const prev = tr.previousElementSibling;
          if (!prev) {{ tr.remove(); continue; }}
          tr.style.display = prev.style.display;
          continue;
        }}

        const cellDateStr = tr.cells[0].innerText || '';
        const cellNameStr = tr.cells[2].innerText || '';
        const rowDate = parseCellDateStr(cellDateStr);
        const nameOk = !nameQ || cellNameStr.toLowerCase().includes(nameQ);

        let dateOk = true;
        if (start && (!rowDate || rowDate < start)) dateOk = false;
        if (end) {{
          const endAdj = new Date(end.getTime() + 24*60*60*1000 - 1);
          if (!rowDate || rowDate > endAdj) dateOk = false;
        }}

        const freeQ = (document.getElementById('filterAgg')?.value || '').toLowerCase();
        const freeOk = !freeQ || tr.innerText.toLowerCase().includes(freeQ);

        tr.style.display = (nameOk && dateOk && freeOk) ? '' : 'none';
      }}

      collapseHiddenDetails();
    }}

    dateStart && dateStart.addEventListener('change', applySpecificFilters);
    dateEnd && dateEnd.addEventListener('change', applySpecificFilters);
    nameFilter && nameFilter.addEventListener('input', applySpecificFilters);
    clearBtn && clearBtn.addEventListener('click', function(){{
      dateStart.value = '';
      dateEnd.value = '';
      nameFilter.value = '';
      const fa = document.getElementById('filterAgg');
      if (fa) fa.value = '';
      applySpecificFilters();
    }});

    applySpecificFilters();
  }})();

  function collapseHiddenDetails(){{
    const tbody = document.getElementById('tbodyAgg');
    if (!tbody) return;
    for (const tr of Array.from(tbody.querySelectorAll('tr.detail-row'))) {{
      const prev = tr.previousElementSibling;
      if (!prev || prev.style.display === 'none') tr.remove();
    }}
  }}

  // Expandir detalle por nombre
  (function attachExpandHandler(){{
    const tbody = document.getElementById('tbodyAgg');
    if (!tbody) return;

    tbody.addEventListener('click', function(e){{
      const btn = e.target.closest('.name-btn');
      if (!btn) return;

      const tr = btn.closest('tr');
      const key = tr.getAttribute('data-key');
      const next = tr.nextElementSibling;
      const alreadyOpen = next && next.classList.contains('detail-row');

      if (alreadyOpen) {{ next.remove(); return; }}
      if (next && next.classList.contains('detail-row')) next.remove();

      const arr = (TIMES_BY[key] || []);

      const lines = arr.length
        ? arr.map(o => {{
            const sid = o.sid || '';
            const cid = o.cid || '';
            const ts  = o.ts  || '';

            const url = (sid && cid && ts)
              ? `https://dashboard.verifyfaces.com/company/54/stream/${{sid}}/camera/${{cid}}?timestamp=${{encodeURIComponent(ts)}}&search=real-time`
              : '';

            const horaChip = url
              ? `<a class="chip" href="${{url}}" target="_blank" rel="noopener noreferrer">${{o.h}}</a>`
              : `<span class="chip">${{o.h}}</span>`;

            const camaraChip = url
              ? `<a class="chip" href="${{url}}" target="_blank" rel="noopener noreferrer">${{o.c}}</a>`
              : `<span class="chip">${{o.c}}</span>`;

            return `<div class="detail-line">Persona reconocida a las ${{horaChip}} en la cámara <span>${{camaraChip || 'N/D'}}</span></div>`;
        }}).join('')
        : `<em class="muted">Sin horas registradas</em>`;

      const detail = document.createElement('tr');
      detail.className = 'detail-row';
      detail.innerHTML = `<td colspan="4">
        <div class="detail-col">
          ${{lines}}
        </div>
      </td>`;

      tr.parentNode.insertBefore(detail, tr.nextSibling);
    }});
  }})();

  function triggerVisualAlert() {{
    const alertEl = document.getElementById('newAlert');
    document.body.classList.add('flash');
    alertEl.classList.add('show');
    setTimeout(() => {{
      alertEl.classList.remove('show');
      document.body.classList.remove('flash');
    }}, 5000);
  }}

  // Charts
  const personasCtx = document.getElementById('personasChart').getContext('2d');
  const top10Ctx    = document.getElementById('top10Chart').getContext('2d');
  const weekCtx     = document.getElementById('thisWeekChart').getContext('2d');

  const personasChart = new Chart(personasCtx, {{
    type: 'bar',
    data: {{ labels: {personasChart_labels}, datasets: [{{ label: 'Detecciones', data: {personasChart_data}, backgroundColor: 'rgba(255,206,86,.7)', borderColor: 'rgba(255,206,86,1)', borderWidth: 1 }}] }},
    options: {{ responsive: true, indexAxis: 'y', scales: {{ x: {{ beginAtZero: true, ticks: {{ precision:0 }} }} }} }}
  }});

  const top10Chart = new Chart(top10Ctx, {{
    type: 'bar',
    data: {{ labels: {top10_labels}, datasets: {top10_datasets} }},
    options: {{ responsive: true, scales: {{ x: {{ stacked: true }}, y: {{ stacked: true, beginAtZero: true, ticks: {{ precision:0 }} }} }} }}
  }});

  const thisWeekChart = new Chart(weekCtx, {{
    type: 'bar',
    data: {{ labels: {week_labels}, datasets: {week_datasets} }},
    options: {{ responsive: true, scales: {{ x: {{ stacked: true }}, y: {{ stacked: true, beginAtZero: true, ticks: {{ precision:0 }} }} }} }}
  }});

  function applyEstado(estado) {{
    const k = estado.kpis || {{}};
    document.getElementById('kpi_gallery_pct').innerText = (k.percent_gallery ?? 0).toFixed(1) + '%';
    document.getElementById('kpi_gallery_rec').innerText = k.recognized_in_gallery ?? 0;
    document.getElementById('kpi_gallery_total').innerText = k.total_gallery_persons ?? 0;
    document.getElementById('kpi_ingresos').innerText = k.ingresos_hoy ?? 0;
    document.getElementById('kpi_hoy').innerText = k.hoy_iso ?? '';
    document.getElementById('kpi_personas_hoy').innerText = k.personas_unicas_hoy ?? 0;
    const kpiReg = document.getElementById('kpi_registros'); if (kpiReg) kpiReg.innerText = k.total_registros ?? 0;

    document.getElementById('top_list').innerHTML = estado.top_items_html || '';

    const tbody = document.getElementById('tbodyAgg');
    const oldHTML = tbody.innerHTML;
    tbody.innerHTML = estado.tbody_html || '';
    if (tbody.innerHTML !== oldHTML) {{
      if (typeof applySpecificFilters === 'function') applySpecificFilters();
      triggerVisualAlert();
    }}

    TIMES_BY = estado.times_by || {{}};

    personasChart.data.labels = (estado.personasChart && estado.personasChart.labels) || [];
    personasChart.data.datasets[0].data = (estado.personasChart && estado.personasChart.data) || [];
    personasChart.update();

    top10Chart.data.labels = (estado.top10Chart && estado.top10Chart.labels) || [];
    top10Chart.data.datasets = (estado.top10Chart && estado.top10Chart.datasets) || [];
    top10Chart.update();

    thisWeekChart.data.labels = (estado.thisWeekChart && estado.thisWeekChart.labels) || [];
    thisWeekChart.data.datasets = (estado.thisWeekChart && estado.thisWeekChart.datasets) || [];
    thisWeekChart.update();

    CURRENT_LAST_TS = estado.last_ts_iso || CURRENT_LAST_TS;
  }}

  // SSE: ojo que usa la galería actual
  const es = new EventSource('/stream?gallery={gallery_id}');
  es.addEventListener('update', (ev) => {{
    try {{
      const data = JSON.parse(ev.data);
      applyEstado(data);
    }} catch (e) {{}}
  }});
  es.onerror = () => {{}};
</script>
</body>
</html>
"""



# =========================
# Ingesta en segundo plano (por galería)
# =========================

def _fetch_and_write_csv_for_gallery(gallery_id: int, total_records_needed: int = TOTAL_RECORDS_NEEDED) -> bool:
    """
    Descarga eventos realtime (últimas 24h), filtra por gallery_id, y escribe el CSV de esa galería.
    Devuelve True si hubo cambios escritos.
    """
    global TOKEN, _last_fetch_hash_by

    csv_path = CSV_FILES[gallery_id]

    with _ingest_lock:
        # Asegurar token + cache de galería
        if not TOKEN and not obtener_nuevo_token():
            print(f"Ingesta[{gallery_id}]: no se pudo autenticar")
            return False
        if not cargar_cache_galeria(gallery_id):
            print(f"Ingesta[{gallery_id}]: no se pudo cargar galería")
            return False

        headers = {"Authorization": f"Bearer {TOKEN}"}
        try:
            from_dt_utc = datetime.now(timezone.utc) - timedelta(days=1)
            to_dt_utc   = datetime.now(timezone.utc)

            # Nota: se conserva el formato original del código
            from_str = from_dt_utc.strftime("%Y-08-%dT%H:%M:%S.000Z")
            to_str   = to_dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            all_searches = []
            num_pages = math.ceil(total_records_needed / PER_PAGE)

            for page_num in range(1, num_pages + 1):
                params = {"from": from_str, "to": to_str, "page": page_num, "perPage": PER_PAGE}
                response = requests.get(API_URL, headers=headers, params=params, timeout=15)
                if response.status_code == 401:
                    if not obtener_nuevo_token():
                        return False
                    headers["Authorization"] = f"Bearer {TOKEN}"
                    response = requests.get(API_URL, headers=headers, params=params, timeout=15)
                response.raise_for_status()

                data = response.json()
                searches_on_page = data.get("searches", []) or []
                all_searches.extend(searches_on_page)
                if len(searches_on_page) < PER_PAGE:
                    break

            # Filtra SOLO las búsquedas de la galería indicada
            filtered = []
            for s in all_searches:
                try:
                    g = (((s or {}).get("payload", {}) or {}).get("image", {}) or {}).get("gallery", {}) or {}
                    gid = g.get("id")
                    if gid == gallery_id:
                        filtered.append(s)
                except Exception:
                    continue

            filtered = filtered[:total_records_needed]

            # Hash para esta galería
            payload_keys = []
            for s in filtered:
                sid = s.get("id", "")
                t = (s.get("result", {}).get("image", {}) or {}).get("time", "")
                camid = (s.get("payload", {}).get("camera", {}) or {}).get("id", "")
                payload_keys.append(f"{sid}|{t}|{camid}")
            new_hash = hash("|".join(payload_keys))
            if new_hash == _last_fetch_hash_by.get(gallery_id):
                return False

            # Escribir CSV
            with csv_path.open(mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["fecha", "hora", "nombre_persona", "conteo", "camara", "search_id", "camera_id", "ts_utc"])
                for search in filtered:
                    if not search.get("payload") or not search["payload"].get("image"):
                        continue

                    original_filename = search["payload"]["image"].get("originalFilename")
                    ts_raw = (search.get("result", {}).get("image", {}) or {}).get("time")
                    camera_obj = (search.get("payload", {}).get("camera", {}) or {}) or {}
                    camera_name = camera_obj.get("name", "")
                    camera_id = camera_obj.get("id", "")
                    search_id = search.get("id", "")

                    # nombre desde el cache de ESTA galería
                    entry = GALLERY_CACHE[gallery_id].get(original_filename, {})
                    metadata = entry.get("metadata", {}) if entry else {}
                    person_name = metadata.get("name", "Nombre Desconocido")

                    try:
                        # VerifyFaces time viene como "%Y%m%d%H%M%S.%f"
                        dt_utc  = datetime.strptime(ts_raw, "%Y%m%d%H%M%S.%f")
                        # Ajuste a America/Guayaquil (-05:00) si aplica
                        dt_local = dt_utc - timedelta(hours=5)
                        fecha_str = dt_local.date().isoformat()
                        hora_str  = dt_local.strftime("%H:%M:%S")
                        ts_iso_z = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                        writer.writerow([fecha_str, hora_str, person_name, 1, camera_name, search_id, camera_id, ts_iso_z])
                    except Exception:
                        continue

            _last_fetch_hash_by[gallery_id] = new_hash
            print(f"Ingesta[{gallery_id}]: escrito CSV {csv_path.name} con {len(filtered)} registros")
            return True

        except requests.exceptions.RequestException as e:
            print(f"Ingesta[{gallery_id}]: error de red/API: {e}")
            return False
        except Exception as e:
            print(f"Ingesta[{gallery_id}]: error inesperado: {e}")
            return False


def background_worker():
    while not _stop_event.is_set():
        try:
            for gid in GALLERY_IDS:
                _fetch_and_write_csv_for_gallery(gid, TOTAL_RECORDS_NEEDED)
        finally:
            _stop_event.wait(FETCH_PERIOD_SECONDS)


def _get_resources_for_gallery(gallery_id: int):
    if gallery_id in CSV_FILES:
        csv_path: Path = CSV_FILES[gallery_id]
        person_img_map = PERSON_IMG_MAP_BY.get(gallery_id, {})
    else:
        csv_path = CSV_FILES[EMP_GALLERY_ID]
        person_img_map = PERSON_IMG_MAP_BY.get(EMP_GALLERY_ID, {})
    return csv_path, person_img_map



# =========================
# Endpoints
# =========================

@app.route("/login")
def login():
    return render_template_string("""
    <!doctype html>
    <html lang="es">
    <head><title>Login</title><style>
        body { font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background: #0b1020; color: #fff; }
        .login-box { padding: 40px; border-radius: 8px; background: #12172a; text-align: center; border: 1px solid #223052; }
        .login-box h1 { margin-bottom: 20px; color: #e8eefc; }
        .google-btn {
            background: #fff; color: #000; padding: 10px 20px; border-radius: 4px; text-decoration: none; font-weight: bold;
            display: inline-flex; align-items: center; gap: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
        .google-btn img { height: 20px; }
    </style></head>
    <body>
        <div class="login-box">
            <span><img src="https://res.cloudinary.com/df5olfhrq/image/upload/v1756228647/logo_tpskcd.png" alt="BlockSecurity" style="height:80px; margin-bottom:16px;"></span>
            <h1>Iniciar Sesión</h1>
            <a href="/login_google" class="google-btn">
                <img src="https://www.google.com/favicon.ico" alt="Google icon">
                Iniciar sesión con Google
            </a>
        </div>
    </body>
    </html>
    """)

@app.route("/login_google")
def login_google():
    google_provider_cfg = get_google_provider_cfg()
    authorization_endpoint = google_provider_cfg["authorization_endpoint"]
    request_uri = client.prepare_request_uri(
        authorization_endpoint,
        redirect_uri=request.base_url.replace("/login_google", "/callback"),
        scope=["openid", "email", "profile"],
    )
    return redirect(request_uri)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    google_provider_cfg = get_google_provider_cfg()
    token_endpoint = google_provider_cfg["token_endpoint"]
    token_url, headers, body = client.prepare_token_request(
        token_endpoint,
        authorization_response=request.url,
        redirect_url=request.base_url,
        code=code
    )
    token_response = requests.post(
        token_url,
        headers=headers,
        data=body,
        auth=(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET),
    ).json()

    if "id_token" not in token_response:
        return "Failed to get ID token.", 400

    client.parse_request_body_response(json.dumps(token_response))
    userinfo_endpoint = google_provider_cfg["userinfo_endpoint"]
    uri, headers, body = client.add_token(userinfo_endpoint)
    userinfo_response = requests.get(uri, headers=headers, data=body).json()

    if userinfo_response.get("email_verified"):
        session["user_email"] = userinfo_response["email"]
        if session["user_email"] in AUTHORIZED_USERS:
            return redirect(url_for("index"))
        else:
            return "Unauthorized user.", 403
    else:
        return "User email not available or not verified.", 400

@app.route("/logout")
def logout():
    session.pop("user_email", None)
    return redirect(url_for("login"))

@app.before_request
def before_request_func():
    g.user = None
    if "user_email" in session:
        g.user = session["user_email"]


@app.route("/")
@login_required
def index():
    _ensure_background_started()
    gallery_id = EMP_GALLERY_ID
    csv_path, person_img_map = _get_resources_for_gallery(gallery_id)
    registros, agg, agg_hora_latest, fechas, personas, personas_total = leer_csv(csv_path)
    estado = construir_estado_dashboard(registros, agg, agg_hora_latest, fechas, personas, personas_total, person_img_map)
    return render_template_string(construir_html_dashboard_bootstrap(estado, gallery_id=gallery_id, titulo="Dashboard Empleados"))


@app.route("/socios")
@login_required
def socios():
    _ensure_background_started()
    gallery_id = SOC_GALLERY_ID
    csv_path, person_img_map = _get_resources_for_gallery(gallery_id)
    registros, agg, agg_hora_latest, fechas, personas, personas_total = leer_csv(csv_path)
    estado = construir_estado_dashboard(registros, agg, agg_hora_latest, fechas, personas, personas_total, person_img_map)
    return render_template_string(construir_html_dashboard_bootstrap(estado, gallery_id=gallery_id, titulo="Dashboard Socios"))


@app.route("/proveedores")
@login_required
def proveedores():
    _ensure_background_started()
    gallery_id = PROV_GALLERY_ID
    csv_path, person_img_map = _get_resources_for_gallery(gallery_id)
    registros, agg, agg_hora_latest, fechas, personas, personas_total = leer_csv(csv_path)
    estado = construir_estado_dashboard(registros, agg, agg_hora_latest, fechas, personas, personas_total, person_img_map)
    return render_template_string(construir_html_dashboard_bootstrap(estado, gallery_id=gallery_id, titulo="Dashboard Proveedores"))


@app.route("/api/stats")
def api_stats():
    csv_path, _ = _get_resources_for_gallery(EMP_GALLERY_ID)
    registros, _, _, _, _, _ = leer_csv(csv_path)
    total = sum((r.get("conteo", 0) or 0) for r in registros) if registros else 0
    ultima = None
    for r in registros:
        f = r.get("fecha") or ""
        h = r.get("hora") or ""
        if f and h:
            try:
                dt = datetime.fromisoformat(f)
                hh, mm, ss = h.split(":")
                dt = dt.replace(hour=int(hh), minute=int(mm), second=int(ss))
                if (ultima is None) or (dt > ultima):
                    ultima = dt
            except Exception:
                continue
    ultima_iso = ultima.isoformat() if ultima else ""
    return jsonify({"total_registros": total, "ultima_ts_iso": ultima_iso})


@app.route("/stream")
def stream():
    _ensure_background_started()
    try:
        gallery_id = int(request.args.get("gallery", str(EMP_GALLERY_ID)))
    except Exception:
        gallery_id = EMP_GALLERY_ID

    csv_path, person_img_map = _get_resources_for_gallery(gallery_id)

    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

    def gen():
        last_mtime = None
        keepalive_every = 15
        last_ping = time.monotonic()
        try:
            while not _stop_event.is_set():
                try:
                    current_mtime = os.path.getmtime(csv_path) if csv_path.exists() else None
                except Exception:
                    current_mtime = None

                if current_mtime and current_mtime != last_mtime:
                    last_mtime = current_mtime
                    registros, agg, agg_hora_latest, fechas, personas, personas_total = leer_csv(csv_path)
                    estado = construir_estado_dashboard(registros, agg, agg_hora_latest, fechas, personas, personas_total, person_img_map)
                    yield f"event: update\ndata: {json.dumps(estado, ensure_ascii=False)}\n\n"

                now = time.monotonic()
                if now - last_ping >= keepalive_every:
                    yield ": keep-alive\n\n"
                    last_ping = now

                time.sleep(STREAM_POLL_MTIME_SECONDS)

        except (GeneratorExit, SystemExit):
            return
        except Exception:
            try:
                yield "event: error\ndata: {{\"msg\":\"sse-internal-error\"}}\n\n"
            except Exception:
                pass
            return

    return Response(gen(), headers=headers)


# =========================
# Arranque (opcional: test de galería)
# =========================

def obtener_imagenes_galeria(gallery_id: int = EMP_GALLERY_ID):
    """Prueba rápida de acceso a galería (logs)."""
    global TOKEN
    if not TOKEN:
        if not obtener_nuevo_token():
            print("Error: No se pudo obtener un token de autenticación.")
            return
    gallery_url = f"https://dashboard-api.verifyfaces.com/companies/54/galleries/{gallery_id}"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"perPage": 50, "page": 1}
    try:
        print(f"Haciendo llamada a la API de la galería {gallery_id}...")
        response = requests.get(gallery_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        _ = response.json()
        print(f"Respuesta OK a la API de la galería {gallery_id}")
    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP al obtener imágenes: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión al obtener imágenes: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")


if __name__ == "__main__":
    try:
        obtener_imagenes_galeria(EMP_GALLERY_ID)
        obtener_imagenes_galeria(SOC_GALLERY_ID)
        obtener_imagenes_galeria(PROV_GALLERY_ID)
    except Exception as e:
        print("Init galleries error:", e)

    # Lanzar hilo de ingesta
    t = threading.Thread(target=background_worker, daemon=True)
    t.start()
    try:
        app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
    finally:
        _stop_event.set()
