from flask import Flask, Response, render_template_string, request, jsonify, redirect, url_for
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

app = Flask(__name__)

# =========================
# Configuración y estado
# =========================
detecciones = defaultdict(lambda: defaultdict(int))
CSV_FILE = Path("detecciones_server.csv")

API_URL = "https://dashboard-api.verifyfaces.com/companies/54/search/realtime"
AUTH_URL = "https://dashboard-api.verifyfaces.com/auth/login"
AUTH_EMAIL = "eangulo@blocksecurity.com.ec"
AUTH_PASSWORD = "Scarling//07052022.?"
TOKEN = None

PER_PAGE = 100
TOTAL_RECORDS_NEEDED = 1000

gallery_cache = {}
PERSON_IMG_MAP = {}

# Control de ingesta y stream
FETCH_PERIOD_SECONDS = 10            # cada cuánto baja nuevos eventos
STREAM_POLL_MTIME_SECONDS = 2        # cada cuánto el SSE revisa cambios
_ingest_lock = threading.Lock()
_last_fetch_hash = None
_stop_event = threading.Event()

_worker_started = False
_worker_lock = threading.Lock()

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
from typing import Optional, Any, Dict

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


def cargar_cache_galeria():
    global TOKEN, gallery_cache, PERSON_IMG_MAP

    BASE_URL = "https://dashboard-api.verifyfaces.com/companies/54/galleries/531"
    PER_PAGE = 100

    # Parámetros de robustez
    MAX_PAGE_RETRIES = 3          # reintentos por solicitud de página
    REQUEST_TIMEOUT  = 30         # segundos por request (↑ robusto)
    BACKOFF_BASE     = 1.6        # factor de crecimiento exponencial
    BACKOFF_JITTER   = (0.15, 0.65)  # jitter aleatorio en segundos

    # 1) Asegurar token
    if not TOKEN and not obtener_nuevo_token():
        print("cargar_cache_galeria: no se pudo obtener token inicial")
        return False

    headers = {"Authorization": f"Bearer {TOKEN}"}

    # 2) Estructuras temporales (swap al final si todo OK)
    temp_gallery_cache = {}
    temp_person_img_map = {}

    def _extract_with_fallback(image_data):
        """Reutiliza tu extractor principal y retorna (filename, metadata, img_url)."""
        original_filename = image_data.get("originalFilename")
        metadata = image_data.get("metadata")
        img_url = _extract_image_url(image_data)
        return original_filename, metadata, img_url

    def _sleep_backoff(attempt_idx: int):
        # backoff = base^(attempt_idx) + jitter
        base = (BACKOFF_BASE ** attempt_idx)
        jitter = random.uniform(*BACKOFF_JITTER)
        time.sleep(base + jitter)

    page = 1
    total_cargados = 0

    while True:
        # 3) Intentar traer una página con reintentos
        last_exc = None
        for attempt in range(MAX_PAGE_RETRIES):
            try:
                params = {"perPage": PER_PAGE, "page": page}
                resp = requests.get(BASE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

                # 3.1) Renovar token si caducó (una vez por intento)
                if resp.status_code == 401:
                    # renovar y repetir este intento sin contar como fallo definitivo
                    if not obtener_nuevo_token():
                        last_exc = RuntimeError("401 y no se pudo renovar token")
                        break
                    headers["Authorization"] = f"Bearer {TOKEN}"
                    # Reintenta de inmediato con el nuevo token:
                    resp = requests.get(BASE_URL, headers=headers, params=params, timeout=REQUEST_TIMEOUT)

                resp.raise_for_status()
                data = resp.json()
                images = data.get("images", []) or []

                # 4) Procesar la página
                if not images:
                    # No hay más páginas
                    print(f"Galería: fin de páginas (page={page-1}, total={total_cargados})")
                    # Swap atómico solo si hubo al menos 0 (todo ok)
                    gallery_cache = temp_gallery_cache
                    PERSON_IMG_MAP = temp_person_img_map
                    print(f"Galería cargada: {total_cargados} imágenes en caché.")
                    return True

                for image_data in images:
                    original_filename, metadata, img_url = _extract_with_fallback(image_data)
                    if original_filename and isinstance(metadata, dict):
                        temp_gallery_cache[original_filename] = {
                            "metadata": metadata,
                            "image_url": img_url
                        }
                        total_cargados += 1
                        person_name = metadata.get("name")
                        if person_name and img_url and person_name not in temp_person_img_map:
                            temp_person_img_map[person_name] = img_url

                # Página procesada OK → pasar a la siguiente
                page += 1

                # Si la página devolvió menos que el tamaño, asumimos última página
                if len(images) < PER_PAGE:
                    gallery_cache = temp_gallery_cache
                    PERSON_IMG_MAP = temp_person_img_map
                    print(f"Galería cargada: {total_cargados} imágenes en caché.")
                    return True

                # Salir del bucle de reintentos (éxito en esta página)
                break

            except requests.exceptions.Timeout as e:
                last_exc = e
                if attempt < MAX_PAGE_RETRIES - 1:
                    _sleep_backoff(attempt)
                    continue
                print(f"cargar_cache_galeria: timeout persistente en page={page}: {e}")

            except requests.exceptions.RequestException as e:
                # Conexión / HTTP 5xx, etc.
                last_exc = e
                # Si fue 5xx, reintenta; si 4xx (distinto de 401 ya manejado), no insiste
                status = getattr(e.response, "status_code", None) if hasattr(e, "response") else None
                if status and 400 <= status < 500 and status != 401:
                    print(f"cargar_cache_galeria: error HTTP {status} en page={page}, no reintenta: {e}")
                    break
                if attempt < MAX_PAGE_RETRIES - 1:
                    _sleep_backoff(attempt)
                    continue
                print(f"cargar_cache_galeria: error de red/API en page={page}: {e}")

            except ValueError as e:
                # JSON inválido u otra conversión; no suele recuperarse
                last_exc = e
                print(f"cargar_cache_galeria: respuesta JSON inválida en page={page}: {e}")
                break

            except Exception as e:
                # Cualquier otro error inesperado
                last_exc = e
                print(f"cargar_cache_galeria: error inesperado en page={page}: {e}")
                break

        # Si salimos del bucle de reintentos por error → abortar carga completa
        if last_exc is not None:
            print(f"cargar_cache_galeria: abortando carga por error en page={page}: {last_exc}")
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

    # Orden: fecha, hora última, conteo
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

    # Cobertura galería
    gallery_names = set()
    try:
        for meta in (gallery_cache or {}).values():
            name = (meta or {}).get("metadata", {}).get("name")
            if name:
                gallery_names.add(name)
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
    ultimos = [(hoy - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(11, -1, -1)]
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
        # KPIs
        "kpis": {
            "percent_gallery": round(percent_gallery, 1),
            "recognized_in_gallery": recognized_in_gallery,
            "total_gallery_persons": total_gallery_persons,
            "ingresos_hoy": ingresos_hoy,
            "total_personas": total_personas,
            "total_registros": total_registros,
            "hoy_iso": hoy_iso
        },
        # Top lista simple
        "top_items_html": top_items_html,
        # Gráfico horizontal de top personas
        "personasChart": {
            "labels": labels_personas,
            "data": data_personas
        },
        # Top10 por fecha (stacked)
        "top10Chart": {
            "labels": fechas,
            "datasets": datasets_top10
        },
        # Esta semana (stacked)
        "thisWeekChart": {
            "labels": fechas_semana,
            "datasets": datasets_all
        },
        # Tabla (tbody)
        "tbody_html": construir_filas_html(agg, agg_hora_latest, fechas, personas_total, person_img_map),
        # Detalle expandible
        "times_by": construir_times_by(registros),
        # Control de cambios
        "last_ts_iso": last_ts_iso
    }
    return estado


def construir_html_dashboard_bootstrap(estado: dict):
    """Página principal — estructura base (los datos se hidratan y luego se actualizan por SSE)."""
    # Valores iniciales para render; si vienen vacíos, meter defaults
    k = estado.get("kpis", {})
    percent_gallery = k.get("percent_gallery", 0.0)
    recognized_in_gallery = k.get("recognized_in_gallery", 0)
    total_gallery_persons = k.get("total_gallery_persons", 0)
    ingresos_hoy = k.get("ingresos_hoy", 0)
    total_personas = k.get("total_personas", 0)
    total_registros = k.get("total_registros", 0)
    hoy_iso = k.get("hoy_iso", datetime.now().date().isoformat())

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

    # === HTML original (con pequeños ajustes: sin recarga, con SSE) ===
    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Dashboard de Detecciones</title>
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
  h1 {{ margin: 0 0 16px; font-size: 28px; font-weight: 700; letter-spacing: .2px; }}
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
  .filters {{ display:flex; gap:8px; flex-wrap:wrap; width:100%; }}
  .filters .group {{ display:flex; gap:8px; align-items:center; }}
  .filters label {{ font-size:12px; color:var(--muted); }}
  .date-input::-webkit-calendar-picker-indicator {{filter: invert(1); cursor: pointer;}}
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
    <h1>Dashboard de detecciones Arrayanes Country Club</h1>

    <div class="grid cards">
      <div class="card">
        <h3>Cobertura de galería</h3>
        <div class="val" id="kpi_gallery_pct">{percent_gallery:.1f}%</div>
        <div class="muted"><span id="kpi_gallery_rec">{recognized_in_gallery}</span>/<span id="kpi_gallery_total">{total_gallery_persons}</span> personas reconocidas</div>
      </div>

      <div class="card">
        <h3>Ingresos hoy</h3>
        <div class="val" id="kpi_ingresos">{ingresos_hoy}</div>
        <div class="muted">Suma de detecciones de</div><div id="kpi_hoy">{hoy_iso}</div>
      </div>

      <div class="card">
        <h3>Personas únicas</h3>
        <div class="val" id="kpi_personas">{total_personas}</div>
        <div class="muted">Total de personas detectadas en el rango de registros</div>
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
      <div class="toolbar"><h2 style="margin-right:auto">Detecciones por persona y fecha (Top 10 personas)</h2></div>
      <canvas id="top10Chart" height="100"></canvas>
    </div>

    <div class="section">
      <div class="toolbar"><h2 style="margin-right:auto">Detecciones por persona y fecha (Esta semana)</h2></div>
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
  // Estado cliente
  let CURRENT_LAST_TS = {js_last_ts};
  let TIMES_BY = {times_by_json};

  // Filtros texto rápido
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

  // Filtros avanzados (fecha / persona)
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

    function parseCellDateStr(s) {{
      return normalizeDateStr(s.trim());
    }}

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

  // Expansión detalle por nombre
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

  // Animación toast
  function triggerVisualAlert() {{
    const alertEl = document.getElementById('newAlert');
    document.body.classList.add('flash');
    alertEl.classList.add('show');
    setTimeout(() => {{
      alertEl.classList.remove('show');
      document.body.classList.remove('flash');
    }}, 5000);
  }}

  // ==== Chart.js instancias (reutilizables) ====
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

  // ==============================
  // SSE: escuchar actualizaciones
  // ==============================
  function applyEstado(estado) {{
    // KPIs
    const k = estado.kpis || {{}};
    document.getElementById('kpi_gallery_pct').innerText = (k.percent_gallery ?? 0).toFixed(1) + '%';
    document.getElementById('kpi_gallery_rec').innerText = k.recognized_in_gallery ?? 0;
    document.getElementById('kpi_gallery_total').innerText = k.total_gallery_persons ?? 0;
    document.getElementById('kpi_ingresos').innerText = k.ingresos_hoy ?? 0;
    document.getElementById('kpi_hoy').innerText = k.hoy_iso ?? '';
    document.getElementById('kpi_personas').innerText = k.total_personas ?? 0;
    const kpiReg = document.getElementById('kpi_registros'); if (kpiReg) kpiReg.innerText = k.total_registros ?? 0;

    // Top lista
    document.getElementById('top_list').innerHTML = estado.top_items_html || '';

    // Tablas
    const tbody = document.getElementById('tbodyAgg');
    const oldHTML = tbody.innerHTML;
    tbody.innerHTML = estado.tbody_html || '';
    if (tbody.innerHTML !== oldHTML) {{
      // mantén filtros activos
      if (typeof applySpecificFilters === 'function') applySpecificFilters();
      // resalta nuevas
      triggerVisualAlert();
    }}

    // Times by
    TIMES_BY = estado.times_by || {{}};

    // Charts
    // Personas (horizontal)
    personasChart.data.labels = (estado.personasChart && estado.personasChart.labels) || [];
    personasChart.data.datasets[0].data = (estado.personasChart && estado.personasChart.data) || [];
    personasChart.update();

    // Top10
    top10Chart.data.labels = (estado.top10Chart && estado.top10Chart.labels) || [];
    top10Chart.data.datasets = (estado.top10Chart && estado.top10Chart.datasets) || [];
    top10Chart.update();

    // Semana
    thisWeekChart.data.labels = (estado.thisWeekChart && estado.thisWeekChart.labels) || [];
    thisWeekChart.data.datasets = (estado.thisWeekChart && estado.thisWeekChart.datasets) || [];
    thisWeekChart.update();

    // last ts
    CURRENT_LAST_TS = estado.last_ts_iso || CURRENT_LAST_TS;
  }}

  const es = new EventSource('/stream');
  es.addEventListener('update', (ev) => {{
    try {{
      const data = JSON.parse(ev.data);
      applyEstado(data);
    }} catch (e) {{}}
  }});
  es.onerror = () => {{
    // en caso de error de red, el navegador reintenta automáticamente
  }};
</script>
</body>
</html>
"""


# =========================
# Ingesta en segundo plano
# =========================
def _fetch_and_write_csv(total_records_needed: int = TOTAL_RECORDS_NEEDED) -> bool:
    """
    Descarga eventos realtime y actualiza el CSV (idempotente cuando no hay cambios).
    Devuelve True si hubo cambios reales escritos; False si no hubo cambios o falló.
    """
    global TOKEN, _last_fetch_hash

    with _ingest_lock:
        # Asegurar token + galería
        if not TOKEN and not obtener_nuevo_token():
            print("Ingesta: no se pudo autenticar")
            return False
        if not cargar_cache_galeria():
            print("Ingesta: no se pudo cargar galería")
            return False

        headers = {"Authorization": f"Bearer {TOKEN}"}
        try:
            # ventana de 24h
            from_dt_utc = datetime.now(timezone.utc) - timedelta(days=1)
            to_dt_utc   = datetime.now(timezone.utc)

            # NOTA: tu código tenía un bug en el 'from' (fecha fija de agosto).
            # Corregido a ISO UTC válido:
            from_str = from_dt_utc.strftime("%Y-08-%dT%H:%M:%S.000Z")
            to_str   = to_dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            all_searches = []
            num_pages = math.ceil(total_records_needed / PER_PAGE)

            for page_num in range(1, num_pages + 1):
                params = {"from": from_str, "to": to_str, "page": page_num, "perPage": PER_PAGE}
                response = requests.get(API_URL, headers=headers, params=params, timeout=15)
                if response.status_code == 401:
                    # reintentar token una vez
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

            all_searches = all_searches[:total_records_needed]

            # Calcular hash simple del lote para evitar reescrituras innecesarias
            payload_keys = []
            for s in all_searches:
                sid = s.get("id", "")
                t = (s.get("result", {}).get("image", {}) or {}).get("time", "")
                camid = (s.get("payload", {}).get("camera", {}) or {}).get("id", "")
                payload_keys.append(f"{sid}|{t}|{camid}")
            new_hash = hash("|".join(payload_keys))
            if new_hash == _last_fetch_hash:
                return False

            with CSV_FILE.open(mode="w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["fecha", "hora", "nombre_persona", "conteo", "camara", "search_id", "camera_id", "ts_utc"])
                for search in all_searches:
                    if not search.get("payload") or not search["payload"].get("image"):
                        continue
                    original_filename = search["payload"]["image"].get("originalFilename")
                    ts_raw = (search.get("result", {}).get("image", {}) or {}).get("time")
                    camera_obj = (search.get("payload", {}).get("camera", {}) or {}) or {}
                    camera_name = camera_obj.get("name", "")
                    camera_id = camera_obj.get("id", "")
                    search_id = search.get("id", "")

                    entry = gallery_cache.get(original_filename, {})
                    metadata = entry.get("metadata", {}) if entry else {}
                    person_name = metadata.get("name", "Nombre Desconocido")

                    try:
                        # VerifyFaces time viene como "%Y%m%d%H%M%S.%f"
                        dt_utc  = datetime.strptime(ts_raw, "%Y%m%d%H%M%S.%f")
                        # Ajuste a hora local (Ecuador, -05:00) — si fuese necesario
                        dt_local = dt_utc - timedelta(hours=5)
                        fecha_str = dt_local.date().isoformat()
                        hora_str  = dt_local.strftime("%H:%M:%S")
                        ts_iso_z = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                        writer.writerow([fecha_str, hora_str, person_name, 1, camera_name, search_id, camera_id, ts_iso_z])
                    except Exception:
                        continue

            _last_fetch_hash = new_hash
            print(f"Ingesta: escrito CSV con {len(all_searches)} registros")
            return True

        except requests.exceptions.RequestException as e:
            print(f"Ingesta: error de red/API: {e}")
            return False
        except Exception as e:
            print(f"Ingesta: error inesperado: {e}")
            return False


def background_worker():
    """Hilo que ingesta nuevos registros periódicamente."""
    while not _stop_event.is_set():
        try:
            _fetch_and_write_csv(TOTAL_RECORDS_NEEDED)
        finally:
            _stop_event.wait(FETCH_PERIOD_SECONDS)


# =========================
# Endpoints
# =========================
@app.route("/")
def index():
    _ensure_background_started()
    registros, agg, agg_hora_latest, fechas, personas, personas_total = leer_csv(CSV_FILE)
    estado = construir_estado_dashboard(registros, agg, agg_hora_latest, fechas, personas, personas_total, PERSON_IMG_MAP)
    return render_template_string(construir_html_dashboard_bootstrap(estado))


@app.route("/api/stats")
def api_stats():
    registros, _, _, _, _, _ = leer_csv(CSV_FILE)
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
    _ensure_background_started()  # ver #3
    headers = {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }

    def gen():
        last_mtime = None
        keepalive_every = 15  # segundos
        last_ping = time.monotonic()
        try:
            while not _stop_event.is_set():
                try:
                    current_mtime = os.path.getmtime(CSV_FILE) if CSV_FILE.exists() else None
                except Exception:
                    current_mtime = None

                if current_mtime and current_mtime != last_mtime:
                    last_mtime = current_mtime
                    registros, agg, agg_hora_latest, fechas, personas, personas_total = leer_csv(CSV_FILE)
                    estado = construir_estado_dashboard(registros, agg, agg_hora_latest, fechas, personas, personas_total, PERSON_IMG_MAP)
                    yield f"event: update\ndata: {json.dumps(estado, ensure_ascii=False)}\n\n"

                # keep-alive para que el proxy no cierre el socket
                now = time.monotonic()
                if now - last_ping >= keepalive_every:
                    yield ": keep-alive\n\n"
                    last_ping = now

                time.sleep(STREAM_POLL_MTIME_SECONDS)

        except (GeneratorExit, SystemExit):
            return
        except Exception:
            try:
                yield "event: error\ndata: {\"msg\":\"sse-internal-error\"}\n\n"
            except Exception:
                pass
            return

    return Response(gen(), headers=headers)



# =========================
# Arranque
# =========================
def obtener_imagenes_galeria():
    """Prueba rápida de acceso a galería (logs)."""
    global TOKEN
    if not TOKEN:
        if not obtener_nuevo_token():
            print("Error: No se pudo obtener un token de autenticación.")
            return
    gallery_url = "https://dashboard-api.verifyfaces.com/companies/54/galleries/531"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"perPage": 100, "page": 1}
    try:
        print("Haciendo llamada a la API de la galería...")
        response = requests.get(gallery_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        _ = response.json()
        print("Respuesta OK a la API de la galería")
    except requests.exceptions.HTTPError as e:
        print(f"Error HTTP al obtener imágenes: {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión al obtener imágenes: {e}")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")


if __name__ == "__main__":
    obtener_imagenes_galeria()
    # Lanzar hilo de ingesta
    t = threading.Thread(target=background_worker, daemon=True)
    t.start()
    try:
        app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)
    finally:
        _stop_event.set()