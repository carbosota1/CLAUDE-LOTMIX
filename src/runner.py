"""
runner.py — Orquestador principal del sistema OPV.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

# Asegura que /src esté en el path (funciona local y en GitHub Actions)
sys.path.insert(0, os.path.dirname(__file__))

from io_xlsx import ensure_dir, read_history_xlsx, upsert_history_xlsx, normalize_2d
from analyze import explode, recommend_for_target, should_alert, top_pales
from telegram import send_telegram

# ---------------------------------------------------------------------------
# Constantes de rutas
# ---------------------------------------------------------------------------
TZ = ZoneInfo("America/Santo_Domingo")

DATA_DIR = "data"
HIST_DIR = os.path.join(DATA_DIR, "histories")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
OUT_DIR = "outputs"

XLSX_FILES: dict[str, str] = {
    "La Primera":   os.path.join(HIST_DIR, "La Primera History.xlsx"),
    "Anguilla":     os.path.join(HIST_DIR, "Anguilla history.xlsx"),
    "La Nacional":  os.path.join(HIST_DIR, "La nacional history.xlsx"),
    "La Suerte":    os.path.join(HIST_DIR, "La suerte history.xlsx"),
}

# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------
UPDATE_AFTER = 2  # minutos después del sorteo para intentar scraping

SCHEDULE: list[dict] = [
    # Anguilla
    {"lottery": "Anguilla", "draw": "Anguila 10AM", "time": "10:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "Anguilla", "draw": "Anguila 1PM",  "time": "13:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "Anguilla", "draw": "Anguila 6PM",  "time": "18:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "Anguilla", "draw": "Anguila 9PM",  "time": "21:00", "update_after_minutes": UPDATE_AFTER},
    # La Primera
    {"lottery": "La Primera", "draw": "Quiniela La Primera",       "time": "12:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "La Primera", "draw": "Quiniela La Primera Noche", "time": "19:00", "update_after_minutes": UPDATE_AFTER},
    # La Nacional
    {"lottery": "La Nacional", "draw": "Loteria Nacional- Gana Más", "time": "14:30", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "La Nacional", "draw": "Loteria Nacional- Noche",    "time": "21:00", "update_after_minutes": UPDATE_AFTER},
    # La Suerte
    {"lottery": "La Suerte", "draw": "Quiniela La Suerte",      "time": "12:30", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "La Suerte", "draw": "Quiniela La Suerte 6PM",  "time": "18:00", "update_after_minutes": UPDATE_AFTER},
]

# ---------------------------------------------------------------------------
# Precisión de salida (Telegram)
# ---------------------------------------------------------------------------
TOPK_QUINIELA = 3
TOPK_FULL = 12
PALES_OUT = 3

# ---------------------------------------------------------------------------
# Umbrales base
# ---------------------------------------------------------------------------
MIN_SIGNAL = 0.0075
MIN_A11 = 11

LOOKAHEAD_MINUTES = 5 * 60
UPCOMING_GRACE_SECONDS = 10 * 60

FORCE_NOTIFY: bool = os.getenv("FORCE_NOTIFY", "0").strip() == "1"

# ---------------------------------------------------------------------------
# Ajustes de fuente histórica
# ---------------------------------------------------------------------------
MIN_SOURCE_ROWS = 1500
MAX_SOURCE_ROWS = 2400
RECENT_DAYS_CAP = 180
FIRST_TARGET_RECENT_DAYS = 120
MIN_OBS_FOR_STRICT_NUM_MASK = 5
STRUCTURED_ROWS_MAX = 2000  # ← definida aquí para evitar NameError en runner

# ---------------------------------------------------------------------------
# Pesos y penalizaciones del score
# ---------------------------------------------------------------------------
SIGNAL_WEIGHT = 1.00
A11_WEIGHT = 0.08

TOP12_REPEAT_THRESHOLD = 8

NO_PLAY_OBS_THRESHOLD = 16
STRUCTURED_OBS_THRESHOLD = 13

WEAK_SIGNAL_HARD_BLOCK = 0.015
FAKE_SIGNAL_PENALTY = 0.35
REPEAT_PENALTY = 0.30
HOT_NUM_BOOST = 0.20
INTRADAY_HIT_BOOST = 0.05
POWER_COMBO_BOOST = 0.20
FRESH_NUM_BOOST = 0.05

RECENT_LOG_WINDOW = 80
FREQ_PENALTY_PER_HIT = 0.0012
MAX_RECENT_FREQ = 2


# ===========================================================================
# Helpers de tiempo
# ===========================================================================

def now_rd() -> datetime:
    return datetime.now(TZ)


def today_str() -> str:
    return now_rd().strftime("%Y-%m-%d")


def _norm2(x: str) -> str:
    s = str(x).strip()
    return s.zfill(2) if s.isdigit() else s


def _norm_pair(a: str, b: str) -> str:
    a, b = _norm2(a), _norm2(b)
    aa, bb = sorted([a, b])
    return f"{aa}-{bb}"


def format_pales(pales_raw) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for p in (pales_raw or []):
        try:
            if isinstance(p, (tuple, list)) and len(p) >= 2:
                a, b = str(p[0]).strip(), str(p[1]).strip()
            else:
                s = str(p).strip()
                if "-" not in s:
                    continue
                a, b = s.split("-", 1)
                a, b = a.strip(), b.strip()

            a, b = _norm2(a), _norm2(b)
            if not a or not b or a == b:
                continue

            pair = _norm_pair(a, b)
            if pair in seen:
                continue
            seen.add(pair)
            out.append(pair)
        except Exception:
            continue
    return out


def fingerprint(topq: list, top12: list, pales: list) -> str:
    s = "|".join(topq) + "||" + "|".join(top12) + "||" + "|".join(pales)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def _parse_json_list(value) -> list:
    if isinstance(value, list):
        return value
    if value in (None, "", "nan"):
        return []
    try:
        return json.loads(value)
    except Exception:
        return []


def _recent_pick_frequency() -> Counter:
    log_path = os.path.join(DATA_DIR, "picks_log.csv")
    freq: Counter = Counter()
    if not os.path.exists(log_path):
        return freq
    try:
        df = pd.read_csv(log_path, dtype=str).tail(RECENT_LOG_WINDOW)
        if "top12" not in df.columns:
            return freq
        for raw in df["top12"]:
            for n in _parse_json_list(raw):
                freq[_norm2(n)] += 1
    except Exception:
        return Counter()
    return freq


# ===========================================================================
# State
# ===========================================================================

def _fresh_state() -> dict:
    return {
        "last_updates": {},
        "last_event_key": "",
        "sent_by_target_fp": {},
        "last_wait_key": "",
        "last_top12": [],
    }


def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return _fresh_state()
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
        if not raw:
            raise ValueError("state.json vacío")
        st = json.loads(raw)
        if not isinstance(st, dict):
            raise ValueError("state.json no es un dict")
    except Exception:
        return _fresh_state()

    st.setdefault("last_updates", {})
    st.setdefault("last_event_key", "")
    st.setdefault("sent_by_target_fp", {})
    st.setdefault("last_wait_key", "")
    st.setdefault("last_top12", [])
    return st


def save_state(state: dict) -> None:
    ensure_dir(DATA_DIR)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)  # escritura atómica


# ===========================================================================
# Horarios dinámicos
# ===========================================================================

def item_time(item: dict) -> str:
    """Nacional Noche cambia a 18:00 los domingos."""
    try:
        if (
            item["lottery"] == "La Nacional"
            and item["draw"] == "Loteria Nacional- Noche"
            and now_rd().weekday() == 6
        ):
            return "18:00"
    except Exception:
        pass
    return item["time"]


def draw_datetime_today(item: dict) -> datetime:
    h, m = map(int, item_time(item).split(":"))
    return now_rd().replace(hour=h, minute=m, second=0, microsecond=0)


def _due_dt(item: dict) -> datetime:
    return draw_datetime_today(item) + timedelta(minutes=item["update_after_minutes"])


def _is_due(item: dict, now: datetime) -> bool:
    return now >= _due_dt(item)


# ===========================================================================
# Scraper dinámico
# ===========================================================================

def fetch_result(lottery: str, draw: str, date: str) -> tuple[str, str, str]:
    file_map = {
        "Anguilla":    "anguilla_scraper.py",
        "La Primera":  "laprimera_scraper.py",
        "La Nacional": "lanacional_scraper.py",
        "La Suerte":   "lasuerte_scraper.py",
    }
    if lottery not in file_map:
        raise ValueError(f"Lottery no soportada: {lottery}")

    scrapers_dir = os.path.join(os.path.dirname(__file__), "scrapers")
    file_path = os.path.join(scrapers_dir, file_map[lottery])

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Scraper no encontrado: {file_path}")

    spec = importlib.util.spec_from_file_location(f"{lottery}_scraper", file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"No se pudo cargar el spec del scraper: {file_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore[union-attr]

    if not hasattr(module, "get_result"):
        raise AttributeError(f"El scraper {file_path} no implementa get_result(draw, date)")

    return module.get_result(draw, date)


# ===========================================================================
# XLSX helpers (con caché por sesión)
# ===========================================================================
_xlsx_cache: dict[str, pd.DataFrame] = {}


def _get_history_df(lottery: str) -> pd.DataFrame:
    if lottery not in _xlsx_cache:
        path = XLSX_FILES.get(lottery, "")
        if path and os.path.exists(path):
            try:
                df = pd.read_excel(path, dtype=str)
                df.columns = [str(c).strip().lower() for c in df.columns]
                df["fecha"] = df["fecha"].astype(str).str[:10]
                df["sorteo"] = df["sorteo"].astype(str)
                _xlsx_cache[lottery] = df
            except Exception:
                _xlsx_cache[lottery] = pd.DataFrame()
        else:
            _xlsx_cache[lottery] = pd.DataFrame()
    return _xlsx_cache[lottery]


def _invalidate_cache(lottery: str) -> None:
    _xlsx_cache.pop(lottery, None)


def _has_row_for_date(lottery: str, draw: str, date_str: str) -> bool:
    df = _get_history_df(lottery)
    if df.empty:
        return False
    required = {"fecha", "sorteo", "primero", "segundo", "tercero"}
    if not required.issubset(df.columns):
        return False
    return not df[(df["fecha"] == date_str) & (df["sorteo"] == draw)].empty


def _get_row_for_date(lottery: str, draw: str, date_str: str) -> tuple[str, str, str] | None:
    df = _get_history_df(lottery)
    if df.empty:
        return None
    required = {"fecha", "sorteo", "primero", "segundo", "tercero"}
    if not required.issubset(df.columns):
        return None
    m = df[(df["fecha"] == date_str) & (df["sorteo"] == draw)]
    if m.empty:
        return None
    r = m.iloc[-1]
    return (
        normalize_2d(str(r["primero"])),
        normalize_2d(str(r["segundo"])),
        normalize_2d(str(r["tercero"])),
    )


# ===========================================================================
# Actualización normal (hoy, solo si due)
# ===========================================================================

def try_update_one(item: dict, state: dict) -> bool:
    n = now_rd()
    date_str = today_str()

    if not _is_due(item, n):
        return False

    key = f"{date_str}|{item['lottery']}|{item['draw']}"
    last_updates = state.setdefault("last_updates", {})

    if last_updates.get(key) == "done" and _has_row_for_date(item["lottery"], item["draw"], date_str):
        return False

    p1, p2, p3 = fetch_result(item["lottery"], item["draw"], date_str)
    p1, p2, p3 = normalize_2d(p1), normalize_2d(p2), normalize_2d(p3)

    # Sanity check: mismo resultado que ayer dentro de la ventana de gracia
    yday = (now_rd().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    yres = _get_row_for_date(item["lottery"], item["draw"], yday)
    if yres is not None and (p1, p2, p3) == yres:
        if n < (_due_dt(item) + timedelta(minutes=90)):
            raise RuntimeError(
                f"Resultado idéntico al de ayer para {item['draw']} — aún no publicado. Skipping."
            )

    new_row = pd.DataFrame([{
        "fecha": date_str,
        "sorteo": item["draw"],
        "primero": p1,
        "segundo": p2,
        "tercero": p3,
    }])

    ensure_dir(HIST_DIR)
    upsert_history_xlsx(XLSX_FILES[item["lottery"]], new_row)
    _invalidate_cache(item["lottery"])

    last_updates[key] = "done"
    return True


# ===========================================================================
# Force refresh + backfill (hoy + N días atrás)
# ===========================================================================

def _missing_for_date(date_str: str) -> list[dict]:
    n = now_rd()
    today = today_str()
    return [
        item for item in SCHEDULE
        if not (date_str == today and not _is_due(item, n))
        and not _has_row_for_date(item["lottery"], item["draw"], date_str)
    ]


def _try_update_for_date(item: dict, date_str: str, state: dict) -> bool:
    key = f"{date_str}|{item['lottery']}|{item['draw']}"
    last_updates = state.setdefault("last_updates", {})

    if _has_row_for_date(item["lottery"], item["draw"], date_str):
        last_updates[key] = "done"
        return False

    p1, p2, p3 = fetch_result(item["lottery"], item["draw"], date_str)
    p1, p2, p3 = normalize_2d(p1), normalize_2d(p2), normalize_2d(p3)

    if date_str == today_str():
        yday = (now_rd().date() - timedelta(days=1)).strftime("%Y-%m-%d")
        yres = _get_row_for_date(item["lottery"], item["draw"], yday)
        if yres is not None and (p1, p2, p3) == yres:
            if now_rd() < (_due_dt(item) + timedelta(minutes=90)):
                raise RuntimeError(
                    f"Resultado idéntico al de ayer para {item['draw']} — Skipping."
                )

    new_row = pd.DataFrame([{
        "fecha": date_str,
        "sorteo": item["draw"],
        "primero": p1,
        "segundo": p2,
        "tercero": p3,
    }])

    ensure_dir(HIST_DIR)
    upsert_history_xlsx(XLSX_FILES[item["lottery"]], new_row)
    _invalidate_cache(item["lottery"])

    last_updates[key] = "done"
    return True


def force_refresh_backfill(
    state: dict,
    days_back: int = 1,
    max_attempts: int = 5,
    backoff_seconds: list[int] | None = None,
) -> dict:
    if backoff_seconds is None:
        backoff_seconds = [2, 5, 10, 20, 30]

    base = now_rd().date()
    dates = [(base - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(days_back + 1)]

    for attempt in range(max_attempts):
        any_fixed = False

        for ds in dates:
            for item in _missing_for_date(ds):
                try:
                    if _try_update_for_date(item, ds, state):
                        any_fixed = True
                        print(f"[OK] Backfilled: {ds} | {item['lottery']} {item['draw']}")
                except Exception as e:
                    print(f"[WARN] Backfill skip: {ds} | {item['lottery']} {item['draw']}: {e}")

        if not any_fixed and attempt < max_attempts - 1:
            wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
            print(f"[INFO] FORCE_REFRESH esperando {wait}s antes de reintentar...")
            time.sleep(wait)

    return state


# ===========================================================================
# Gates: verificación de datos antes de analizar
# ===========================================================================

def missing_due_updates_before_target(target_dt: datetime) -> list[str]:
    n = now_rd()
    date_str = today_str()
    return [
        f"{item['lottery']} | {item['draw']} (due {_due_dt(item).strftime('%H:%M')})"
        for item in SCHEDULE
        if draw_datetime_today(item).date() == target_dt.date()
        and draw_datetime_today(item) < target_dt
        and _is_due(item, n)
        and not _has_row_for_date(item["lottery"], item["draw"], date_str)
    ]


def missing_due_updates_global_today() -> list[str]:
    n = now_rd()
    date_str = today_str()
    return [
        f"{item['lottery']} | {item['draw']} (due {_due_dt(item).strftime('%H:%M')})"
        for item in SCHEDULE
        if _is_due(item, n)
        and not _has_row_for_date(item["lottery"], item["draw"], date_str)
    ]


# ===========================================================================
# Próximos targets (mismo slot de hora)
# ===========================================================================

def next_targets_same_time() -> tuple[datetime, list[dict]] | None:
    n = now_rd()
    candidates = [
        (draw_datetime_today(item), item)
        for item in SCHEDULE
        if draw_datetime_today(item).date() == n.date()
        and draw_datetime_today(item) >= (n - timedelta(seconds=UPCOMING_GRACE_SECONDS))
        and (draw_datetime_today(item) - n).total_seconds() <= LOOKAHEAD_MINUTES * 60
    ]

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    dt_min = candidates[0][0]
    same = [it for dt, it in candidates if dt == dt_min]
    return dt_min, same


# ===========================================================================
# Intradía
# ===========================================================================

def observed_nums_today_before(target_dt: datetime) -> set[str]:
    date_str = today_str()
    obs: set[str] = set()
    for it in SCHEDULE:
        dt = draw_datetime_today(it)
        if dt.date() != target_dt.date() or dt >= target_dt:
            continue
        if not _has_row_for_date(it["lottery"], it["draw"], date_str):
            continue
        r = _get_row_for_date(it["lottery"], it["draw"], date_str)
        if r:
            obs.update(r)
    return obs


def intraday_counter_before(target_dt: datetime) -> Counter:
    date_str = today_str()
    counts: Counter = Counter()
    for it in SCHEDULE:
        dt = draw_datetime_today(it)
        if dt.date() != target_dt.date() or dt >= target_dt:
            continue
        if not _has_row_for_date(it["lottery"], it["draw"], date_str):
            continue
        r = _get_row_for_date(it["lottery"], it["draw"], date_str)
        if r:
            counts.update(r)
    return counts


# ===========================================================================
# Picks logging
# ===========================================================================

def log_pick(payload: dict) -> None:
    ensure_dir(DATA_DIR)
    log_path = os.path.join(DATA_DIR, "picks_log.csv")

    generated_at = payload.get("generated_at", "")
    date_str = generated_at[:10] if generated_at else today_str()
    bp = payload.get("best_play", {})

    key = f"{date_str}|{bp.get('lottery','')}" \
          f"|{bp.get('draw','')}" \
          f"|{bp.get('time_rd','')}"

    row = {
        "key": key,
        "date": date_str,
        "time_rd": bp.get("time_rd", ""),
        "lottery": bp.get("lottery", ""),
        "draw": bp.get("draw", ""),
        "generated_at": generated_at,
        "best_signal": bp.get("best_signal"),
        "best_a11": bp.get("best_a11"),
        "ok_alert": bp.get("ok_alert"),
        "decision": bp.get("decision", ""),
        "top12": json.dumps(bp.get("top12", []), ensure_ascii=False),
        "topq": json.dumps(bp.get("topq", []), ensure_ascii=False),
        "pales": json.dumps(bp.get("pales", []), ensure_ascii=False),
        "fingerprint": bp.get("fingerprint", ""),
        "source_rows_hist_used": bp.get("debug", {}).get("source_rows_hist_used"),
        "graded": "0",
    }

    new_df = pd.DataFrame([row])

    if os.path.exists(log_path):
        old = pd.read_csv(log_path, dtype=str)
        combined = (
            pd.concat([old, new_df], ignore_index=True)
            .drop_duplicates(subset=["key"], keep="last")
        )
        combined.to_csv(log_path, index=False, encoding="utf-8")
    else:
        new_df.to_csv(log_path, index=False, encoding="utf-8")


# ===========================================================================
# Grading
# ===========================================================================

def grade_picks_from_histories() -> None:
    log_path = os.path.join(DATA_DIR, "picks_log.csv")
    if not os.path.exists(log_path):
        return

    df = pd.read_csv(log_path, dtype=str)
    if df.empty:
        return

    pending = df[df["graded"].fillna("0") != "1"].copy()
    if pending.empty:
        return

    def _pale_hits(pales: list, drawn: set) -> int:
        drawn_s = sorted(drawn)
        if len(drawn_s) < 3:
            return 0
        real_pairs = {
            f"{drawn_s[0]}-{drawn_s[1]}",
            f"{drawn_s[0]}-{drawn_s[2]}",
            f"{drawn_s[1]}-{drawn_s[2]}",
        }
        norm = set()
        for p in pales:
            try:
                a, b = str(p).split("-", 1)
                a, b = a.strip().zfill(2), b.strip().zfill(2)
                aa, bb = sorted([a, b])
                norm.add(f"{aa}-{bb}")
            except Exception:
                continue
        return len(norm & real_pairs)

    perf_rows = []
    any_graded = False

    for _, r in pending.iterrows():
        date_s = r.get("date", "")
        lottery = r.get("lottery", "")
        draw = r.get("draw", "")
        key = r.get("key", "")

        hx = _get_history_df(lottery)
        if hx.empty:
            continue

        required = {"fecha", "sorteo", "primero", "segundo", "tercero"}
        if not required.issubset(hx.columns):
            continue

        match = hx[(hx["fecha"] == date_s) & (hx["sorteo"] == draw)]
        if match.empty:
            continue

        row_hx = match.iloc[-1]
        for col in ("primero", "segundo", "tercero"):
            hx.loc[match.index, col] = (
                hx.loc[match.index, col]
                .astype(str)
                .str.extract(r"(\d{1,2})")[0]
                .fillna("")
                .str.zfill(2)
            )
        drawn = {
            str(row_hx["primero"]).zfill(2),
            str(row_hx["segundo"]).zfill(2),
            str(row_hx["tercero"]).zfill(2),
        }

        top12 = _parse_json_list(r.get("top12", "[]"))
        pales = _parse_json_list(r.get("pales", "[]"))

        def _safe_float(v):
            try:
                return float(v) if v not in (None, "", "nan") else None
            except Exception:
                return None

        def _safe_int(v):
            try:
                return int(float(v)) if v not in (None, "", "nan") else None
            except Exception:
                return None

        perf_rows.append({
            "key": key,
            "date": date_s,
            "time_rd": r.get("time_rd", ""),
            "lottery": lottery,
            "draw": draw,
            "result": f"{row_hx['primero']}-{row_hx['segundo']}-{row_hx['tercero']}",
            "best_signal": _safe_float(r.get("best_signal")),
            "best_a11": _safe_int(r.get("best_a11")),
            "ok_alert": r.get("ok_alert", ""),
            "decision": r.get("decision", ""),
            "hits_quiniela_topq": len(set(_parse_json_list(r.get("topq", "[]"))) & drawn),
            "hits_quiniela_top12": len(set(top12) & drawn),
            "pale_hits": _pale_hits(pales, drawn),
            "source_rows_hist_used": _safe_float(r.get("source_rows_hist_used")),
        })

        df.loc[df["key"] == key, "graded"] = "1"
        any_graded = True

    if perf_rows:
        ensure_dir(OUT_DIR)
        perf_path = os.path.join(OUT_DIR, "performance.csv")
        perf_df = pd.DataFrame(perf_rows)

        if os.path.exists(perf_path):
            old_p = pd.read_csv(perf_path, dtype=str)
            out_p = (
                pd.concat([old_p, perf_df], ignore_index=True)
                .drop_duplicates(subset=["key"], keep="last")
            )
            out_p.to_csv(perf_path, index=False, encoding="utf-8")
        else:
            perf_df.to_csv(perf_path, index=False, encoding="utf-8")

    if any_graded:
        df.to_csv(log_path, index=False, encoding="utf-8")


# ===========================================================================
# Construcción del historial explosionado
# ===========================================================================

def build_exploded_history() -> pd.DataFrame | None:
    frames = []
    for lottery, path in XLSX_FILES.items():
        df = read_history_xlsx(path)
        if not df.empty:
            frames.append(explode(df, lottery))

    if not frames:
        return None

    exp = (
        pd.concat(frames, ignore_index=True)
        .sort_values("fecha_dt")
        .reset_index(drop=True)
    )
    exp["fecha_dt"] = pd.to_datetime(exp["fecha_dt"], errors="coerce")
    return exp.dropna(subset=["fecha_dt"])


# ===========================================================================
# Análisis + notificación por target
# ===========================================================================

def _build_no_play_payload(
    event_key: str,
    target: dict,
    target_dt: datetime,
    obs_nums: set,
    intraday_counts: Counter,
    prior_pairs: set,
    reason: str,
) -> dict:
    decision = "❌ NO JUGAR"
    payload = {
        "generated_at": now_rd().isoformat(),
        "event_key": event_key,
        "target": {
            "time_rd": target_dt.strftime("%Y-%m-%d %H:%M"),
            "lottery": target["lottery"],
            "draw": target["draw"],
        },
        "best_play": {
            "time_rd": target_dt.strftime("%Y-%m-%d %H:%M"),
            "lottery": target["lottery"],
            "draw": target["draw"],
            "top12": [], "topq": [], "pales": [],
            "fingerprint": "",
            "ok_alert": False,
            "decision": decision,
            "best_signal": None,
            "best_a11": None,
            "debug": {
                "reason": reason,
                "today_observed_nums": len(obs_nums),
                "source_pairs_today": len(prior_pairs),
                "source_rows_hist_used": 0,
                "top12_overlap_prev": 0,
                "top_intraday": intraday_counts.most_common(5),
            },
        },
    }
    return payload


def analyze_target_and_maybe_notify(
    exp: pd.DataFrame,
    event_key: str,
    target_dt: datetime,
    target_item: dict,
    state: dict,
) -> dict | None:
    target = target_item
    print(f"[INFO] Target: {target_dt:%Y-%m-%d %H:%M} {target['lottery']} {target['draw']}")

    # Gate: datos previos obligatorios
    missing = missing_due_updates_before_target(target_dt)
    if missing and not FORCE_NOTIFY:
        print("[INFO] Faltan updates previos — skip picks.")
        for m in missing:
            print(f"[INFO]  Missing: {m}")
        return None

    prior_pairs = {
        (it["lottery"], it["draw"])
        for it in SCHEDULE
        if draw_datetime_today(it).date() == target_dt.date()
        and draw_datetime_today(it) < target_dt
    }

    obs_nums = observed_nums_today_before(target_dt)
    intraday_counts = intraday_counter_before(target_dt)

    # Día caótico → no jugar
    if len(obs_nums) >= NO_PLAY_OBS_THRESHOLD and not FORCE_NOTIFY:
        print(f"[INFO] NO PLAY: día caótico obs_nums={len(obs_nums)}")
        payload = _build_no_play_payload(
            event_key, target, target_dt, obs_nums, intraday_counts, prior_pairs,
            reason="día caótico",
        )
        log_pick(payload)
        _send_pick_telegram(event_key, target, target_dt, payload["best_play"], obs_nums, used_rows=0)
        return payload

    # -----------------------------------------------------------------------
    # Filtro histórico
    # -----------------------------------------------------------------------
    target_dt_naive = target_dt.replace(tzinfo=None)

    if not prior_pairs:
        cutoff = target_dt_naive - timedelta(days=FIRST_TARGET_RECENT_DAYS)
        recent_mask = exp["fecha_dt"] >= cutoff

        def src_filter_first(e, _rm=recent_mask):
            return _rm & ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))

        rec_hist = recommend_for_target(
            exp, src_filter_first, target["lottery"], target["draw"],
            lag_days=0, top_n=TOPK_FULL,
        )
        used_rows = int(recent_mask.sum())
        used_pairs = 0
    else:
        base_mask = exp.apply(
            lambda r: (r.get("lottery"), r.get("sorteo")) in prior_pairs, axis=1
        )

        if obs_nums and len(obs_nums) >= MIN_OBS_FOR_STRICT_NUM_MASK:
            mask = base_mask & exp["num"].astype(str).isin(obs_nums)
        else:
            mask = base_mask

        used_pairs = len(prior_pairs)

        # Recortar si excede MAX_SOURCE_ROWS
        if mask.sum() > MAX_SOURCE_ROWS:
            cutoff = target_dt_naive - timedelta(days=RECENT_DAYS_CAP)
            mask = mask & (exp["fecha_dt"] >= cutoff)

        if mask.sum() > MAX_SOURCE_ROWS:
            tail_idx = exp[mask].sort_values("fecha_dt").tail(MAX_SOURCE_ROWS).index
            mask = exp.index.isin(tail_idx)

        mask_idx = set(exp[mask].index)
        used_rows = len(mask_idx)

        # Fallback si muy pocos datos
        if used_rows < MIN_SOURCE_ROWS:
            mask2 = base_mask
            if mask2.sum() > MAX_SOURCE_ROWS:
                cutoff = target_dt_naive - timedelta(days=RECENT_DAYS_CAP)
                mask2 = mask2 & (exp["fecha_dt"] >= cutoff)
            if mask2.sum() > MAX_SOURCE_ROWS:
                tail_idx2 = exp[mask2].sort_values("fecha_dt").tail(MAX_SOURCE_ROWS).index
                mask2 = exp.index.isin(tail_idx2)
            mask_idx = set(exp[mask2].index)
            used_rows = len(mask_idx)

        if not mask_idx:
            cutoff = target_dt_naive - timedelta(days=RECENT_DAYS_CAP)
            recent_mask = exp["fecha_dt"] >= cutoff

            def src_filter(e, _rm=recent_mask):
                return _rm & ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))

            rec_hist = recommend_for_target(
                exp, src_filter, target["lottery"], target["draw"],
                lag_days=0, top_n=TOPK_FULL,
            )
            used_rows = int(recent_mask.sum())
        else:
            rec_hist = recommend_for_target(
                exp,
                lambda e, _m=mask_idx: e.index.isin(_m),
                target["lottery"],
                target["draw"],
                lag_days=0,
                top_n=TOPK_FULL,
            )

    if rec_hist is None or rec_hist.empty:
        print("[INFO] Datos insuficientes para calcular recomendaciones.")
        return None

    # -----------------------------------------------------------------------
    # Score híbrido
    # -----------------------------------------------------------------------
    rec = rec_hist.copy()
    rec["num"] = rec["num"].astype(str).map(_norm2)
    rec["signal"] = pd.to_numeric(rec.get("signal", 0), errors="coerce").fillna(0.0)
    rec["a11"] = pd.to_numeric(rec.get("a11", 0), errors="coerce").fillna(0).astype(int)

    rec["score"] = rec["signal"] * SIGNAL_WEIGHT + rec["a11"] * A11_WEIGHT
    rec["score"] += rec["num"].isin(obs_nums).astype(int) * HOT_NUM_BOOST
    rec["score"] += rec["num"].map(lambda x: intraday_counts.get(x, 0)) * INTRADAY_HIT_BOOST
    rec["score"] -= ((rec["signal"] > 0.02) & (rec["a11"] <= 3)).astype(int) * FAKE_SIGNAL_PENALTY

    last_top12 = {_norm2(x) for x in state.get("last_top12", [])}
    rec["score"] -= rec["num"].isin(last_top12).astype(int) * REPEAT_PENALTY

    recent_freq = _recent_pick_frequency()
    rec_freq_vals = rec["num"].map(lambda x: recent_freq.get(x, 0))
    rec["score"] -= rec_freq_vals * FREQ_PENALTY_PER_HIT
    rec["score"] -= (rec_freq_vals >= MAX_RECENT_FREQ).astype(int) * 0.15
    rec["score"] += ((rec["signal"] >= 0.015) & (rec["a11"] >= 5)).astype(int) * POWER_COMBO_BOOST
    rec["score"] += (rec_freq_vals == 0).astype(int) * FRESH_NUM_BOOST

    rec = rec.sort_values(["score", "signal", "a11"], ascending=False)
    top12 = rec["num"].tolist()[:TOPK_FULL]

    overlap = len(set(top12) & last_top12)
    if overlap >= TOP12_REPEAT_THRESHOLD:
        print(f"[INFO] Anti-repeat triggered overlap={overlap}")
        rec = rec.sort_values(["a11", "signal", "score"], ascending=False)
        top12 = rec["num"].tolist()[:TOPK_FULL]
        overlap = len(set(top12) & last_top12)

    topq = top12[:TOPK_QUINIELA]
    pales = format_pales(top_pales(top12[:10], 40))[:PALES_OUT]

    best_signal = float(rec["signal"].max()) if not rec.empty else None
    best_a11 = int(rec["a11"].max()) if not rec.empty else None

    # -----------------------------------------------------------------------
    # Umbrales por lotería
    # -----------------------------------------------------------------------
    lottery_name = target["lottery"]
    draw_name = target["draw"]

    thresholds = {
        "La Nacional": (0.007, 6),
        "Anguilla":    (0.018, 7),
        "La Primera":  (0.020, 3),
        "La Suerte":   (MIN_SIGNAL, 8),
    }
    min_signal, min_a11 = thresholds.get(lottery_name, (MIN_SIGNAL, MIN_A11))

    # -----------------------------------------------------------------------
    # Decision engine
    # -----------------------------------------------------------------------
    decision: str
    n_obs = len(obs_nums)
    bs = best_signal or 0.0
    ba = best_a11 or 0

    if n_obs >= NO_PLAY_OBS_THRESHOLD:
        decision = "❌ NO JUGAR"
    elif draw_name == "Loteria Nacional- Gana Más" and ba < 5:
        decision = "❌ NO JUGAR"
    elif ba <= 3:
        decision = "❌ NO JUGAR"
    elif n_obs <= STRUCTURED_OBS_THRESHOLD and bs < WEAK_SIGNAL_HARD_BLOCK:
        decision = "❌ NO JUGAR"
    elif bs >= 0.025 and ba >= 6 and n_obs <= 12 and used_rows <= STRUCTURED_ROWS_MAX:
        decision = "🔥 JUGAR AGRESIVO"
    elif bs >= 0.018 and ba >= 4 and n_obs <= 13:
        decision = "⚠️ JUGAR"
    else:
        decision = "❌ NO JUGAR"

    ok = should_alert(rec_hist, min_signal=min_signal, min_count_hits=min_a11)
    fp = fingerprint(topq, top12, pales)

    payload = {
        "generated_at": now_rd().isoformat(),
        "event_key": event_key,
        "target": {
            "time_rd": target_dt.strftime("%Y-%m-%d %H:%M"),
            "lottery": target["lottery"],
            "draw": target["draw"],
        },
        "best_play": {
            "time_rd": target_dt.strftime("%Y-%m-%d %H:%M"),
            "lottery": target["lottery"],
            "draw": target["draw"],
            "top12": top12,
            "topq": topq,
            "pales": pales,
            "fingerprint": fp,
            "ok_alert": bool(ok),
            "decision": decision,
            "best_signal": best_signal,
            "best_a11": best_a11,
            "debug": {
                "today_observed_nums": n_obs,
                "source_pairs_today": int(used_pairs),
                "source_rows_hist_used": int(used_rows),
                "top12_overlap_prev": overlap,
                "top_intraday": intraday_counts.most_common(5),
            },
        },
    }

    log_pick(payload)
    _send_pick_telegram(event_key, target, target_dt, payload["best_play"], obs_nums, used_rows, state)

    state["last_top12"] = top12
    return payload


def _send_pick_telegram(
    event_key: str,
    target: dict,
    target_dt: datetime,
    bp: dict,
    obs_nums: set,
    used_rows: int,
    state: dict | None = None,
) -> None:
    """Construye y envía el mensaje de Telegram, deduplicando por fingerprint."""
    decision = bp.get("decision", "")
    topq = bp.get("topq", [])
    top12 = bp.get("top12", [])
    pales = bp.get("pales", [])
    fp = bp.get("fingerprint", "")
    best_signal = bp.get("best_signal")
    best_a11 = bp.get("best_a11")
    ok = bp.get("ok_alert")

    lines = [
        "🚨 OPV (Cross-Match SECUENCIAL / MI + Chi² HISTÓRICO)",
        f"🧩 Señal nueva: {event_key}",
        f"🎯 Target: {target['lottery']} | {target['draw']}",
        f"⏰ Hora: {target_dt.strftime('%H:%M')} RD",
        "",
        decision,
        "",
    ]
    if topq:
        lines += [f"✅ QUINIELA Top{len(topq)}:", ", ".join(topq), ""]
    if top12:
        lines += ["📌 Top12:", ", ".join(top12), ""]
    if pales:
        lines += [f"🎲 PALE Top{len(pales)}:", " | ".join(pales), ""]

    lines += [
        "📊 Debug:",
        f"best_signal={best_signal} best_a11={best_a11} ok_alert={ok}",
        f"today_observed_nums={len(obs_nums)} source_rows_hist_used={used_rows}",
    ]

    target_key = f"{bp['time_rd']}|{target['lottery']}|{target['draw']}"
    sent_map = state.get("sent_by_target_fp", {}) if state else {}

    if sent_map.get(target_key, "") != fp or FORCE_NOTIFY:
        send_telegram("\n".join(lines))
        print("[OK] Telegram enviado para target.")
        if state is not None:
            sent_map[target_key] = fp
            state["sent_by_target_fp"] = sent_map
    else:
        print("[INFO] Mismo fingerprint — Telegram ya enviado para este target.")


# ===========================================================================
# MAIN
# ===========================================================================

def main() -> None:
    for d in (DATA_DIR, HIST_DIR, OUT_DIR):
        ensure_dir(d)

    state = load_state()
    updated_today: list[dict] = []

    # 1) Actualización normal (hoy, solo si due)
    for item in SCHEDULE:
        try:
            if try_update_one(item, state):
                print(f"[OK] Updated: {item['lottery']} {item['draw']}")
                updated_today.append(item)
        except Exception as e:
            print(f"[WARN] update failed {item['lottery']}|{item['draw']}: {e}")

    # 2) Force refresh + backfill (hoy + ayer)
    try:
        state = force_refresh_backfill(state, days_back=1, max_attempts=5)
    except Exception as e:
        print(f"[WARN] force_refresh_backfill failed: {e}")

    # 3) Grading
    try:
        grade_picks_from_histories()
        print("[OK] Grading completado.")
    except Exception as e:
        print(f"[WARN] grading failed: {e}")

    # 4) Gate: faltan updates due de hoy
    missing_due_today = missing_due_updates_global_today()
    if missing_due_today and not FORCE_NOTIFY:
        print("[INFO] Faltan updates due hoy — skip análisis.")
        for m in missing_due_today:
            print(f"[INFO]  Missing: {m}")

        wait_key = f"{today_str()}|WAIT|{len(missing_due_today)}"
        if state.get("last_wait_key") != wait_key:
            try:
                lines = [
                    "⏳ OPV (Esperando resultados)",
                    "No se generarán picks hasta actualizar TODOS los sorteos debidos.",
                    "",
                    "Faltan:",
                    *[f"• {x}" for x in missing_due_today[:20]],
                ]
                send_telegram("\n".join(lines))
                state["last_wait_key"] = wait_key
            except Exception as e:
                print(f"[WARN] Telegram wait message failed: {e}")

        save_state(state)
        print("[OK] runner finished")
        return

    # 5) Sin updates nuevos y sin test → no spam
    if not updated_today and not FORCE_NOTIFY:
        print("[INFO] Sin nuevos updates hoy — skip análisis.")
        save_state(state)
        print("[OK] runner finished")
        return

    # 6) Event key
    if FORCE_NOTIFY and not updated_today:
        event_key = f"{today_str()}|TEST|NO-UPDATE"
    else:
        last_event = sorted(updated_today, key=draw_datetime_today)[-1]
        event_key = f"{today_str()}|{last_event['lottery']}|{last_event['draw']}"

    if state.get("last_event_key") == event_key and not FORCE_NOTIFY:
        print("[INFO] Evento ya procesado — skip.")
        save_state(state)
        print("[OK] runner finished")
        return

    # 7) Historia + targets
    exp = build_exploded_history()
    if exp is None:
        print("[INFO] Sin historial cargado — exit.")
        save_state(state)
        print("[OK] runner finished")
        return

    nxt = next_targets_same_time()
    if not nxt:
        print("[INFO] Sin targets próximos.")
        state["last_event_key"] = event_key
        save_state(state)
        print("[OK] runner finished")
        return

    dt_min, targets = nxt
    print(f"[INFO] Slot: {dt_min:%H:%M} targets={len(targets)}")

    picks_all = []
    for t in targets:
        try:
            payload = analyze_target_and_maybe_notify(exp, event_key, dt_min, t, state)
            if payload:
                picks_all.append(payload)
        except Exception as e:
            print(f"[WARN] target analysis failed {t['lottery']}|{t['draw']}: {e}")

    # 8) Outputs
    ensure_dir(OUT_DIR)
    if picks_all:
        with open(os.path.join(OUT_DIR, "picks_all.json"), "w", encoding="utf-8") as f:
            json.dump(
                {"generated_at": now_rd().isoformat(), "event_key": event_key, "items": picks_all},
                f, ensure_ascii=False, indent=2,
            )
        with open(os.path.join(OUT_DIR, "picks.json"), "w", encoding="utf-8") as f:
            json.dump(picks_all[-1], f, ensure_ascii=False, indent=2)
        print("[OK] Escritos outputs/picks_all.json y outputs/picks.json")
    else:
        print("[INFO] Sin payloads producidos.")

    state["last_event_key"] = event_key
    save_state(state)
    print("[OK] runner finished")


if __name__ == "__main__":
    main()
