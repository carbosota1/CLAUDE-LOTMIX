"""
grading.py — Logging de picks y evaluación de resultados.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Dict, List

import pandas as pd

_LOG_COLS = [
    "key", "date", "time_rd", "lottery", "draw", "generated_at",
    "best_score", "best_signal", "best_a11", "ok_alert",
    "top12", "pales10", "graded",
]


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _mk_key(date_str: str, lottery: str, draw: str, time_rd: str) -> str:
    return f"{date_str}|{lottery}|{draw}|{time_rd}"


def _parse_json_list(raw) -> list:
    if isinstance(raw, list):
        return raw
    try:
        return json.loads(raw or "[]")
    except Exception:
        return []


def _norm_pale(p: str) -> str | None:
    """Normaliza un palé 'A-B' a 'AA-BB' con orden canónico."""
    try:
        a, b = str(p).split("-", 1)
        a, b = a.strip().zfill(2), b.strip().zfill(2)
        aa, bb = sorted([a, b])
        return f"{aa}-{bb}"
    except Exception:
        return None


def _hits_topk(nums: List[str], drawn: set, k: int) -> int:
    return len(set(nums[:k]).intersection(drawn))


def _pale_hits(pales: List[str], drawn: set) -> int:
    drawn_sorted = sorted(drawn)
    if len(drawn_sorted) < 3:
        return 0
    real_pairs = {
        f"{drawn_sorted[0]}-{drawn_sorted[1]}",
        f"{drawn_sorted[0]}-{drawn_sorted[2]}",
        f"{drawn_sorted[1]}-{drawn_sorted[2]}",
    }
    norm = {_norm_pale(p) for p in pales if _norm_pale(p)}
    return len(norm.intersection(real_pairs))


# ---------------------------------------------------------------------------
# Logging de picks
# ---------------------------------------------------------------------------

def log_candidates(outputs_dir: str, payload: dict) -> None:
    """
    Persiste todos los candidatos de un payload en data/picks_log.csv.
    Hace upsert por clave (date|lottery|draw|time_rd).
    """
    data_dir = "data"
    _ensure_dir(data_dir)
    log_path = os.path.join(data_dir, "picks_log.csv")

    generated_at = payload.get("generated_at", "")
    date_str = generated_at[:10] if generated_at else datetime.now().strftime("%Y-%m-%d")

    new_rows = []
    for c in payload.get("candidates_ranked", []):
        time_rd = c.get("time_rd", "")
        lottery = c.get("lottery", "")
        draw = c.get("draw", "")
        key = _mk_key(date_str, lottery, draw, time_rd)

        new_rows.append({
            "key": key,
            "date": date_str,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "generated_at": generated_at,
            "best_score": c.get("best_score"),
            "best_signal": c.get("best_signal"),
            "best_a11": c.get("best_a11"),
            "ok_alert": c.get("ok_alert"),
            "top12": json.dumps(c.get("top_nums", []), ensure_ascii=False),
            "pales10": json.dumps(c.get("pales", []), ensure_ascii=False),
            "graded": "0",
        })

    if not new_rows:
        return

    new_df = pd.DataFrame(new_rows)

    if os.path.exists(log_path):
        old = pd.read_csv(log_path, dtype=str)
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["key"], keep="last")
        combined.to_csv(log_path, index=False, encoding="utf-8")
    else:
        new_df.to_csv(log_path, index=False, encoding="utf-8")


# ---------------------------------------------------------------------------
# Grading
# ---------------------------------------------------------------------------

def grade_picks_from_histories(outputs_dir: str, xlsx_files: Dict[str, str]) -> None:
    """
    Evalúa los picks pendientes contra los resultados reales del historial.
    Escribe hits en outputs/performance.csv y marca graded=1 en picks_log.csv.
    """
    log_path = os.path.join("data", "picks_log.csv")
    if not os.path.exists(log_path):
        return

    df = pd.read_csv(log_path, dtype=str)
    if df.empty:
        return

    pending = df[df["graded"].fillna("0") != "1"].copy()
    if pending.empty:
        return

    # Cache de historiales
    hist_cache: Dict[str, pd.DataFrame] = {}

    def _load_hist(lottery: str) -> pd.DataFrame:
        if lottery in hist_cache:
            return hist_cache[lottery]

        path = xlsx_files.get(lottery)
        if not path or not os.path.exists(path):
            hist_cache[lottery] = pd.DataFrame()
            return hist_cache[lottery]

        try:
            hx = pd.read_excel(path, dtype=str)
        except Exception:
            hist_cache[lottery] = pd.DataFrame()
            return hist_cache[lottery]

        hx.columns = [str(c).strip().lower() for c in hx.columns]
        required = {"fecha", "sorteo", "primero", "segundo", "tercero"}
        if not required.issubset(hx.columns):
            hist_cache[lottery] = pd.DataFrame()
            return hist_cache[lottery]

        hx["fecha"] = hx["fecha"].astype(str).str[:10]
        hx["sorteo"] = hx["sorteo"].astype(str)
        for col in ("primero", "segundo", "tercero"):
            hx[col] = (
                hx[col].astype(str)
                .str.extract(r"(\d{1,2})")[0]
                .fillna("")
                .str.zfill(2)
            )

        hist_cache[lottery] = hx
        return hx

    perf_rows = []
    any_graded = False

    for _, r in pending.iterrows():
        date_s = r.get("date", "")
        lottery = r.get("lottery", "")
        draw = r.get("draw", "")
        time_rd = r.get("time_rd", "")
        key = r.get("key", "")

        hx = _load_hist(lottery)
        if hx.empty:
            continue

        match = hx[(hx["fecha"] == date_s) & (hx["sorteo"] == draw)]
        if match.empty:
            continue  # resultado aún no disponible

        row = match.iloc[-1]
        drawn = {row["primero"], row["segundo"], row["tercero"]}

        top12 = _parse_json_list(r.get("top12", "[]"))
        pales10 = _parse_json_list(r.get("pales10", "[]"))

        perf_rows.append({
            "key": key,
            "date": date_s,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "result": f"{row['primero']}-{row['segundo']}-{row['tercero']}",
            "hits_top6": _hits_topk(top12, drawn, 6),
            "hits_top8": _hits_topk(top12, drawn, 8),
            "hits_top12": _hits_topk(top12, drawn, 12),
            "pale_hits_top10": _pale_hits(pales10, drawn),
            "best_signal": r.get("best_signal"),
            "best_a11": r.get("best_a11"),
            "ok_alert": r.get("ok_alert"),
        })

        df.loc[df["key"] == key, "graded"] = "1"
        any_graded = True

    if perf_rows:
        _ensure_dir(outputs_dir)
        perf_path = os.path.join(outputs_dir, "performance.csv")
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
