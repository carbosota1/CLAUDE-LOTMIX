"""
analyze.py — Estadísticas de correlación mejoradas.

Mejoras sobre la versión original:
1. Ventanas temporales dinámicas (30 / 90 / 365 días)
2. MI condicional por día de semana y franja horaria
3. Markov de orden 2 (secuencias de 2 sorteos anteriores)
4. Ensemble ponderado de los 3 scores
"""
from __future__ import annotations

import itertools
from typing import Callable, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import chi2_contingency
from sklearn.metrics import mutual_info_score

NUMBER_RANGE = range(0, 100)
_ALL_NUMS = [str(n).zfill(2) for n in NUMBER_RANGE]

# ---------------------------------------------------------------------------
# Pesos del ensemble final
# ---------------------------------------------------------------------------
W_WINDOW  = 0.40   # ventanas temporales
W_COND_MI = 0.30   # MI condicional
W_MARKOV  = 0.30   # Markov orden 2

# Pesos internos de las 3 ventanas
W_30D  = 0.50
W_90D  = 0.30
W_365D = 0.20


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def z2(n: int | str) -> str:
    return str(n).zfill(2)


def _time_bucket(dt) -> str:
    """Franja horaria: morning / afternoon / night."""
    try:
        h = pd.Timestamp(dt).hour
        if h < 14:
            return "morning"
        elif h < 18:
            return "afternoon"
        else:
            return "night"
    except Exception:
        return "unknown"


def _weekday(dt) -> int:
    try:
        return pd.Timestamp(dt).weekday()
    except Exception:
        return -1


# ---------------------------------------------------------------------------
# Explosión del historial
# ---------------------------------------------------------------------------

def explode(df: pd.DataFrame, lottery: str) -> pd.DataFrame:
    """Convierte cada fila (fecha, sorteo, p1, p2, p3) en 3 filas 'num'."""
    x = df.copy()
    x["lottery"] = lottery
    x["fecha_dt"] = pd.to_datetime(x["fecha"], errors="coerce")
    x = x.dropna(subset=["fecha_dt"])
    x["nums"] = x[["primero", "segundo", "tercero"]].values.tolist()
    x = x.explode("nums").rename(columns={"nums": "num"})
    x["num"] = x["num"].astype(str).str.strip().str.zfill(2)
    x["weekday"] = x["fecha_dt"].map(_weekday)
    x["time_bucket"] = x["fecha_dt"].map(_time_bucket)
    return x[["fecha_dt", "fecha", "lottery", "sorteo", "num", "weekday", "time_bucket"]]


# ---------------------------------------------------------------------------
# Pivot vectorizado
# ---------------------------------------------------------------------------

def _pivot(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot: fecha_dt → columnas de nums (0/1)."""
    tmp = df[["fecha_dt", "num"]].copy()
    tmp["present"] = 1
    return (
        tmp.groupby(["fecha_dt", "num"])["present"]
        .max()
        .unstack(fill_value=0)
        .reindex(columns=_ALL_NUMS, fill_value=0)
    )


def _chi2_mi_score(src_p: pd.DataFrame, tgt_p: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Chi² + MI entre src y tgt pivotados.
    Retorna DataFrame: num, chi2, p_value, mi, a11, signal
    """
    common = src_p.index.intersection(tgt_p.index)
    if common.empty:
        return pd.DataFrame(columns=["num", "chi2", "p_value", "mi", "a11", "signal"])

    s = src_p.loc[common]
    t = tgt_p.loc[common]

    out = []
    for num in _ALL_NUMS:
        sv = s[num].values if num in s.columns else np.zeros(len(common), dtype=int)
        tv = t[num].values if num in t.columns else np.zeros(len(common), dtype=int)

        a = int(((sv == 1) & (tv == 1)).sum())
        b = int(((sv == 1) & (tv == 0)).sum())
        c = int(((sv == 0) & (tv == 1)).sum())
        d = int(((sv == 0) & (tv == 0)).sum())

        try:
            chi2, p, _, _ = chi2_contingency([[a, b], [c, d]], correction=False)
        except Exception:
            chi2, p = 0.0, 1.0

        mi = mutual_info_score(sv, tv)
        out.append({"num": num, "chi2": float(chi2), "p_value": float(p),
                    "mi": float(mi), "a11": a})

    df = pd.DataFrame(out)
    df["signal"] = df["mi"] * (1.0 - df["p_value"].clip(0, 1))
    return df


# ---------------------------------------------------------------------------
# Compatibilidad con runner (build_pairs / stats_per_num)
# ---------------------------------------------------------------------------

def build_pairs(
    exp: pd.DataFrame,
    src_filter: Callable,
    tgt_filter: Callable,
    lag_days: int,
) -> Optional[pd.DataFrame]:
    src = exp[src_filter(exp)][["fecha_dt", "num"]].copy()
    tgt = exp[tgt_filter(exp)][["fecha_dt", "num"]].copy()

    if src.empty or tgt.empty:
        return None

    src_p = _pivot(src)
    tgt_p = _pivot(tgt)

    if lag_days:
        tgt_p.index = tgt_p.index - pd.Timedelta(days=lag_days)

    common = src_p.index.intersection(tgt_p.index)
    if common.empty:
        return None

    rows = []
    for num in _ALL_NUMS:
        sv = src_p.loc[common, num].values if num in src_p.columns else np.zeros(len(common), int)
        tv = tgt_p.loc[common, num].values if num in tgt_p.columns else np.zeros(len(common), int)
        rows.append(pd.DataFrame({"num": num, "src_event": sv, "tgt_event": tv}))

    return pd.concat(rows, ignore_index=True)


def stats_per_num(pairs: pd.DataFrame) -> pd.DataFrame:
    out = []
    for num, sub in pairs.groupby("num"):
        s = sub["src_event"].values
        t = sub["tgt_event"].values
        a = int(((s == 1) & (t == 1)).sum())
        b = int(((s == 1) & (t == 0)).sum())
        c = int(((s == 0) & (t == 1)).sum())
        d = int(((s == 0) & (t == 0)).sum())
        try:
            chi2, p, _, _ = chi2_contingency([[a, b], [c, d]], correction=False)
        except Exception:
            chi2, p = 0.0, 1.0
        mi = mutual_info_score(s, t)
        out.append({"num": num, "chi2": float(chi2), "p_value": float(p),
                    "mi": float(mi), "a11": a})
    df = pd.DataFrame(out)
    df["signal"] = df["mi"] * (1.0 - df["p_value"].clip(0, 1))
    return df


# ---------------------------------------------------------------------------
# OPCIÓN 1 — Ventanas temporales dinámicas
# ---------------------------------------------------------------------------

def _score_window(
    src_p: pd.DataFrame,
    tgt_p: pd.DataFrame,
    ref_date: pd.Timestamp,
) -> pd.DataFrame:
    """
    Calcula score Chi²+MI en 3 ventanas (30d, 90d, 365d) y las combina.
    Los números consistentes en todas las ventanas tienen señal más confiable.
    """
    windows = {
        "30d":  (ref_date - pd.Timedelta(days=30),  W_30D),
        "90d":  (ref_date - pd.Timedelta(days=90),  W_90D),
        "365d": (ref_date - pd.Timedelta(days=365), W_365D),
    }

    combined = pd.DataFrame({"num": _ALL_NUMS}).set_index("num")
    combined["score_window"] = 0.0

    for label, (cutoff, weight) in windows.items():
        src_w = src_p[src_p.index >= cutoff]
        tgt_w = tgt_p[tgt_p.index >= cutoff]
        st = _chi2_mi_score(src_w, tgt_w)
        if st.empty:
            continue
        st = st.set_index("num")
        combined["score_window"] = combined["score_window"].add(
            st["signal"].reindex(combined.index, fill_value=0.0) * weight,
            fill_value=0.0,
        )

    # Stats completas desde ventana 365d para compatibilidad con runner
    cutoff_full = ref_date - pd.Timedelta(days=365)
    st_full = _chi2_mi_score(
        src_p[src_p.index >= cutoff_full],
        tgt_p[tgt_p.index >= cutoff_full],
    )
    if not st_full.empty:
        st_full = st_full.set_index("num")
        combined["a11"]     = st_full["a11"].reindex(combined.index, fill_value=0).astype(int)
        combined["signal"]  = st_full["signal"].reindex(combined.index, fill_value=0.0)
        combined["mi"]      = st_full["mi"].reindex(combined.index, fill_value=0.0)
        combined["p_value"] = st_full["p_value"].reindex(combined.index, fill_value=1.0)
    else:
        combined["a11"]     = 0
        combined["signal"]  = 0.0
        combined["mi"]      = 0.0
        combined["p_value"] = 1.0

    return combined.reset_index()


# ---------------------------------------------------------------------------
# OPCIÓN 2 — MI condicional por día de semana y franja horaria
# ---------------------------------------------------------------------------

def _score_conditional_mi(
    src: pd.DataFrame,
    tgt: pd.DataFrame,
    target_weekday: int,
    target_bucket: str,
) -> pd.DataFrame:
    """
    Calcula MI filtrando solo fechas con el mismo día de semana
    y franja horaria del sorteo target.
    Fallback progresivo si no hay suficiente data.
    """
    def _try(s, t):
        if s.empty or t.empty or len(s) < 10 or len(t) < 10:
            return None
        sp = _pivot(s[["fecha_dt", "num"]])
        tp = _pivot(t[["fecha_dt", "num"]])
        st = _chi2_mi_score(sp, tp)
        return st if not st.empty else None

    # Intento 1: día + franja
    src_ctx = src[(src["weekday"] == target_weekday) & (src["time_bucket"] == target_bucket)]
    tgt_ctx = tgt[(tgt["weekday"] == target_weekday) & (tgt["time_bucket"] == target_bucket)]
    st = _try(src_ctx, tgt_ctx)

    # Intento 2: solo día
    if st is None:
        src_ctx = src[src["weekday"] == target_weekday]
        tgt_ctx = tgt[tgt["weekday"] == target_weekday]
        st = _try(src_ctx, tgt_ctx)

    # Intento 3: data completa
    if st is None:
        st = _try(src, tgt)

    if st is None:
        return pd.DataFrame({"num": _ALL_NUMS, "score_cond_mi": 0.0})

    st["score_cond_mi"] = st["signal"]
    return st[["num", "score_cond_mi"]]


# ---------------------------------------------------------------------------
# OPCIÓN 3 — Markov de orden 2
# ---------------------------------------------------------------------------

def _build_markov2_score(
    exp: pd.DataFrame,
    tgt_lottery: str,
    tgt_draw: str,
    last_two_draws: list[set[str]],
) -> pd.DataFrame:
    """
    Markov orden 2: dado que en los últimos 2 sorteos salieron
    ciertos números, ¿qué números tienden a salir después en el target?
    """
    score_mk = {n: 0.0 for n in _ALL_NUMS}

    if not last_two_draws:
        return pd.DataFrame({"num": _ALL_NUMS, "score_markov": 0.0})

    tgt_hist = (
        exp[(exp["lottery"] == tgt_lottery) & (exp["sorteo"] == tgt_draw)]
        .sort_values("fecha_dt")
        .copy()
    )

    if tgt_hist.empty:
        return pd.DataFrame({"num": _ALL_NUMS, "score_markov": 0.0})

    by_date = (
        tgt_hist.groupby("fecha_dt")["num"]
        .apply(set)
        .reset_index()
        .sort_values("fecha_dt")
        .reset_index(drop=True)
    )

    if len(by_date) < 3:
        return pd.DataFrame({"num": _ALL_NUMS, "score_markov": 0.0})

    hits: dict[str, float] = {n: 0.0 for n in _ALL_NUMS}
    total = 0.0

    for i in range(len(by_date) - 2):
        prev2 = by_date.iloc[i]["num"]
        prev1 = by_date.iloc[i + 1]["num"]
        next_draw = by_date.iloc[i + 2]["num"]

        overlap = 0
        if len(last_two_draws) >= 2:
            overlap = len(prev2 & last_two_draws[0]) + len(prev1 & last_two_draws[1])
        elif len(last_two_draws) == 1:
            overlap = len(prev1 & last_two_draws[0])

        if overlap >= 1:
            weight = float(overlap)
            for n in next_draw:
                if n in hits:
                    hits[n] += weight
            total += weight

    if total > 0:
        for n in _ALL_NUMS:
            score_mk[n] = hits.get(n, 0.0) / total

    return pd.DataFrame({
        "num": list(score_mk.keys()),
        "score_markov": list(score_mk.values()),
    })


# ---------------------------------------------------------------------------
# OPCIÓN 4 — Ensemble: combina las 3 capas + frecuencia base
# ---------------------------------------------------------------------------

def recommend_for_target(
    exp: pd.DataFrame,
    src_filter: Callable,
    tgt_lottery: str,
    tgt_draw: str,
    lag_days: int,
    top_n: int = 12,
    signal_weight: float = 0.70,
    base_weight: float = 0.30,
    ref_date: pd.Timestamp | None = None,
    target_weekday: int = -1,
    target_bucket: str = "unknown",
    last_two_draws: list[set[str]] | None = None,
) -> pd.DataFrame:
    """
    Ensemble de 4 capas:
      1. Ventanas temporales (30/90/365d)
      2. MI condicional por día/franja
      3. Markov orden 2
      4. Frecuencia base del target
    """
    if ref_date is None:
        ref_date = pd.Timestamp(exp["fecha_dt"].max())

    tgt_filter: Callable = (
        lambda e: (e["lottery"] == tgt_lottery) & (e["sorteo"] == tgt_draw)
    )

    src = exp[src_filter(exp)].copy()
    tgt = exp[tgt_filter(exp)].copy()

    if src.empty or tgt.empty:
        return pd.DataFrame(columns=[
            "num", "signal", "mi", "p_value", "a11",
            "score", "score_window", "score_cond_mi", "score_markov"
        ])

    src_p = _pivot(src[["fecha_dt", "num"]])
    tgt_p = _pivot(tgt[["fecha_dt", "num"]])

    if lag_days:
        tgt_p.index = tgt_p.index - pd.Timedelta(days=lag_days)

    # --- Capa 1: Ventanas temporales ---
    df_window = _score_window(src_p, tgt_p, ref_date).set_index("num")

    # --- Capa 2: MI condicional ---
    df_cond = _score_conditional_mi(src, tgt, target_weekday, target_bucket).set_index("num")

    # --- Capa 3: Markov orden 2 ---
    df_markov = _build_markov2_score(
        exp, tgt_lottery, tgt_draw,
        last_two_draws or []
    ).set_index("num")

    # --- Score base (frecuencia histórica del target) ---
    base = tgt.groupby("num").size().reset_index(name="count")
    base["p_base"] = base["count"] / max(len(tgt), 1)
    base = base.set_index("num")

    # --- Ensemble ---
    idx = pd.Index(_ALL_NUMS, name="num")

    def _get(df, col, default):
        return df[col].reindex(idx, fill_value=default) if col in df.columns else pd.Series(default, index=idx)

    sw  = _get(df_window, "score_window",  0.0)
    scm = _get(df_cond,   "score_cond_mi", 0.0)
    smk = _get(df_markov, "score_markov",  0.0)
    pb  = _get(base,      "p_base",        0.0)

    # Normalizar al rango [0, 1] antes de combinar
    def _norm(s: pd.Series) -> pd.Series:
        mn, mx = s.min(), s.max()
        return (s - mn) / (mx - mn) if mx > mn else pd.Series(0.0, index=s.index)

    score_ensemble = (
        _norm(sw)  * W_WINDOW  +
        _norm(scm) * W_COND_MI +
        _norm(smk) * W_MARKOV  +
        _norm(pb)  * 0.10
    )

    result = pd.DataFrame({
        "num":           list(idx),
        "score":         score_ensemble.values,
        "score_window":  sw.values,
        "score_cond_mi": scm.values,
        "score_markov":  smk.values,
        "p_base":        pb.values,
        "signal":        _get(df_window, "signal",  0.0).values,
        "mi":            _get(df_window, "mi",      0.0).values,
        "p_value":       _get(df_window, "p_value", 1.0).values,
        "a11":           _get(df_window, "a11",     0  ).astype(int).values,
    })

    return result.sort_values("score", ascending=False).head(top_n).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Palés
# ---------------------------------------------------------------------------

def top_pales(nums: List[str], k: int) -> List[Tuple[str, str]]:
    return list(itertools.islice(itertools.combinations(nums, 2), k))


# ---------------------------------------------------------------------------
# Alerta
# ---------------------------------------------------------------------------

def should_alert(
    recs: pd.DataFrame,
    min_signal: float,
    min_count_hits: int,
    min_strong: int = 2,
) -> bool:
    if recs.empty:
        return False
    strong = recs[
        (recs["signal"] >= min_signal) &
        (recs["a11"] >= min_count_hits)
    ]
    return len(strong) >= min_strong


# ---------------------------------------------------------------------------
# Helper: últimos 2 sorteos del target para Markov
# ---------------------------------------------------------------------------

def get_last_two_draws(
    exp: pd.DataFrame,
    lottery: str,
    draw: str,
    before_dt: pd.Timestamp,
) -> list[set[str]]:
    """
    Retorna los sets de números de los últimos 2 sorteos del target
    anteriores a before_dt. Alimenta Markov orden 2.
    """
    hist = exp[
        (exp["lottery"] == lottery) &
        (exp["sorteo"] == draw) &
        (exp["fecha_dt"] < before_dt)
    ].sort_values("fecha_dt")

    if hist.empty:
        return []

    by_date = (
        hist.groupby("fecha_dt")["num"]
        .apply(set)
        .reset_index()
        .sort_values("fecha_dt")
        .tail(2)
    )
    return by_date["num"].tolist()
