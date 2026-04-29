"""
analyze.py — Estadísticas de correlación entre sorteos (Chi² + MI).
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
# Utilidades
# ---------------------------------------------------------------------------

def z2(n: int | str) -> str:
    return str(n).zfill(2)


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
    return x[["fecha_dt", "fecha", "lottery", "sorteo", "num"]]


# ---------------------------------------------------------------------------
# Construcción de pares (vectorizada)
# ---------------------------------------------------------------------------

def build_pairs(
    exp: pd.DataFrame,
    src_filter: Callable[[pd.DataFrame], pd.Series],
    tgt_filter: Callable[[pd.DataFrame], pd.Series],
    lag_days: int,
) -> Optional[pd.DataFrame]:
    """
    Para cada (fecha_src, fecha_tgt=fecha_src+lag) construye una tabla
    indicando si cada número del rango apareció en src y en tgt.

    Retorna None si no hay datos suficientes.
    """
    src = exp[src_filter(exp)][["fecha_dt", "num"]].copy()
    tgt = exp[tgt_filter(exp)][["fecha_dt", "num"]].copy()

    if src.empty or tgt.empty:
        return None

    # Pivot: una fila por fecha, una columna por número (0/1)
    def _pivot(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["present"] = 1
        p = (
            df.groupby(["fecha_dt", "num"])["present"]
            .max()
            .unstack(fill_value=0)
            .reindex(columns=_ALL_NUMS, fill_value=0)
        )
        return p

    src_p = _pivot(src)
    tgt_p = _pivot(tgt)

    if lag_days:
        tgt_p.index = tgt_p.index - pd.Timedelta(days=lag_days)

    common_dates = src_p.index.intersection(tgt_p.index)
    if common_dates.empty:
        return None

    src_p = src_p.loc[common_dates]
    tgt_p = tgt_p.loc[common_dates]

    # Construir DataFrame largo: (num, src_event, tgt_event)
    rows = []
    for num in _ALL_NUMS:
        s = src_p[num].values if num in src_p.columns else np.zeros(len(common_dates), dtype=int)
        t = tgt_p[num].values if num in tgt_p.columns else np.zeros(len(common_dates), dtype=int)
        col = np.column_stack([s, t])
        rows.append(
            pd.DataFrame(col, columns=["src_event", "tgt_event"]).assign(num=num)
        )

    return pd.concat(rows, ignore_index=True)[["num", "src_event", "tgt_event"]]


# ---------------------------------------------------------------------------
# Estadísticas por número (vectorizada donde es posible)
# ---------------------------------------------------------------------------

def stats_per_num(pairs: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula Chi², p-value, MI y a11 por número.
    Combina la señal como: signal = mi * (1 - p_value).
    """
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

        out.append({
            "num": num,
            "chi2": float(chi2),
            "p_value": float(p),
            "mi": float(mi),
            "a11": a,
        })

    df = pd.DataFrame(out)
    df["signal"] = df["mi"] * (1.0 - df["p_value"].clip(0, 1))
    return df


# ---------------------------------------------------------------------------
# Recomendación para un target
# ---------------------------------------------------------------------------

def recommend_for_target(
    exp: pd.DataFrame,
    src_filter: Callable[[pd.DataFrame], pd.Series],
    tgt_lottery: str,
    tgt_draw: str,
    lag_days: int,
    top_n: int = 12,
    signal_weight: float = 0.70,
    base_weight: float = 0.30,
) -> pd.DataFrame:
    """
    Devuelve los top_n números con mayor score para el sorteo target.
    score = signal_weight * signal + base_weight * p_base
    """
    tgt_filter: Callable[[pd.DataFrame], pd.Series] = (
        lambda e: (e["lottery"] == tgt_lottery) & (e["sorteo"] == tgt_draw)
    )

    pairs = build_pairs(exp, src_filter, tgt_filter, lag_days=lag_days)
    if pairs is None:
        return pd.DataFrame(columns=["num", "signal", "mi", "p_value", "a11", "score"])

    st = stats_per_num(pairs)

    tgt = exp[tgt_filter(exp)]
    base = tgt.groupby("num").size().reset_index(name="count")
    base["p_base"] = base["count"] / max(len(tgt), 1)

    out = st.merge(base[["num", "p_base"]], on="num", how="left").fillna({"p_base": 0.0})
    out["score"] = signal_weight * out["signal"] + base_weight * out["p_base"]
    return out.sort_values("score", ascending=False).head(top_n)


# ---------------------------------------------------------------------------
# Pares (palés)
# ---------------------------------------------------------------------------

def top_pales(nums: List[str], k: int) -> List[Tuple[str, str]]:
    """Genera combinaciones de pares sin repetición, limitado a k."""
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
    """
    Devuelve True si hay al menos min_strong números con señal >= min_signal
    y a11 >= min_count_hits.
    """
    if recs.empty:
        return False

    strong = recs[
        (recs["signal"] >= min_signal) &
        (recs["a11"] >= min_count_hits)
    ]
    return len(strong) >= min_strong
