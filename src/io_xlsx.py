"""
io_xlsx.py — Lectura y escritura del historial en Excel.
"""
import os
import pandas as pd

COLS = ["fecha", "sorteo", "primero", "segundo", "tercero"]
_HIST_SHEET = "history"


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def normalize_2d(x: str) -> str:
    """Extrae dígitos y devuelve exactamente 2 caracteres (zfill). Devuelve '' si no hay dígitos."""
    digits = "".join(c for c in str(x).strip() if c.isdigit())
    return digits.zfill(2) if digits else ""


def read_history_xlsx(path: str) -> pd.DataFrame:
    """Lee el historial desde un XLSX. Devuelve DataFrame vacío si no existe."""
    if not os.path.exists(path):
        return pd.DataFrame(columns=COLS)

    try:
        df = pd.read_excel(path, sheet_name=_HIST_SHEET, dtype=str, engine="openpyxl")
    except ValueError:
        xls = pd.ExcelFile(path, engine="openpyxl")
        df = pd.read_excel(path, sheet_name=xls.sheet_names[0], dtype=str, engine="openpyxl")

    # Asegurar todas las columnas esperadas
    for c in COLS:
        if c not in df.columns:
            df[c] = ""

    df = df[COLS].fillna("")

    for c in ("primero", "segundo", "tercero"):
        df[c] = df[c].astype(str).map(normalize_2d)

    df["fecha"] = df["fecha"].astype(str).str.strip()
    df["sorteo"] = df["sorteo"].astype(str).str.strip()
    return df


def upsert_history_xlsx(path: str, new_rows: pd.DataFrame) -> None:
    """
    Inserta o actualiza filas en el XLSX.
    Unicidad por (fecha, sorteo) — mantiene la última versión.
    Escribe siempre en hoja 'history'.
    """
    old = read_history_xlsx(path)
    df = (
        pd.concat([old, new_rows], ignore_index=True)
        .fillna("")
        .drop_duplicates(subset=["fecha", "sorteo"], keep="last")
        .sort_values(["fecha", "sorteo"])
        .reset_index(drop=True)
    )

    ensure_dir(os.path.dirname(path) or ".")
    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as writer:
        df.to_excel(writer, sheet_name=_HIST_SHEET, index=False)
