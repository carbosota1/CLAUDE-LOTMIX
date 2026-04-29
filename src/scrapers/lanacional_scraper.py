"""
lanacional_scraper.py — Resultados de La Nacional (loteriadominicana.com.do).
"""
from scraper_base import get_result_generic

_BASE_URL = "https://www.loteriadominicana.com.do/Lottery/National"

_ALIASES = {
    "LN-GanaMas": "Loteria Nacional- Gana Más",
    "LN-Noche":   "Loteria Nacional- Noche",
}


def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: nombre exacto del sorteo o alias (LN-GanaMas, LN-Noche)
    date: 'YYYY-MM-DD'
    return: (primero, segundo, tercero) con 2 dígitos
    """
    return get_result_generic(
        base_url=_BASE_URL,
        draw=draw,
        date=date,
        aliases=_ALIASES,
        lottery_label="La Nacional",
    )
