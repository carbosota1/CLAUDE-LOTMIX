"""
anguilla_scraper.py — Resultados de Anguilla (loteriadominicana.com.do).
"""
from scraper_base import get_result_generic

_BASE_URL = "https://www.loteriadominicana.com.do/Lottery/Anguilla"

_ALIASES = {
    "ANG-10AM": "Anguila 10AM",
    "ANG-1PM":  "Anguila 1PM",
    "ANG-6PM":  "Anguila 6PM",
    "ANG-9PM":  "Anguila 9PM",
}


def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: nombre exacto del sorteo o alias (ANG-10AM, etc.)
    date: 'YYYY-MM-DD'
    return: (primero, segundo, tercero) con 2 dígitos
    """
    return get_result_generic(
        base_url=_BASE_URL,
        draw=draw,
        date=date,
        aliases=_ALIASES,
        lottery_label="Anguilla",
    )
