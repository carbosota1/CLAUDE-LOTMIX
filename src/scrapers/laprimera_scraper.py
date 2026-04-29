"""
laprimera_scraper.py — Resultados de La Primera (loteriadominicana.com.do).
"""
from scraper_base import get_result_generic

_BASE_URL = "https://www.loteriadominicana.com.do/Lottery/Lotodom"

_VALID_DRAWS = {
    "Quiniela La Primera",
    "Quiniela La Primera Noche",
}


def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: 'Quiniela La Primera' o 'Quiniela La Primera Noche'
    date: 'YYYY-MM-DD'
    return: (primero, segundo, tercero) con 2 dígitos
    """
    draw = " ".join(draw.split()).strip()
    if draw not in _VALID_DRAWS:
        raise ValueError(
            f"[La Primera] Draw no reconocido: '{draw}'. "
            f"Esperado uno de: {sorted(_VALID_DRAWS)}"
        )

    return get_result_generic(
        base_url=_BASE_URL,
        draw=draw,
        date=date,
        lottery_label="La Primera",
    )
