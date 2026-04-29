"""
lasuerte_scraper.py — Resultados de La Suerte (loteriadominicana.com.do).
"""
from scraper_base import get_result_generic

_BASE_URL = "https://www.loteriadominicana.com.do/Lottery/DominicanLuck"

_VALID_DRAWS = {
    "Quiniela La Suerte",
    "Quiniela La Suerte 6PM",
}


def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: 'Quiniela La Suerte' o 'Quiniela La Suerte 6PM'
    date: 'YYYY-MM-DD'
    return: (primero, segundo, tercero) con 2 dígitos
    """
    draw = " ".join(draw.split()).strip()
    if draw not in _VALID_DRAWS:
        raise ValueError(
            f"[La Suerte] Draw no reconocido: '{draw}'. "
            f"Esperado uno de: {sorted(_VALID_DRAWS)}"
        )

    return get_result_generic(
        base_url=_BASE_URL,
        draw=draw,
        date=date,
        lottery_label="La Suerte",
    )
