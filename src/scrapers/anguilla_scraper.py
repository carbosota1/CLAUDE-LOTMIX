"""
anguilla_scraper.py — Resultados de Anguilla vía enloteria.com.

Fuente reemplazada: loteriadominicana.com.do tenía resultados desactualizados
para algunos sorteos de Anguilla (ej. 6PM repetía el resultado del día anterior).
"""
import re
from datetime import datetime, date as dt_date
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

TZ_RD = ZoneInfo("America/Santo_Domingo")

BASE_URL = "https://enloteria.com"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# Mapea el nombre EXACTO del XLSX (columna 'sorteo') al nombre usado en enloteria.com
DRAW_ALIASES = {
    "Anguila 10AM": "Anguilla 10AM",
    "Anguila 1PM":  "Anguilla 1PM",
    "Anguila 6PM":  "Anguilla 6PM",
    "Anguila 9PM":  "Anguilla 9PM",
}

VALID_SORTEOS = {
    "Anguilla 8AM",  "Anguilla 9AM",  "Anguilla 10AM", "Anguilla 11AM",
    "Anguilla 12PM", "Anguilla 1PM",  "Anguilla 2PM",  "Anguilla 3PM",
    "Anguilla 4PM",  "Anguilla 5PM",  "Anguilla 6PM",  "Anguilla 7PM",
    "Anguilla 8PM",  "Anguilla 9PM",  "Anguilla 10PM",
}


def z2(x: str) -> str:
    """Normaliza a exactamente 2 dígitos."""
    s = str(x).strip()
    if re.fullmatch(r"\d{2}", s):
        return s
    m = re.search(r"\d+", s)
    return m.group(0).zfill(2) if m else ""


def _parse_date(date_str: str) -> dt_date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def _fetch_html(url: str) -> str:
    resp = requests.get(url, headers=HEADERS, timeout=25)
    resp.raise_for_status()
    return resp.text


def _extraer_sorteos(html: str) -> dict[str, list[str]]:
    """
    Extrae todos los sorteos de Anguilla disponibles en la página diaria.
    Retorna {nombre_sorteo: [p1, p2, p3]}.
    """
    soup = BeautifulSoup(html, "html.parser")
    blocks = soup.find_all(attrs={"data-lottery-name": True})

    resultados: dict[str, list[str]] = {}
    for block in blocks:
        nombre = (block.get("data-lottery-name") or "").strip()
        if nombre not in VALID_SORTEOS:
            continue

        num_divs = block.find_all("div", class_="result-number")
        numeros = []
        for d in num_divs:
            txt = d.get_text(strip=True)
            if re.fullmatch(r"\d{1,2}", txt):
                numeros.append(z2(txt))

        if len(numeros) >= 3:
            resultados[nombre] = numeros[:3]

    return resultados


def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: nombre exacto del sorteo en el XLSX (ej. 'Anguila 6PM')
    date: 'YYYY-MM-DD'
    return: (primero, segundo, tercero) con 2 dígitos.

    Lanza ValueError si el resultado aún no está publicado o el sorteo
    no se reconoce.
    """
    target_title = DRAW_ALIASES.get(draw, draw).strip()

    d = _parse_date(date)
    url = f"{BASE_URL}/resultados-loterias-{d.strftime('%Y-%m-%d')}"

    html = _fetch_html(url)
    resultados = _extraer_sorteos(html)

    if target_title not in resultados:
        raise ValueError(
            f"[Anguilla] Resultado aún no publicado para '{target_title}' ({date}). "
            f"Disponibles: {sorted(resultados.keys())}"
        )

    nums = resultados[target_title]
    if len(nums) < 3:
        raise ValueError(f"[Anguilla] Números insuficientes para '{target_title}' ({date}).")

    return nums[0], nums[1], nums[2]