"""
scraper_base.py — Utilidades compartidas por todos los scrapers.
"""
from __future__ import annotations

import base64
import re
from datetime import date as dt_date, datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup, Tag

TZ_RD = ZoneInfo("America/Santo_Domingo")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def z2(x: str) -> str:
    """Normaliza a exactamente 2 dígitos. Devuelve '' si no hay dígitos."""
    s = str(x).strip()
    if re.fullmatch(r"\d{2}", s):
        return s
    m = re.search(r"\d+", s)
    return m.group(0).zfill(2) if m else ""


def encode_d_param(d: dt_date) -> str:
    """
    Genera el parámetro ?d= usado por loteriadominicana.com.do:
        ddmmyyyy → invertir → decimal → HEX uppercase → base64(HEX)
    """
    ddmmyyyy = d.strftime("%d%m%Y")
    rev = ddmmyyyy[::-1]
    hx = format(int(rev), "X")
    return base64.b64encode(hx.encode()).decode()


def build_url(base: str, d: dt_date) -> str:
    return f"{base}?d={encode_d_param(d)}"


def parse_date(date_str: str) -> dt_date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()


def fetch_soup(url: str, timeout: int = 30) -> BeautifulSoup:
    resp = requests.get(url, headers=HEADERS, timeout=timeout)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def extract_numbers_near_h4(h4: Tag, max_ascent: int = 10) -> list[str]:
    """
    Sube por el DOM desde <h4> hasta encontrar el contenedor con bolas
    y extrae los primeros 3 números.
    """
    container = h4.parent
    for _ in range(max_ascent):
        if container is None:
            break
        if container.find(class_=re.compile(r"result-item-ball-content|ball")):
            break
        container = container.parent

    if not container:
        return []

    # Método principal: <div class="ball"><span>NN</span></div>
    balls = container.select("div.ball span")
    nums = [z2(b.get_text(strip=True)) for b in balls if b.get_text(strip=True)]
    nums = [n for n in nums if n]

    # Fallback: texto plano del bloque
    if len(nums) < 3:
        txt_nums = re.findall(r"\b\d{1,2}\b", container.get_text(" ", strip=True))
        nums = [z2(x) for x in txt_nums if z2(x)]

    return nums[:3]


def find_h4_by_title(soup: BeautifulSoup, target_title: str) -> Tag | None:
    """Devuelve el primer <h4> cuyo texto coincide exactamente con target_title."""
    for h4 in soup.find_all("h4"):
        title = re.sub(r"\s+", " ", h4.get_text(strip=True)).strip()
        if title == target_title:
            return h4
    return None


def get_result_generic(
    base_url: str,
    draw: str,
    date: str,
    aliases: dict[str, str] | None = None,
    lottery_label: str = "Lottery",
) -> tuple[str, str, str]:
    """
    Lógica genérica de scraping para sorteos en loteriadominicana.com.do.

    Args:
        base_url:      URL base del sorteo (sin ?d=).
        draw:          Nombre del sorteo (exacto o alias).
        date:          'YYYY-MM-DD'.
        aliases:       Mapeo {alias → nombre real en <h4>}.
        lottery_label: Etiqueta para mensajes de error.

    Returns:
        (primero, segundo, tercero) como strings de 2 dígitos.
    """
    aliases = aliases or {}
    target_title = re.sub(r"\s+", " ", aliases.get(draw, draw).strip())
    d = parse_date(date)
    url = build_url(base_url, d)

    soup = fetch_soup(url)
    h4 = find_h4_by_title(soup, target_title)

    if h4 is None:
        visible = [re.sub(r"\s+", " ", t.get_text(strip=True)) for t in soup.find_all("h4")]
        raise ValueError(
            f"[{lottery_label}] Sorteo '{target_title}' no encontrado para {date}. "
            f"H4 visibles: {visible[:20]}"
        )

    nums = extract_numbers_near_h4(h4)
    if len(nums) < 3:
        raise ValueError(
            f"[{lottery_label}] Resultado aún no publicado para '{target_title}' ({date})."
        )

    return nums[0], nums[1], nums[2]
