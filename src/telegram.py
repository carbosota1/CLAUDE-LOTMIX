"""
telegram.py — Envío de mensajes por Telegram.
"""
import os
import requests


def send_telegram(text: str, parse_mode: str | None = None) -> None:
    """
    Envía un mensaje al chat configurado vía variables de entorno.

    Variables requeridas:
        TELEGRAM_BOT_TOKEN
        TELEGRAM_CHAT_ID

    Lanza RuntimeError si faltan variables o la petición falla.
    """
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        raise RuntimeError(
            "Faltan variables de entorno: TELEGRAM_BOT_TOKEN y/o TELEGRAM_CHAT_ID"
        )

    payload: dict = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(url, json=payload, timeout=15)
    resp.raise_for_status()
