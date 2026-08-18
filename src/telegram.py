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
    Reintenta con timeouts progresivos (20s, 35s, 60s) para
    conexiones lentas o intermitentes.
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

    # Reintentos con timeout progresivo para conexiones lentas
    last_err = None
    for timeout in (20, 35, 60):
        try:
            resp = requests.post(url, json=payload, timeout=timeout)
            resp.raise_for_status()
            return
        except requests.exceptions.Timeout:
            last_err = f"Timeout después de {timeout}s"
            continue
        except requests.exceptions.RequestException as e:
            raise RuntimeError(f"Error enviando Telegram: {e}") from e

    raise RuntimeError(f"Telegram falló tras 3 intentos: {last_err}")
