"""
generar_caliente.py
-------------------
Lee el JSON que produce el sistema principal (picks activos) y genera
caliente.json con los números calientes por lotería.

Lógica de selección (basada en análisis del picks_log + performance):
  - Señal MUY ALTA  (>= 0.035) → usa posiciones top12 prioritarias del draw  (hasta 6 números)
  - Señal ALTA      (>= 0.025) → posiciones calientes confirmadas            (hasta 5 números)
  - Señal MEDIA     (>= 0.015) → top3 (topq) + posiciones extra confirmadas  (hasta 4 números)
  - Señal BAJA      (<  0.015) → solo top3 (topq)                            (3 números)

  Solo se procesan picks con decision ⚠️ JUGAR o 🔥 JUGAR AGRESIVO.
  Si corren varias loterías a la vez, cada una va como clave separada.

Uso:
    python3 generar_caliente.py --input picks_activos.json [--output caliente.json]

El --input puede ser un archivo con un solo objeto (una lotería) o una
lista de objetos (varias loterías corriendo juntas).
"""

import json
import argparse
import sys
from datetime import datetime

# ── Posiciones calientes por draw (derivadas del análisis histórico) ──────────
# Clave: nombre exacto del draw tal como aparece en el JSON del sistema
# "hot_pos": posiciones 1-indexadas del top12 que históricamente pegan más
# "hit_rate": tasa de hit top12 observada (referencia)
DRAW_PROFILE = {
    "Anguila 1PM": {
        "hot_pos": [1, 5, 8, 3, 4, 6],   # P1(×2), P5(×2), P8(×2) son las más frecuentes
        "hit_rate": 57.1
    },
    "Anguila 6PM": {
        "hot_pos": [8, 1, 5],
        "hit_rate": 25.0
    },
    "Anguila 9PM": {
        "hot_pos": [8, 4, 5, 3, 7, 10, 11],  # P8(×2) lidera, cobertura amplia
        "hit_rate": 75.0
    },
    "Loteria Nacional- Gana Más": {
        "hot_pos": [8, 2, 5],
        "hit_rate": 33.3
    },
    "Loteria Nacional- Noche": {
        "hot_pos": [5, 2, 8],              # P5(×2) dominante
        "hit_rate": 42.9
    },
    "Quiniela La Primera": {
        "hot_pos": [4, 5, 3],             # P4(×3) muy dominante
        "hit_rate": 50.0
    },
    "Quiniela La Primera Noche": {
        "hot_pos": [5, 12, 4],
        "hit_rate": 40.0
    },
    "Quiniela La Suerte": {
        "hot_pos": [1, 10, 5],
        "hit_rate": 28.6
    },
    "Quiniela La Suerte 6PM": {
        "hot_pos": [1, 5, 8],             # sin hits históricos, fallback genérico
        "hit_rate": 0.0
    },
}

# Posiciones calientes globales (fallback si el draw no está en el perfil)
GLOBAL_HOT_POS = [5, 8, 4, 1, 3]

DECISIONES_VALIDAS = {"⚠️ JUGAR", "🔥 JUGAR AGRESIVO"}


def nivel_senal(signal: float) -> str:
    if signal >= 0.035:
        return "muy_alta"
    elif signal >= 0.025:
        return "alta"
    elif signal >= 0.015:
        return "media"
    else:
        return "baja"


def seleccionar_numeros(pick: dict) -> dict:
    """
    Dado un pick del sistema, devuelve la lista de números calientes
    y metadata de decisión.
    """
    signal    = pick.get("best_signal", 0) or 0
    top12     = pick.get("top12", [])
    topq      = pick.get("topq", [])
    draw      = pick.get("draw", "")
    decision  = pick.get("decision", "")
    a11       = pick.get("best_a11", 0) or 0
    ok_alert  = pick.get("ok_alert", False)

    # Normalizar listas (pueden venir como string JSON o ya como lista)
    if isinstance(top12, str):
        try:
            top12 = json.loads(top12)
        except Exception:
            top12 = []
    if isinstance(topq, str):
        try:
            topq = json.loads(topq)
        except Exception:
            topq = []

    perfil   = DRAW_PROFILE.get(draw, {})
    hot_pos  = perfil.get("hot_pos", GLOBAL_HOT_POS)
    hit_rate = perfil.get("hit_rate", 0)
    nivel    = nivel_senal(signal)

    numeros_calientes = []

    # ── Estrategia por nivel de señal ──────────────────────────────────────
    if nivel == "muy_alta":
        # Señal >= 0.035: cubrir todas las posiciones calientes históricas
        limite = 6
        for pos in hot_pos[:limite]:
            idx = pos - 1
            if 0 <= idx < len(top12):
                numeros_calientes.append(top12[idx])
        # Asegurar que el top3 también esté incluido
        for n in topq:
            if n not in numeros_calientes:
                numeros_calientes.insert(0, n)

    elif nivel == "alta":
        # Señal >= 0.025: posiciones calientes confirmadas
        limite = 5
        for pos in hot_pos[:limite]:
            idx = pos - 1
            if 0 <= idx < len(top12):
                numeros_calientes.append(top12[idx])
        # Añadir top3 si no está
        for n in topq:
            if n not in numeros_calientes:
                numeros_calientes.insert(0, n)

    elif nivel == "media":
        # Señal >= 0.015: top3 + hasta 1-2 posiciones extra calientes
        numeros_calientes = list(topq)
        extra_limite = 4 - len(numeros_calientes)
        for pos in hot_pos[:extra_limite + 2]:
            idx = pos - 1
            if 0 <= idx < len(top12):
                n = top12[idx]
                if n not in numeros_calientes:
                    numeros_calientes.append(n)
                    if len(numeros_calientes) >= 4:
                        break

    else:
        # Señal baja < 0.015: solo top3
        numeros_calientes = list(topq)

    # Bonus: si ok_alert=True y a11 >= 3, añadir siguiente posición caliente
    if ok_alert and a11 >= 3 and len(numeros_calientes) < 7:
        for pos in hot_pos:
            idx = pos - 1
            if 0 <= idx < len(top12):
                n = top12[idx]
                if n not in numeros_calientes:
                    numeros_calientes.append(n)
                    break

    # Deduplicar preservando orden
    seen = set()
    final = []
    for n in numeros_calientes:
        if n not in seen:
            seen.add(n)
            final.append(n)

    return {
        "numeros": final,
        "signal": round(signal, 6),
        "nivel_senal": nivel,
        "decision": decision,
        "a11": a11,
        "ok_alert": ok_alert,
        "hit_rate_historico": hit_rate,
        "draw": draw,
        "lottery": pick.get("lottery", ""),
        "date": pick.get("date", ""),
        "time_rd": pick.get("time_rd", ""),
    }


def procesar_picks(picks: list) -> dict:
    """
    Recibe lista de picks (puede ser 1 o varios corriendo juntos).
    Devuelve dict con clave = draw, valor = resultado de selección.
    """
    resultado = {}

    for pick in picks:
        decision = pick.get("decision", "")
        if decision not in DECISIONES_VALIDAS:
            continue

        draw = pick.get("draw", "SIN_DRAW")
        resultado[draw] = seleccionar_numeros(pick)

    return resultado


def main():
    parser = argparse.ArgumentParser(
        description="Genera caliente.json desde el JSON del sistema de picks"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Archivo JSON generado por el sistema principal (objeto único o lista)"
    )
    parser.add_argument(
        "--output", "-o",
        default="caliente.json",
        help="Archivo de salida (default: caliente.json)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Mostrar resumen en consola"
    )
    args = parser.parse_args()

    # Leer input
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] No se encontró el archivo: {args.input}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON inválido en {args.input}: {e}", file=sys.stderr)
        sys.exit(1)

    # Normalizar: aceptar objeto único o lista
    if isinstance(data, dict):
        picks = [data]
    elif isinstance(data, list):
        picks = data
    else:
        print("[ERROR] El JSON debe ser un objeto o una lista de objetos.", file=sys.stderr)
        sys.exit(1)

    caliente = procesar_picks(picks)

    if not caliente:
        print("[INFO] Ningún pick con decisión JUGAR encontrado. caliente.json vacío.")
        caliente = {}

    # Metadata del run
    output = {
        "generado_en": datetime.now().isoformat(),
        "total_loterias": len(caliente),
        "loterias": caliente
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    if args.verbose or True:
        print(f"\n✅  caliente.json generado → {args.output}")
        print(f"   Loterías procesadas: {len(caliente)}\n")
        for draw, info in caliente.items():
            nums = ", ".join(info["numeros"])
            print(f"   [{info['nivel_senal'].upper():9s}] {draw}")
            print(f"              Números : {nums}")
            print(f"              Señal   : {info['signal']}  |  a11: {info['a11']}  |  ok_alert: {info['ok_alert']}")
            print(f"              Hit rate histórico del draw: {info['hit_rate_historico']}%\n")


if __name__ == "__main__":
    main()
