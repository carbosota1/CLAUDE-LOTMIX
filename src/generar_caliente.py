"""
generar_caliente.py  —  v3.0  (self-learning)
----------------------------------------------
Lee picks_log.csv y performance.csv desde Gitea (o rutas locales),
aprende percentiles de señal y posiciones calientes por draw en tiempo
real, y genera caliente.json con los números más probables.

MODOS DE USO:
  # CLI
  python3 generar_caliente.py --input picks_activos.json [--output caliente.json]
        [--picks-log data/picks_log.csv] [--perf-log outputs/performance.csv]

  # Desde runner.py
  from generar_caliente import run
  resultado = run(picks=lista_de_picks, output_path="outputs/caliente.json")

FUENTES DE DATOS HISTÓRICOS (en orden de prioridad):
  1. Rutas locales pasadas como argumento / parámetro
  2. Gitea remoto usando GITEA_TOKEN en variables de entorno
  3. Fallback a DRAW_PROFILE hardcodeado (última defensa)
"""

import json
import argparse
import sys
import os
import ast
from datetime import datetime
from collections import Counter

# ── CONFIGURACIÓN GITEA ────────────────────────────────────────────────────────
GITEA_BASE   = "https://gitea.totipicks.com"
GITEA_REPO   = "edgar26/CLAUDE-LOTMIX"
GITEA_TOKEN  = os.environ.get("GITEA_TOKEN", "62f0a9c97a551b23010ee7bd09ba3f817b873b7f")
PICKS_LOG_PATH = "data/picks_log.csv"
PERF_LOG_PATH  = "outputs/performance.csv"

# ── FALLBACK DRAW_PROFILE (si no hay datos históricos disponibles) ─────────────
# Calibrado con dataset 2026-04-29 → 2026-07-08 (496 picks)
FALLBACK_DRAW_PROFILE = {
    "Anguila 1PM":               {"hot_pos": [1, 3, 4, 11, 2, 5, 6, 8], "hit_rate": 40.4},
    "Anguila 6PM":               {"hot_pos": [8, 3, 7, 10, 11, 5, 6, 9], "hit_rate": 35.3},
    "Anguila 9PM":               {"hot_pos": [3, 4, 8, 9, 1, 5, 6, 7],  "hit_rate": 31.7},
    "Loteria Nacional- Gana Más":{"hot_pos": [8, 2, 4, 5, 6],           "hit_rate": 42.9},
    "Loteria Nacional- Noche":   {"hot_pos": [5, 3, 4, 1, 2, 6, 7],     "hit_rate": 35.1},
    "Quiniela La Primera":       {"hot_pos": [4, 3, 5, 9, 10, 11],       "hit_rate": 46.7},
    "Quiniela La Primera Noche": {"hot_pos": [1, 6, 5, 7, 8, 12],        "hit_rate": 25.0},
    "Quiniela La Suerte":        {"hot_pos": [2, 1, 7, 10, 12, 4, 6],    "hit_rate": 34.1},
    "Quiniela La Suerte 6PM":    {"hot_pos": [8, 10, 1, 2, 3, 4, 5],     "hit_rate": 27.6},
}

GLOBAL_HOT_POS   = [4, 5, 8, 3, 1, 2]
DECISIONES_VALIDAS = {"⚠️ JUGAR", "🔥 JUGAR AGRESIVO"}


# ══════════════════════════════════════════════════════════════════════════════
# CARGA DE DATOS HISTÓRICOS
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_gitea_csv(path: str) -> str | None:
    """Descarga un CSV de Gitea y retorna el contenido como string."""
    try:
        import urllib.request
        url = f"{GITEA_BASE}/api/v1/repos/{GITEA_REPO}/contents/{path}"
        req = urllib.request.Request(url, headers={"Authorization": f"token {GITEA_TOKEN}"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        import base64
        return base64.b64decode(data["content"]).decode("utf-8")
    except Exception as e:
        print(f"[WARN] No se pudo bajar {path} de Gitea: {e}", file=sys.stderr)
        return None


def _read_csv_string(content: str):
    """Parsea CSV string a lista de dicts."""
    import csv, io
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def _read_csv_file(path: str):
    """Lee CSV desde disco."""
    import csv
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def cargar_historico(picks_log_path: str = None, perf_log_path: str = None):
    """
    Carga picks_log y performance desde:
    1. Rutas locales si se pasan
    2. Gitea remoto
    3. Retorna None si falla todo
    """
    picks_rows, perf_rows = None, None

    # Intentar rutas locales
    if picks_log_path and os.path.exists(picks_log_path):
        picks_rows = _read_csv_file(picks_log_path)
        print(f"[INFO] picks_log cargado desde disco: {len(picks_rows)} filas")
    else:
        content = _fetch_gitea_csv(PICKS_LOG_PATH)
        if content:
            picks_rows = _read_csv_string(content)
            print(f"[INFO] picks_log cargado desde Gitea: {len(picks_rows)} filas")

    if perf_log_path and os.path.exists(perf_log_path):
        perf_rows = _read_csv_file(perf_log_path)
        print(f"[INFO] performance cargado desde disco: {len(perf_rows)} filas")
    else:
        content = _fetch_gitea_csv(PERF_LOG_PATH)
        if content:
            perf_rows = _read_csv_string(content)
            print(f"[INFO] performance cargado desde Gitea: {len(perf_rows)} filas")

    return picks_rows, perf_rows


# ══════════════════════════════════════════════════════════════════════════════
# APRENDIZAJE: CALCULAR PERFILES POR DRAW
# ══════════════════════════════════════════════════════════════════════════════

def _parse_list(val):
    """Convierte string tipo "['01','02']" a lista Python."""
    if isinstance(val, list):
        return val
    try:
        return ast.literal_eval(val)
    except Exception:
        return []


def aprender_perfiles(picks_rows, perf_rows) -> dict:
    """
    Cruza picks_log + performance, calcula por draw:
    - hot_pos: posiciones ordenadas por frecuencia de acierto
    - hit_rate: % de picks JUGAR que pegaron en top12
    - percentiles de señal: p25, p50, p75
    Retorna dict draw → perfil
    """
    # Indexar picks por key
    picks_by_key = {r["key"]: r for r in picks_rows}

    # Solo picks JUGAR con resultado disponible
    jugar = []
    for row in perf_rows:
        if row.get("decision", "") not in DECISIONES_VALIDAS:
            continue
        key = row.get("key", "")
        pick = picks_by_key.get(key, {})
        top12 = _parse_list(pick.get("top12", []))
        result = str(row.get("result", ""))
        result_nums = result.split("-") if result else []

        positions = []
        for n in result_nums:
            if n in top12:
                positions.append(top12.index(n) + 1)

        jugar.append({
            "draw":       row.get("draw", ""),
            "signal":     float(row.get("best_signal", 0) or 0),
            "a11":        float(row.get("best_a11", 0) or 0),
            "ok_alert":   row.get("ok_alert", "False") == "True",
            "pos_hit":    positions,
            "hit":        len(positions) > 0,
        })

    if not jugar:
        print("[WARN] Sin datos para aprender, usando fallback.", file=sys.stderr)
        return {}

    # Agrupar por draw
    by_draw = {}
    for r in jugar:
        by_draw.setdefault(r["draw"], []).append(r)

    perfiles = {}
    for draw, rows in by_draw.items():
        signals = [r["signal"] for r in rows]
        signals_sorted = sorted(signals)
        n = len(signals_sorted)

        def percentile(p):
            idx = int(p * n / 100)
            return signals_sorted[min(idx, n-1)]

        # Posiciones ordenadas por frecuencia
        all_pos = []
        for r in rows:
            all_pos.extend(r["pos_hit"])
        pos_counter = Counter(all_pos)
        hot_pos = [p for p, _ in pos_counter.most_common()]

        # Hit rate
        hits = sum(1 for r in rows if r["hit"])
        hit_rate = round(hits / len(rows) * 100, 1) if rows else 0

        perfiles[draw] = {
            "hot_pos":  hot_pos if hot_pos else GLOBAL_HOT_POS,
            "hit_rate": hit_rate,
            "p25":      percentile(25),
            "p50":      percentile(50),
            "p75":      percentile(75),
            "n":        len(rows),
        }
        print(f"[LEARN] {draw}: n={len(rows)} hit_rate={hit_rate}% hot_pos={hot_pos[:5]} p25={perfiles[draw]['p25']:.5f} p75={perfiles[draw]['p75']:.5f}")

    return perfiles


# ══════════════════════════════════════════════════════════════════════════════
# SELECCIÓN DE NÚMEROS
# ══════════════════════════════════════════════════════════════════════════════

def nivel_senal(signal: float, perfil: dict) -> str:
    """Clasifica la señal usando percentiles aprendidos del draw."""
    p25 = perfil.get("p25", 0.015)
    p50 = perfil.get("p50", 0.022)
    p75 = perfil.get("p75", 0.030)

    if signal >= p75:
        return "muy_alta"
    elif signal >= p50:
        return "alta"
    elif signal >= p25:
        return "media"
    else:
        return "baja"


def seleccionar_numeros(pick: dict, perfiles: dict) -> dict:
    """
    Dado un pick y los perfiles aprendidos, devuelve los números calientes.
    Posiciones históricas mandan siempre. Top3 es solo relleno.
    """
    signal   = pick.get("best_signal", 0) or 0
    top12    = _parse_list(pick.get("top12", []))
    topq     = _parse_list(pick.get("topq", []))
    draw     = pick.get("draw", "")
    decision = pick.get("decision", "")
    a11      = float(pick.get("best_a11", 0) or 0)
    ok_alert = pick.get("ok_alert", False)
    if isinstance(ok_alert, str):
        ok_alert = ok_alert == "True"

    # Perfil: aprendido > fallback > global
    perfil   = perfiles.get(draw) or FALLBACK_DRAW_PROFILE.get(draw, {})
    hot_pos  = perfil.get("hot_pos", GLOBAL_HOT_POS)
    hit_rate = perfil.get("hit_rate", 0)
    nivel    = nivel_senal(signal, perfil)

    # Límite de posiciones según nivel de señal
    limites = {"muy_alta": 6, "alta": 5, "media": 4, "baja": 3}
    limite  = limites[nivel]

    # Posiciones calientes primero
    numeros = []
    for pos in hot_pos[:limite]:
        idx = pos - 1
        if 0 <= idx < len(top12):
            numeros.append(top12[idx])

    # Relleno con top3 si faltan números
    for n in topq:
        if n not in numeros:
            numeros.append(n)
        if len(numeros) >= limite:
            break

    # Bonus: ok_alert=True AND a11 >= percentil 75 de a11 (aproximado con 3)
    if ok_alert and a11 >= 3 and len(numeros) < 7:
        for pos in hot_pos:
            idx = pos - 1
            if 0 <= idx < len(top12):
                n = top12[idx]
                if n not in numeros:
                    numeros.append(n)
                    break

    # Deduplicar preservando orden
    seen, final = set(), []
    for n in numeros:
        if n not in seen:
            seen.add(n)
            final.append(n)

    return {
        "numeros":          final,
        "signal":           round(signal, 6),
        "nivel_senal":      nivel,
        "decision":         decision,
        "a11":              a11,
        "ok_alert":         ok_alert,
        "hit_rate_historico": hit_rate,
        "draw":             draw,
        "lottery":          pick.get("lottery", ""),
        "date":             pick.get("date", ""),
        "time_rd":          pick.get("time_rd", ""),
    }


def procesar_picks(picks: list, perfiles: dict) -> dict:
    resultado = {}
    for pick in picks:
        if pick.get("decision", "") not in DECISIONES_VALIDAS:
            continue
        draw = pick.get("draw", "SIN_DRAW")
        resultado[draw] = seleccionar_numeros(pick, perfiles)
    return resultado


# ══════════════════════════════════════════════════════════════════════════════
# FUNCIÓN PÚBLICA PARA RUNNER.PY
# ══════════════════════════════════════════════════════════════════════════════

def run(picks: list,
        output_path: str = "outputs/caliente.json",
        picks_log_path: str = None,
        perf_log_path: str = None) -> dict:
    """
    Punto de entrada para runner.py.

    Args:
        picks:          Lista de picks del payload actual
        output_path:    Ruta de salida del caliente.json
        picks_log_path: Ruta local al picks_log.csv (opcional, si no busca en Gitea)
        perf_log_path:  Ruta local al performance.csv (opcional, si no busca en Gitea)

    Returns:
        Dict con estructura completa del caliente.json
    """
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else ".", exist_ok=True)

    # Aprender del historial
    picks_rows, perf_rows = cargar_historico(picks_log_path, perf_log_path)
    if picks_rows and perf_rows:
        perfiles = aprender_perfiles(picks_rows, perf_rows)
    else:
        print("[WARN] Usando FALLBACK_DRAW_PROFILE sin aprendizaje.", file=sys.stderr)
        perfiles = {}

    caliente = procesar_picks(picks, perfiles)

    output = {
        "generado_en":    datetime.now().isoformat(),
        "total_loterias": len(caliente),
        "loterias":       caliente,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    return output


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Genera caliente.json con aprendizaje dinámico")
    parser.add_argument("--input",     "-i", required=True, help="JSON del sistema principal")
    parser.add_argument("--output",    "-o", default="caliente.json")
    parser.add_argument("--picks-log", default=None, help="Ruta local picks_log.csv")
    parser.add_argument("--perf-log",  default=None, help="Ruta local performance.csv")
    args = parser.parse_args()

    try:
        with open(args.input, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] No se encontró: {args.input}", file=sys.stderr); sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON inválido: {e}", file=sys.stderr); sys.exit(1)

    picks = [data] if isinstance(data, dict) else data

    # Aprender
    picks_rows, perf_rows = cargar_historico(args.picks_log, args.perf_log)
    perfiles = aprender_perfiles(picks_rows, perf_rows) if picks_rows and perf_rows else {}

    caliente = procesar_picks(picks, perfiles)

    output = {
        "generado_en":    datetime.now().isoformat(),
        "total_loterias": len(caliente),
        "loterias":       caliente,
    }

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅  caliente.json → {args.output}")
    print(f"   Loterías: {len(caliente)}\n")
    for draw, info in caliente.items():
        nums = ", ".join(info["numeros"])
        print(f"   [{info['nivel_senal'].upper():9s}] {draw}")
        print(f"              Números : {nums}")
        print(f"              Señal   : {info['signal']}  p/draw  |  a11: {info['a11']}  |  ok_alert: {info['ok_alert']}")
        print(f"              Hit rate histórico: {info['hit_rate_historico']}%\n")


if __name__ == "__main__":
    main()