"""
Meltwater Auto-Capture — CMC (Panamá)
=======================================
Captura el snapshot del shareable report de Meltwater (LATAM) y lo INGESTA
directamente al Cloudflare Worker del Monitor CMC.

Flujo:
  1. Abre la página de Meltwater en Chromium real (Playwright).
  2. Pasa el password gate (Web Component flux-textfield#passcode).
  3. ESPERA a que Meltwater dispare la URL `insightPageSnapshot.json.gz`.
     No recarga en loop: cada reload REINICIA la generación del snapshot.
  4. Baja el .gz, lo descomprime y valida que traiga datos reales (señal > 0).
  5. POSTea el JSON completo a WORKER_INGEST_URL con el header x-sync-token.

Por qué /ingest y no /update-url (ni Make):
  Meltwater devuelve 502 a IPs de datacenter. Los datacenters de Cloudflare y
  de Make entran en esa categoría, así que si el que baja el .gz es el Worker
  (o Make), el fetch contra Meltwater falla y el KV nunca se actualiza.
  Con /ingest, Cloudflare nunca toca Meltwater: el runner de GitHub ya baja el
  .gz de todas formas (para validar la señal), así que mandar el JSON en vez de
  la URL no cuesta nada extra y elimina el bloqueo.

Validación de señal:
  Meltwater puede servir el .gz con la estructura completa (insightPage.tabs)
  pero con las agregaciones en CERO mientras regenera. compute_signal() recorre
  tabs -> rows -> cards -> fragments sumando hits/totals/counts. Si da 0, NO se
  ingesta, para no pisar 'latest_cmc' con un snapshot vacío.

El debug queda en debug_output/ y el workflow lo sube como artifact privado.
"""

import asyncio
import os
import sys
import json
import gzip
import time
import random
import urllib.request
import traceback
from pathlib import Path
from urllib.parse import urlparse
from playwright.async_api import async_playwright


# ── Config (todo por entorno; sin secretos hardcodeados) ─────────────────────
def _req(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        sys.exit(f"FALTA el secret/env requerido: {name}")
    return val


MELTWATER_URL = _req("MELTWATER_URL")
MELTWATER_PASSWORD = os.environ.get("MELTWATER_PASSWORD", "")  # vacío si no hay gate
WORKER_INGEST_URL = _req("WORKER_INGEST_URL")
SYNC_TOKEN = _req("SYNC_TOKEN")

# Tiempos
MAX_WAIT_SECONDS = int(os.environ.get("MAX_WAIT_SECONDS", "300"))
POLL_INTERVAL_MS = int(os.environ.get("POLL_INTERVAL_MS", "5000"))
STALL_RELOAD_SECONDS = int(os.environ.get("STALL_RELOAD_SECONDS", "120"))
JITTER_SECONDS = int(os.environ.get("JITTER_SECONDS", "0"))
HEADLESS = os.environ.get("HEADLESS", "true").lower() != "false"

# Proxy opcional (NO requerido). Ej: http://user:pass@host:port
PROXY_URL = os.environ.get("PROXY_URL", "").strip()

DEBUG_DIR = Path("debug_output")
DEBUG_DIR.mkdir(exist_ok=True)


def compute_signal(data: dict) -> int:
    """Suma de 'señal' recorriendo tabs/rows/cards/fragments. 0 = snapshot vacío."""
    ip = (data or {}).get("insightPage") or {}
    signal = 0
    for t in ip.get("tabs") or []:
        for row in t.get("rows") or []:
            for card in row.get("cards") or []:
                for frag in card.get("fragments") or []:
                    d = frag.get("data") or {}
                    hits = d.get("hits")
                    if isinstance(hits, list):
                        signal += len(hits)
                    tot = d.get("total")
                    if isinstance(tot, (int, float)):
                        signal += tot
                    date = (d.get("aggs") or {}).get("date") or {}
                    for v in date.get("values") or []:
                        signal += (v.get("counts") or {}).get("doc") or 0
    return signal


async def fetch_gz(page, url: str):
    """Baja el .gz en la misma sesión, descomprime y devuelve (data, signal, size)."""
    resp = await page.request.get(url, timeout=45_000)
    if not resp.ok:
        print(f"  GET gz status {resp.status}", flush=True)
        return None, -1, 0
    body = await resp.body()
    size = len(body)
    if len(body) >= 2 and body[0] == 0x1F and body[1] == 0x8B:
        try:
            body = gzip.decompress(body)
        except Exception as e:
            print(f"  gzip.decompress fallo: {e}", flush=True)
            return None, -1, size
    try:
        data = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as e:
        print(f"  json.loads fallo: {e}", flush=True)
        return None, -1, size
    return data, compute_signal(data), size


def _proxy_kwargs():
    if not PROXY_URL:
        return {}
    p = urlparse(PROXY_URL)
    server = f"{p.scheme}://{p.hostname}" + (f":{p.port}" if p.port else "")
    proxy = {"server": server}
    if p.username:
        proxy["username"] = p.username
    if p.password:
        proxy["password"] = p.password
    print(f"→ Usando proxy: {server}", flush=True)
    return {"proxy": proxy}


async def capture_snapshot() -> dict:
    """Devuelve el JSON del snapshot ya validado (señal > 0)."""
    gz = {"url": None}
    snapshot_statuses = []
    all_responses = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=HEADLESS,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            locale="es-ES",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            extra_http_headers={"Accept-Language": "es-ES,es;q=0.9,en;q=0.8"},
            **_proxy_kwargs(),
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        page = await context.new_page()

        def on_response(response):
            url, st = response.url, response.status
            all_responses.append(f"{st} {url[:160]}")
            if "snapshot" in url:
                snapshot_statuses.append(f"{st} {url[:120]}")
            if "insightPageSnapshot.json.gz" in url and st == 200:
                gz["url"] = url
                print("✓ URL .gz detectada", flush=True)

        page.on("response", on_response)

        print("→ Navegando a Meltwater...", flush=True)
        await page.goto(MELTWATER_URL, wait_until="domcontentloaded", timeout=60_000)
        print(f"  page.url = {page.url}", flush=True)
        print(f"  page.title = {await page.title()}", flush=True)

        # ── Password gate (Web Component flux-textfield#passcode) ────────────
        try:
            await page.wait_for_selector("#passcode", timeout=20_000, state="attached")
            print("→ Password gate detectado, enviando passcode...", flush=True)
            await page.wait_for_function(
                "() => document.getElementById('passcode') && "
                "document.getElementById('submit') && "
                "typeof submitPasscode === 'function'",
                timeout=15_000,
            )
            await page.evaluate(
                """(pwd) => {
                    const input = document.getElementById('passcode');
                    input.value = pwd;
                    input.dispatchEvent(new Event('input', { bubbles: true }));
                    submitPasscode();
                }""",
                MELTWATER_PASSWORD,
            )
            await page.wait_for_load_state("domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(2_000)
            print(f"  Post-reload: {await page.title()}", flush=True)
        except Exception:
            print("→ Sin password gate (¿ya autenticado o reporte abierto?)", flush=True)

        try:
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass

        # ── LOOP DE ESPERA (NO reload agresivo) ──────────────────────────────
        print(f"→ Esperando .gz (hasta {MAX_WAIT_SECONDS}s, sin recargas agresivas)...", flush=True)
        deadline = time.time() + MAX_WAIT_SECONDS
        last_reload = time.time()
        validated = None

        while time.time() < deadline:
            if gz["url"]:
                data, signal, size = await fetch_gz(page, gz["url"])
                print(f"  señal={signal}  size={size}B", flush=True)
                if signal > 0:
                    validated = data
                    print(f"✓ Snapshot CON datos (señal={signal}).", flush=True)
                    break
                print("⚠ Snapshot vacío (regenerando). Sigo esperando un .gz nuevo...", flush=True)
                gz["url"] = None

            if (time.time() - last_reload) > STALL_RELOAD_SECONDS:
                rem = int(deadline - time.time())
                print(f"→ Stall > {STALL_RELOAD_SECONDS}s. Reload suave único. (quedan {rem}s)", flush=True)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=45_000)
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                except Exception:
                    pass
                last_reload = time.time()

            await page.wait_for_timeout(POLL_INTERVAL_MS)

        # ── Debug -> debug_output/ (el workflow lo sube como artifact) ───────
        try:
            await page.screenshot(path=str(DEBUG_DIR / "final.png"))
        except Exception:
            pass
        try:
            (DEBUG_DIR / "page.html").write_text((await page.content())[:50_000])
        except Exception:
            pass
        (DEBUG_DIR / "responses.txt").write_text("\n".join(all_responses))
        (DEBUG_DIR / "snapshot_statuses.txt").write_text("\n".join(snapshot_statuses))

        await browser.close()

    if validated is None:
        diag = "\n".join(snapshot_statuses[-10:]) or "(sin requests a /snapshot)"
        raise RuntimeError(
            f"No se obtuvo un .gz con datos en {MAX_WAIT_SECONDS}s tras "
            f"{len(all_responses)} responses.\n"
            f"Últimos estados del endpoint snapshot:\n{diag}\n"
            f"502 persistente = Meltwater bloquea la IP de salida (datacenter)."
        )
    return validated


def post_to_worker(snapshot: dict) -> int:
    """POSTea el JSON completo al endpoint /ingest del Worker."""
    print("→ POST a Cloudflare Worker (/ingest)...", flush=True)
    payload = json.dumps(snapshot).encode("utf-8")
    print(f"  payload: {len(payload)} bytes", flush=True)
    req = urllib.request.Request(
        WORKER_INGEST_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "x-sync-token": SYNC_TOKEN,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read().decode("utf-8", errors="replace")[:400]
            print(f"✓ Worker respondió {resp.status}: {body}", flush=True)
            return resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:400]
        print(f"✗ Worker respondió {e.code}: {body}", flush=True)
        # 422 = capture vacío (el Worker protege 'latest'); 401 = token mal.
        raise RuntimeError(f"Worker devolvió {e.code}: {body}")


async def main():
    try:
        if JITTER_SECONDS > 0:
            d = random.randint(0, JITTER_SECONDS)
            print(f"→ Jitter: durmiendo {d}s antes de arrancar...", flush=True)
            await asyncio.sleep(d)

        snapshot = await capture_snapshot()
        status = post_to_worker(snapshot)
        if status not in (200, 202):
            sys.exit(f"Worker devolvió status inesperado: {status}")
        print("✓ Pipeline completo OK.", flush=True)
    except SystemExit:
        raise
    except Exception as e:
        print(f"\n✗ ERROR: {e}", flush=True)
        traceback.print_exc()
        (DEBUG_DIR / "error.txt").write_text(f"{e}\n\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
