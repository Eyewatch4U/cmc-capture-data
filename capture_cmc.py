"""
Meltwater Auto-Capture CMC (GitHub Actions Edition)
==================================================
Igual que el bookmarklet manual, PERO ahora valida el contenido:
  1. Abre la página de Meltwater en Chrome real
  2. Ingresa la contraseña si aparece el prompt
  3. Espera a que la página haga sus XHR
  4. Captura la URL .json.gz fresca del Network tab
  5. *** BAJA el .gz, lo descomprime y verifica que trae datos reales ***
     - Si viene vacío (snapshot regenerándose) => reload y reintenta
     - Solo si hay señal > 0 hace el POST a Make.com
  6. POST de esa URL al webhook de Make.com → fin

Motivo del cambio: Meltwater a veces sirve el insightPageSnapshot.json.gz con
la estructura completa (insightPage.tabs) pero las agregaciones en CERO mientras
regenera el snapshot. La versión anterior solo chequeaba status 200 y mandaba la
URL igual, por lo que el worker guardaba un snapshot vacío y pisaba 'latest'.
"""

import asyncio
import os
import sys
import json
import gzip
import urllib.request
import traceback
from pathlib import Path
from playwright.async_api import async_playwright

MELTWATER_URL = os.environ["MELTWATER_URL"]
MELTWATER_PASSWORD = os.environ.get("MELTWATER_PASSWORD", "")
MAKE_WEBHOOK = os.environ["MAKE_WEBHOOK_URL"]

# Cuántos reloads extra hacemos si el .gz aparece pero viene VACÍO
MAX_VALIDATE_RELOADS = int(os.environ.get("MAX_VALIDATE_RELOADS", "4"))

DEBUG_DIR = Path("debug_output")
DEBUG_DIR.mkdir(exist_ok=True)


def compute_signal(data: dict) -> int:
    """
    Suma de 'señal' de datos reales recorriendo tabs/rows/cards/fragments.
    Si da 0, el snapshot llegó sin agregaciones (vacío / regenerándose).
    Refleja exactamente lo que hace que el worker guarde un snapshot vacío.
    """
    ip = (data or {}).get("insightPage") or {}
    tabs = ip.get("tabs") or []
    signal = 0
    for t in tabs:
        for row in (t.get("rows") or []):
            for card in (row.get("cards") or []):
                for frag in (card.get("fragments") or []):
                    d = frag.get("data") or {}
                    hits = d.get("hits")
                    if isinstance(hits, list):
                        signal += len(hits)
                    tot = d.get("total")
                    if isinstance(tot, (int, float)):
                        signal += tot
                    date = (d.get("aggs") or {}).get("date") or {}
                    for v in (date.get("values") or []):
                        signal += ((v.get("counts") or {}).get("doc") or 0)
    return signal


async def fetch_gz_signal(page, url: str):
    """Baja el .gz (en la misma sesión), descomprime y devuelve (signal, size_bytes)."""
    resp = await page.request.get(url, timeout=45_000)
    if not resp.ok:
        print(f"  ⚠ GET gz status {resp.status}", flush=True)
        return -1, 0
    body = await resp.body()
    size = len(body)
    # Descomprimir si es gzip (magic 1f 8b). Si Playwright ya lo decodificó, usar tal cual.
    if len(body) >= 2 and body[0] == 0x1F and body[1] == 0x8B:
        try:
            body = gzip.decompress(body)
        except Exception as e:
            print(f"  ⚠ gzip.decompress falló: {e}", flush=True)
            return -1, size
    try:
        data = json.loads(body.decode("utf-8", errors="replace"))
    except Exception as e:
        print(f"  ⚠ json.loads falló: {e}", flush=True)
        return -1, size
    return compute_signal(data), size


async def capture_gz_url() -> str:
    """Abre Meltwater, ingresa el password, captura la URL .gz y VALIDA su contenido."""
    gz_url_holder = {"url": None}
    all_responses = []  # Para diagnóstico

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
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
            extra_http_headers={
                "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            },
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        page = await context.new_page()

        def on_response(response):
            all_responses.append(f"{response.status} {response.url[:120]}")
            if "insightPageSnapshot.json.gz" in response.url and response.status == 200:
                gz_url_holder["url"] = response.url
                print(f"✓ URL .gz capturada", flush=True)

        page.on("response", on_response)

        print(f"→ Navegando a Meltwater...", flush=True)
        await page.goto(MELTWATER_URL, wait_until="domcontentloaded", timeout=60_000)
        print(f"  page.url = {page.url}", flush=True)
        print(f"  page.title = {await page.title()}", flush=True)

        print(f"→ Esperando UI (password gate o reporte)...", flush=True)
        passcode_present = False
        try:
            await page.wait_for_selector("#passcode", timeout=20_000, state="attached")
            passcode_present = True
        except Exception:
            passcode_present = False

        if passcode_present:
            print(f"→ Página de password detectada (flux-textfield#passcode)", flush=True)
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
            print(f"→ Password enviado. Esperando reload...", flush=True)
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=30_000)
                await page.wait_for_timeout(2_000)
                print(f"  Post-reload: {await page.title()}", flush=True)
            except Exception as e:
                print(f"  ⚠ wait post-reload: {e}", flush=True)
        else:
            print(f"→ Sin página de password (¿ya autenticado?)", flush=True)

        print(f"→ Esperando network idle + XHR...", flush=True)
        try:
            await page.wait_for_load_state("networkidle", timeout=45_000)
        except Exception as e:
            print(f"  ⚠ networkidle timeout: {e}", flush=True)
        await page.wait_for_timeout(8_000)

        # ── LOOP CAPTURA + VALIDACIÓN ────────────────────────────────────────
        # Reintenta si: (a) no apareció la URL .gz, o (b) apareció pero VINO VACÍA.
        validated_url = None
        for attempt in range(0, MAX_VALIDATE_RELOADS + 1):
            # ¿ya tenemos URL? si no, forzar un reload (salvo el primer intento que ya esperó)
            if not gz_url_holder["url"]:
                if attempt == 0:
                    # primer intento sin url -> dar una vuelta de reload
                    pass
                print(f"→ .gz no capturado. Reload {attempt + 1}/{MAX_VALIDATE_RELOADS + 1}...", flush=True)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=45_000)
                    await page.wait_for_load_state("networkidle", timeout=45_000)
                except Exception:
                    pass
                await page.wait_for_timeout(8_000)
                if not gz_url_holder["url"]:
                    continue

            # tenemos URL -> validar contenido
            url = gz_url_holder["url"]
            print(f"→ Validando contenido del .gz (intento {attempt + 1})...", flush=True)
            signal, size = await fetch_gz_signal(page, url)
            print(f"  señal={signal}  size={size}B", flush=True)

            if signal > 0:
                validated_url = url
                print(f"✓ Snapshot CON datos (señal={signal}).", flush=True)
                break

            # vacío -> forzar regeneración con reload y volver a capturar
            print(f"⚠ Snapshot VACÍO (señal={signal}). Reload para forzar gz nuevo...", flush=True)
            gz_url_holder["url"] = None
            if attempt < MAX_VALIDATE_RELOADS:
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=45_000)
                    await page.wait_for_load_state("networkidle", timeout=45_000)
                except Exception:
                    pass
                await page.wait_for_timeout(8_000)

        # Diagnóstico
        await page.screenshot(path=str(DEBUG_DIR / "final.png"))
        html = await page.content()
        (DEBUG_DIR / "page.html").write_text(html[:50_000])
        (DEBUG_DIR / "responses.txt").write_text("\n".join(all_responses))

        await browser.close()

    if not gz_url_holder["url"] and not validated_url:
        raise RuntimeError(
            f"No se capturó URL .gz después de {len(all_responses)} responses. "
            f"Ver debug_output/ para detalles."
        )
    if not validated_url:
        raise RuntimeError(
            f"Se capturó la URL .gz pero SIEMPRE vino VACÍA tras "
            f"{MAX_VALIDATE_RELOADS + 1} intentos (snapshot regenerándose). "
            f"NO se envía a Make para no pisar 'latest'."
        )
    return validated_url


def post_to_make(gz_url: str) -> int:
    print(f"→ POST a Make.com...", flush=True)
    payload = json.dumps({"url": gz_url, "sync": True}).encode("utf-8")
    req = urllib.request.Request(
        MAKE_WEBHOOK,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        status = resp.status
        body = resp.read().decode("utf-8", errors="replace")[:200]
        print(f"✓ Make.com respondió {status}: {body}", flush=True)
        return status


async def main():
    try:
        gz_url = await capture_gz_url()
        status = post_to_make(gz_url)
        if status not in (200, 202):
            sys.exit(f"Make.com devolvió status inesperado: {status}")
        print("✓ Pipeline completo OK.", flush=True)
    except Exception as e:
        print(f"\n✗ ERROR: {e}", flush=True)
        traceback.print_exc()
        (DEBUG_DIR / "error.txt").write_text(f"{e}\n\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
