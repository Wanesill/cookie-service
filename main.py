#!/usr/bin/env python3
"""Daemon-сервис для разблокировки Avito cookies.

Мониторит папку cookies/ на появление JSON-файлов, проверяет каждый на бан IP,
и если бан есть — решает GeeTest v4 капчу через CapSolver.

Использование:
    python main.py                        # с config.yaml в текущей директории
    python main.py --config /path/to.yaml # с указанным конфигом
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

import httpx
import orjson
import yaml
from playwright.async_api import Page, async_playwright

try:
    from playwright_stealth import stealth_async
except ImportError:
    stealth_async = None  # type: ignore[assignment]

log = logging.getLogger("cookie-service")

# --- Константы ---

AVITO_URL = "https://www.avito.ru"
GEETEST_CAPTCHA_ID_DEFAULT = "2d9c743cf7d63dbc9db578a608196bcd"
CAPSOLVER_API = "https://api.capsolver.com"
TWOCAPTCHA_API = "https://api.2captcha.com"

# --- Defaults ---

DEFAULTS = {
    "cookies_folder": "./cookies",
    "check_interval": 10,
    "headed": False,
    "geetest_captcha_id": GEETEST_CAPTCHA_ID_DEFAULT,
    "page_load_timeout": 30_000,
    "captcha_poll_interval": 3.0,
    "captcha_max_poll_time": 120.0,
    "captcha_service": "capsolver",
}


# ═══════════════════════════════════════════════════════════════
# Секция A: Конфиг + логирование
# ═══════════════════════════════════════════════════════════════


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(path: str = "config.yaml") -> dict:
    p = Path(path)
    if not p.exists():
        log.error("Конфиг не найден: %s", p)
        sys.exit(1)

    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    config = {**DEFAULTS, **raw}

    service = config.get("captcha_service", "capsolver")
    if service not in ("capsolver", "2captcha"):
        log.error("captcha_service должен быть 'capsolver' или '2captcha', получено: '%s'", service)
        sys.exit(1)

    if service == "capsolver" and not config.get("capsolver_api_key"):
        log.error("capsolver_api_key не указан в %s", p)
        sys.exit(1)

    if service == "2captcha" and not config.get("twocaptcha_api_key"):
        log.error("twocaptcha_api_key не указан в %s", p)
        sys.exit(1)

    return config


# ═══════════════════════════════════════════════════════════════
# Секция B: CapSolver
# ═══════════════════════════════════════════════════════════════


async def capsolver_balance(api_key: str) -> float | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{CAPSOLVER_API}/getBalance",
            json={"clientKey": api_key},
        )
        data = resp.json()

    if data.get("errorId", 0) != 0:
        log.error("CapSolver balance error: %s", data.get("errorDescription", "unknown"))
        return None

    balance = data.get("balance", 0)
    log.info("CapSolver баланс: $%.4f", balance)
    return balance


async def capsolver_solve(api_key: str, captcha_id: str, config: dict) -> dict:
    poll_interval = config["captcha_poll_interval"]
    max_poll_time = config["captcha_max_poll_time"]

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{CAPSOLVER_API}/createTask",
            json={
                "clientKey": api_key,
                "task": {
                    "type": "GeeTestTaskProxyLess",
                    "websiteURL": AVITO_URL,
                    "captchaId": captcha_id,
                },
            },
        )
        data = resp.json()

    if data.get("errorId", 0) != 0:
        log.error("CapSolver createTask error: %s", data.get("errorDescription"))
        return {}

    task_id = data["taskId"]
    log.info("  Задача: %s", task_id)

    start = time.monotonic()
    async with httpx.AsyncClient(timeout=30) as client:
        while (time.monotonic() - start) < max_poll_time:
            await asyncio.sleep(poll_interval)

            resp = await client.post(
                f"{CAPSOLVER_API}/getTaskResult",
                json={"clientKey": api_key, "taskId": task_id},
            )
            data = resp.json()

            if data.get("errorId", 0) != 0:
                log.error("CapSolver poll error: %s", data.get("errorDescription"))
                return {}

            if data.get("status") == "ready":
                elapsed = time.monotonic() - start
                log.info("  Решено за %.1fс", elapsed)
                return data["solution"]

    log.error("  Таймаут CapSolver (%.0fс)", max_poll_time)
    return {}


# ═══════════════════════════════════════════════════════════════
# Секция B2: 2captcha
# ═══════════════════════════════════════════════════════════════


async def twocaptcha_balance(api_key: str) -> float | None:
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(
            f"{TWOCAPTCHA_API}/getBalance",
            json={"clientKey": api_key},
        )
        data = resp.json()

    if data.get("errorId", 0) != 0:
        log.error("2captcha balance error: %s", data.get("errorDescription", "unknown"))
        return None

    balance = data.get("balance", 0)
    log.info("2captcha баланс: $%.4f", balance)
    return balance


async def twocaptcha_solve(api_key: str, captcha_id: str, config: dict) -> dict:
    poll_interval = config["captcha_poll_interval"]
    max_poll_time = config["captcha_max_poll_time"]

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{TWOCAPTCHA_API}/createTask",
            json={
                "clientKey": api_key,
                "task": {
                    "type": "GeeTestTaskProxyless",
                    "websiteURL": AVITO_URL,
                    "gt": captcha_id,
                    "version": 4,
                },
            },
        )
        data = resp.json()

    if data.get("errorId", 0) != 0:
        log.error("2captcha createTask error: %s", data.get("errorDescription"))
        return {}

    task_id = data["taskId"]
    log.info("  Задача: %s", task_id)

    start = time.monotonic()
    async with httpx.AsyncClient(timeout=30) as client:
        while (time.monotonic() - start) < max_poll_time:
            await asyncio.sleep(poll_interval)

            resp = await client.post(
                f"{TWOCAPTCHA_API}/getTaskResult",
                json={"clientKey": api_key, "taskId": task_id},
            )
            data = resp.json()

            if data.get("errorId", 0) != 0:
                log.error("2captcha poll error: %s", data.get("errorDescription"))
                return {}

            if data.get("status") == "ready":
                elapsed = time.monotonic() - start
                log.info("  Решено за %.1fс", elapsed)
                return data["solution"]

    log.error("  Таймаут 2captcha (%.0fс)", max_poll_time)
    return {}


# ═══════════════════════════════════════════════════════════════
# Секция C: Хелперы
# ═══════════════════════════════════════════════════════════════


def load_cookies(file_path: Path) -> tuple[list[dict], str]:
    """Загружает куки из файла → формат Playwright. Возвращает (pw_cookies, user_agent)."""
    data = orjson.loads(file_path.read_bytes())
    user_agent: str = data["user_agent"]
    raw_cookies: dict[str, str] = data["cookies"]

    pw_cookies = []
    for name, value in raw_cookies.items():
        if value is None:
            continue
        pw_cookies.append(
            {
                "name": name,
                "value": value,
                "domain": ".avito.ru",
                "path": "/",
                "httpOnly": False,
                "secure": True,
                "sameSite": "Lax",
            }
        )
    return pw_cookies, user_agent



async def get_captcha_type(page: Page) -> dict:
    """Вызывает /web/1/firewallCaptcha/get через JS в контексте страницы."""
    result = await page.evaluate("""
        async () => {
            try {
                const resp = await fetch('/web/1/firewallCaptcha/get', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ refreshAvitoCaptcha: false }),
                });
                const data = await resp.json();
                return data.result?.captcha || {};
            } catch (e) {
                return { error: e.message };
            }
        }
    """)
    return result or {}


async def verify_captcha(page: Page, geetest_response: dict) -> dict:
    """Вызывает /web/1/firewallCaptcha/verify через JS в контексте страницы."""
    result = await page.evaluate(
        """
        async (geetestResponse) => {
            try {
                const resp = await fetch('/web/1/firewallCaptcha/verify', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        captcha: '',
                        hCaptchaResponse: '',
                        ...geetestResponse,
                    }),
                });
                const data = await resp.json();
                return data.result || {};
            } catch (e) {
                return { error: e.message };
            }
        }
    """,
        geetest_response,
    )
    return result or {}


# ═══════════════════════════════════════════════════════════════
# Секция D: Обработка одного файла
# ═══════════════════════════════════════════════════════════════


async def process_cookie_file(file_path: Path, config: dict) -> bool:
    """Обрабатывает один cookie-файл. Возвращает True при успехе."""
    service = config["captcha_service"]
    api_key = config["twocaptcha_api_key"] if service == "2captcha" else config["capsolver_api_key"]
    headed = config["headed"]
    page_load_timeout = config["page_load_timeout"]

    log.info("Обработка: %s", file_path.name)

    # Загрузка куков
    try:
        pw_cookies, user_agent = load_cookies(file_path)
    except Exception as e:
        log.error("  Ошибка чтения куков: %s", e)
        return False

    log.info("  Куки загружены (%d шт.)", len(pw_cookies))

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=not headed,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )

        context = await browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1920, "height": 1080},
            locale="ru-RU",
        )

        page = await context.new_page()
        if stealth_async is not None:
            await stealth_async(page)

        await context.add_cookies(pw_cookies)

        # [1] Открываем Авито
        log.info("[1/6] Загрузка %s", AVITO_URL)
        try:
            resp = await page.goto(
                AVITO_URL,
                wait_until="domcontentloaded",
                timeout=page_load_timeout,
            )
            status = resp.status if resp else "?"
            log.info("  Статус: %s", status)
        except Exception as e:
            log.error("  Ошибка загрузки: %s", e)
            await browser.close()
            return False

        await asyncio.sleep(2)

        title = await page.title()
        log.info("  Title: %s", title)

        is_blocked = (
            "ограничен" in title.lower()
            or "firewall" in (await page.content()).lower()
            or status in (403, 429)
        )

        if not is_blocked:
            log.info("  Страница НЕ заблокирована — куки в порядке")
            await browser.close()
            return True

        log.info("  → Блокировка обнаружена")

        # [2] Запрашиваем тип капчи
        log.info("[2/5] Запрос типа капчи → /web/1/firewallCaptcha/get")
        captcha_info = await get_captcha_type(page)
        captcha_type = captcha_info.get("type", "unknown")
        log.info("  Тип: %s", captcha_type)

        if captcha_type != "geeTest":
            log.error("  Неподдерживаемый тип капчи: %s", captcha_type)
            await browser.close()
            return False

        # [3] Решаем GeeTest v4
        service_name = "2captcha" if service == "2captcha" else "CapSolver"
        log.info("[3/5] Решение GeeTest v4 через %s", service_name)
        captcha_id = config["geetest_captcha_id"]
        if service == "2captcha":
            solution = await twocaptcha_solve(api_key, captcha_id, config)
        else:
            solution = await capsolver_solve(api_key, captcha_id, config)

        if not solution:
            log.error("  Капча не решена")
            await browser.close()
            return False

        geetest_response = {
            "captcha_id": captcha_id,
            "lot_number": solution.get("lot_number", ""),
            "pass_token": solution.get("pass_token", ""),
            "gen_time": solution.get("gen_time", ""),
            "captcha_output": solution.get("captcha_output", ""),
        }

        log.info("  lot_number: %.40s...", geetest_response["lot_number"])
        log.info("  pass_token: %.40s...", geetest_response["pass_token"])

        # [4] Верификация
        log.info("[4/5] Верификация → /web/1/firewallCaptcha/verify")
        verify_result = await verify_captcha(page, geetest_response)
        log.info("  Ответ: %s", verify_result)

        verified = verify_result.get("verified", False)

        if verified:
            log.info("  Капча пройдена!")
        else:
            log.warning("  Верификация не прошла")

        await browser.close()

    return verified


# ═══════════════════════════════════════════════════════════════
# Секция E: Daemon loop
# ═══════════════════════════════════════════════════════════════


async def run_daemon(config: dict) -> None:
    cookies_folder = Path(config["cookies_folder"])
    interval = config["check_interval"]

    cookies_folder.mkdir(parents=True, exist_ok=True)

    log.info("Daemon запущен. Папка: %s, интервал: %dс", cookies_folder, interval)

    # Проверка баланса при старте
    if config["captcha_service"] == "2captcha":
        await twocaptcha_balance(config["twocaptcha_api_key"])
    else:
        await capsolver_balance(config["capsolver_api_key"])

    while True:
        files = sorted(cookies_folder.glob("*.json"), key=lambda f: f.stat().st_mtime)

        for file_path in files:
            log.info("=" * 60)
            try:
                success = await process_cookie_file(file_path, config)
            except Exception:
                log.exception("Необработанная ошибка при обработке %s", file_path.name)
                success = False

            if success:
                log.info("→ %s: OK", file_path.name)
            else:
                log.info("→ %s: FAIL", file_path.name)

        if files:
            log.info("=" * 60)

        await asyncio.sleep(interval)


async def async_main(config_path: str) -> None:
    setup_logging()
    config = load_config(config_path)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown(sig: signal.Signals) -> None:
        log.info("Получен сигнал %s, завершение...", sig.name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    daemon_task = asyncio.create_task(run_daemon(config))

    # Ждём либо завершения daemon (ошибка), либо сигнала остановки
    done, _ = await asyncio.wait(
        [daemon_task, asyncio.create_task(stop_event.wait())],
        return_when=asyncio.FIRST_COMPLETED,
    )

    if daemon_task in done:
        # Daemon завершился с ошибкой
        daemon_task.result()  # поднимет исключение если было
    else:
        # Graceful shutdown
        daemon_task.cancel()
        try:
            await daemon_task
        except asyncio.CancelledError:
            pass

    log.info("Daemon остановлен.")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Cookie Service Daemon")
    parser.add_argument("--config", default="config.yaml", help="Путь к config.yaml")
    args = parser.parse_args()

    asyncio.run(async_main(args.config))


if __name__ == "__main__":
    main()
