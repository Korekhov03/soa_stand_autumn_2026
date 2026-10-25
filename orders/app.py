import asyncio
import json
import logging
import os
from dataclasses import dataclass

import aiohttp
import asyncpg
from aiohttp import web

from processing import HTTP, Processing
from storage import Repository

log = logging.getLogger(__name__)
MAX_INTEGER = 2**63 - 1
REPOSITORY = web.AppKey("repository", Repository)


@dataclass(frozen=True)
class Settings:
    database_url: str
    payment_url: str
    inventory_url: str
    events_url: str = ""

    @classmethod
    def from_env(cls):
        return cls(os.environ["DATABASE_URL"], os.environ["PAYMENT_URL"],
                   os.environ["INVENTORY_URL"], os.environ.get("EVENTS_URL", ""))


def integer(value, minimum=0):
    return type(value) is int and minimum <= value <= MAX_INTEGER


def text(value):
    return isinstance(value, str) and bool(value.strip())


def order_amount(body):
    if not isinstance(body, dict) or not text(body.get("user_id")):
        raise ValueError("user_id is required")
    items = body.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("items must be a nonempty list")
    amount = 0
    for item in items:
        if (not isinstance(item, dict) or not text(item.get("sku"))
                or not integer(item.get("qty"), 1) or not integer(item.get("price_cents"))):
            raise ValueError("Invalid item")
        amount += item["qty"] * item["price_cents"]
        if amount > MAX_INTEGER:
            raise ValueError("Order amount exceeds BIGINT")
    return amount


def validate_delivery(body):
    if (not isinstance(body, dict) or body.get("type") != "delivery.confirmed"
            or not text(body.get("id")) or not text(body.get("user_id"))
            or not integer(body.get("bonus_points"), 1)):
        raise ValueError("Invalid delivery event")


async def read_json(request):
    try:
        return json.loads(await request.read())
    except (ValueError, UnicodeError, web.HTTPRequestEntityTooLarge) as exc:
        raise ValueError("Invalid JSON body") from exc


@web.middleware
async def errors(request, handler):
    try:
        return await handler(request)
    except ValueError as exc:
        return web.json_response({"error": "bad_request", "message": str(exc)}, status=400)
    except (asyncpg.PostgresError, asyncio.TimeoutError, OSError):
        log.exception("Database operation failed")
        return web.json_response({"error": "database_unavailable"}, status=503)


async def health(request):
    async with asyncio.timeout(0.5):
        await request.app[REPOSITORY].pool.fetchval("SELECT 1")
    return web.json_response({"status": "ok"})


async def create_order(request):
    body = await read_json(request)
    amount = order_amount(body)
    async with asyncio.timeout(0.7):
        result = await request.app[REPOSITORY].accept(body, amount)
    return web.json_response(result, status=201)


async def get_order(request):
    async with asyncio.timeout(0.7):
        result = await request.app[REPOSITORY].get_order(request.match_info["id"])
    if result is None:
        return web.json_response({"error": "not_found"}, status=404)
    return web.json_response(result)


async def delivery(request):
    body = await read_json(request)
    validate_delivery(body)
    async with asyncio.timeout(1.0):
        await request.app[REPOSITORY].credit_delivery(body)
    return web.Response(status=204)


async def bonus(request):
    async with asyncio.timeout(0.7):
        result = await request.app[REPOSITORY].bonus(request.match_info["id"])
    return web.json_response(result)


def create_app(settings, *, workers=True):
    app = web.Application(middlewares=[errors], client_max_size=1_048_576)

    async def lifecycle(app):
        repo = await Repository.open(settings.database_url)
        app[REPOSITORY] = repo
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=8)) as session:
            processing = Processing(repo, HTTP(session), settings.payment_url,
                                    settings.inventory_url, settings.events_url)
            tasks = [asyncio.create_task(processing.run(step)) for step in (
                processing.stock_step, processing.payment_step, processing.event_step,
            )] if workers else []
            try:
                yield
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                await repo.pool.close()

    app.cleanup_ctx.append(lifecycle)
    app.add_routes([
        web.get("/healthz", health), web.post("/orders", create_order),
        web.get("/orders/{id}", get_order), web.post("/webhooks/delivery", delivery),
        web.get("/users/{id}/bonus", bonus),
    ])
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    web.run_app(create_app(Settings.from_env()), host="0.0.0.0",
                port=int(os.environ.get("PORT", "8080")), shutdown_timeout=1, print=None)
