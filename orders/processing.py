import asyncio
import json
import logging

import aiohttp

log = logging.getLogger(__name__)


class RemoteError(Exception):
    pass


class BankUnavailable(Exception):
    pass


class HTTP:
    def __init__(self, session):
        self.session = session

    async def call(self, method, url, *, body=None, key=None, timeout=1.0):
        headers = {"Idempotency-Key": key} if key else {}
        try:
            async with self.session.request(
                method, url, json=body, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout), allow_redirects=False,
            ) as response:
                data = bytearray()
                async for chunk in response.content.iter_chunked(65_536):
                    data.extend(chunk)
                    if len(data) > 1_048_576:
                        raise RemoteError("Response too large")
                try:
                    value = json.loads(data) if data else None
                except (ValueError, UnicodeError):
                    value = None
                return response.status, value
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as exc:
            raise RemoteError(str(exc)) from exc


class Processing:
    def __init__(self, repository, http, payment_url, inventory_url, events_url):
        self.db = repository
        self.http = http
        self.payment_url = payment_url.rstrip("/")
        self.inventory_url = inventory_url.rstrip("/")
        self.events_url = events_url.rstrip("/")
        self.payment_timeout = 0.6
        self.lookup_timeout = 0.6
        self.bank_pause = 5.0

    async def stock_step(self):
        order = await self.db.next_order(("stock", "release"))
        if order is None:
            return False
        order_id = order["id"]
        try:
            if order["phase"] == "stock":
                items = json.loads(order["items"])
                code, _ = await self.http.call(
                    "POST", self.inventory_url + "/reservations", timeout=2,
                    body={"order_id": str(order_id), "items": [
                        {"sku": item["sku"], "qty": item["qty"]} for item in items
                    ]},
                )
                if code not in (200, 201):
                    raise RemoteError(f"Inventory reserve: {code}")
                if order["amount_cents"] == 0:
                    await self.db.mark_paid(order_id)
                else:
                    await self.db.move(order_id, "payment")
            else:
                code, _ = await self.http.call(
                    "DELETE", self.inventory_url + "/reservations/" + str(order_id), timeout=2,
                )
                if code != 204:
                    raise RemoteError(f"Inventory release: {code}")
                await self.db.move(order_id, "rejected")
        except RemoteError as exc:
            log.warning("Stock retry %s: %s", order_id, exc)
            await self.db.retry_order(order, order["phase"])
        return True

    async def lookup(self, order):
        code, charges = await self.http.call(
            "GET", self.payment_url + "/payments?order_id=" + str(order["id"]),
            timeout=self.lookup_timeout,
        )
        if code != 200 or not isinstance(charges, list):
            raise RemoteError(f"Payment lookup: {code}")
        if not charges:
            return False
        if len(charges) != 1 or not isinstance(charges[0], dict):
            raise RemoteError("Unexpected number of charges")
        charge = charges[0]
        if (charge.get("order_id") != str(order["id"])
                or charge.get("amount_cents") != order["amount_cents"]
                or charge.get("currency") != "RUB" or charge.get("status") != "captured"):
            raise RemoteError("Payment facts do not match the order")
        return True

    async def settle(self, order):
        if order["phase"] == "uncertain":
            try:
                if await self.lookup(order):
                    return "paid"
            except RemoteError as exc:
                raise BankUnavailable from exc

        await self.db.remember_uncertain(order["id"])
        try:

            timeout = min(self.payment_timeout * 2 ** min(order["failures"], 4), 8.0)
            code, _ = await self.http.call(
                "POST", self.payment_url + "/payments", timeout=timeout,
                key="purchase/" + str(order["id"]),
                body={"order_id": str(order["id"]),
                      "amount_cents": order["amount_cents"], "currency": "RUB"},
            )
        except RemoteError:


            try:
                return "paid" if await self.lookup(order) else "uncertain"
            except RemoteError as exc:
                raise BankUnavailable from exc
        if code in (200, 201):
            return "paid"
        if code == 402:
            return "release"
        log.warning("Payment HTTP %s for %s", code, order["id"])
        return "payment"

    async def payment_step(self):
        order = await self.db.next_order(("payment", "uncertain"))
        if order is None:
            return False
        try:
            result = await self.settle(order)
        except BankUnavailable:
            await self.db.retry_order(order, "uncertain")


            log.warning("Payment unreachable; pausing lane for %s seconds", self.bank_pause)
            await asyncio.sleep(self.bank_pause)
            return True
        if result == "paid":
            await self.db.mark_paid(order["id"])
        elif result == "release":
            await self.db.move(order["id"], "release")
        else:
            await self.db.retry_order(order, result)
        return True

    async def event_step(self):
        if not self.events_url:
            return False
        event = await self.db.next_event()
        if event is None:
            return False
        try:
            code, _ = await self.http.call(
                "POST", self.events_url + "/events", body=json.loads(event["payload"]),
            )
            if code != 202:
                raise RemoteError(f"Event publication: {code}")
        except RemoteError:
            await self.db.retry_event(event)
        else:
            await self.db.event_delivered(event["order_id"])
        return True

    async def run(self, step):
        while True:
            try:
                if not await step():
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Background lane %s failed; will retry", step.__name__)
                await asyncio.sleep(0.25)
