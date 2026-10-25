import asyncio
import json
import os
import unittest
import uuid

import aiohttp
import asyncpg
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from app import MAX_INTEGER, REPOSITORY, Settings, create_app, order_amount, validate_delivery
from processing import HTTP, Processing


def purchase():
    return {"user_id": "customer", "items": [{"sku": "book", "qty": 2, "price_cents": 150}]}


class InputTests(unittest.TestCase):
    def test_amount(self):
        self.assertEqual(order_amount(purchase()), 300)

    def test_bad_input_and_overflow(self):
        bad_items = [
            {"sku": "a", "qty": True, "price_cents": 100},
            {"sku": "a", "qty": 0, "price_cents": 100},
            {"sku": "a", "qty": 1, "price_cents": -1},
            {"sku": "a", "qty": 2, "price_cents": MAX_INTEGER},
            {"sku": "a", "qty": 1},
        ]
        for item in bad_items:
            with self.subTest(item=item), self.assertRaises(ValueError):
                order_amount({"user_id": "u", "items": [item]})
        with self.assertRaises(ValueError):
            order_amount({"user_id": "u", "items": [
                {"sku": "a", "qty": 1, "price_cents": MAX_INTEGER},
                {"sku": "b", "qty": 1, "price_cents": 1},
            ]})

    def test_delivery_requires_event_id_and_type(self):
        for event in ({"user_id": "u", "bonus_points": 1},
                      {"id": "a", "type": "delivery.confirmed", "user_id": "u", "bonus_points": True}):
            with self.assertRaises(ValueError):
                validate_delivery(event)


@unittest.skipUnless(os.environ.get("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for PostgreSQL tests")
class PostgreSQLTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):

        settings = Settings(os.environ["TEST_DATABASE_URL"], "http://unused", "http://unused")
        self.client = TestClient(TestServer(create_app(settings, workers=False)))
        await self.client.start_server()
        self.repo = self.client.app[REPOSITORY]
        await self.repo.pool.execute("TRUNCATE outgoing_events,purchases,delivery_credits")
        self.mode = "normal"
        self.charges = {}
        self.keys = []
        self.posts = 0
        self.release_attempts = 0
        self.event_ids = []
        self.publish_code = 503

        async def bank_post(request):
            self.posts += 1
            body = await request.json()
            key = request.headers.get("Idempotency-Key")
            self.keys.append((key, body))
            if self.mode == "decline":
                return web.json_response({"error": "declined"}, status=402)
            if self.mode == "offline":
                request.transport.close()
                return web.Response()
            self.charges.setdefault(body["order_id"], {
                **body, "status": "captured", "charge_id": "one-charge",
            })
            if self.mode == "lost":
                request.transport.close()
                return web.Response()
            if self.mode == "slow":
                await asyncio.sleep(0.1)
            return web.json_response(self.charges[body["order_id"]], status=201)

        async def bank_get(request):
            if self.mode == "offline":
                request.transport.close()
                return web.Response()
            charge = self.charges.get(request.query["order_id"])
            return web.json_response([charge] if charge else [])

        async def release(request):
            self.release_attempts += 1
            return web.Response(status=500 if self.release_attempts == 1 else 204)

        async def publish(request):
            self.event_ids.append((await request.json())["id"])
            return web.Response(status=self.publish_code)

        remote = web.Application()
        remote.add_routes([web.post("/payments", bank_post), web.get("/payments", bank_get),
                           web.delete("/reservations/{id}", release), web.post("/events", publish)])
        self.remote = TestServer(remote)
        await self.remote.start_server()
        self.session = aiohttp.ClientSession()
        base = str(self.remote.make_url("/"))
        self.processing = Processing(self.repo, HTTP(self.session), base, base, base)

    async def asyncTearDown(self):
        await self.session.close()
        await self.remote.close()
        await self.client.close()

    async def ready_order(self):
        result = await self.repo.accept(purchase(), 300)
        order_id = uuid.UUID(result["id"])
        await self.repo.move(order_id, "payment")
        return order_id

    async def test_api_accepts_durably_without_neighbours(self):
        response = await self.client.post("/orders", json=purchase())
        self.assertEqual(response.status, 201)
        created = await response.json()
        self.assertEqual(created["status"], "pending")
        fetched = await self.client.get("/orders/" + created["id"])
        self.assertEqual((await fetched.json())["amount_cents"], 300)
        self.assertEqual(self.posts, 0)

    async def test_lost_payment_reply_is_reconciled(self):
        order_id = await self.ready_order()
        self.mode = "lost"
        await self.processing.payment_step()
        self.assertEqual((await self.repo.get_order(str(order_id)))["status"], "paid")
        self.assertEqual(self.posts, 1)
        self.assertEqual(len(self.charges), 1)

    async def test_slow_reply_is_not_a_decline(self):
        order_id = await self.ready_order()
        self.mode = "slow"
        self.processing.payment_timeout = 0.02
        await self.processing.payment_step()
        self.assertEqual((await self.repo.get_order(str(order_id)))["status"], "paid")
        self.assertEqual(self.posts, 1)

    async def test_restart_after_charge_before_status_update(self):
        order_id = await self.ready_order()
        order = await self.repo.next_order(("payment",))
        self.assertEqual(await self.processing.settle(order), "paid")


        replacement = Processing(self.repo, HTTP(self.session),
                                 self.processing.payment_url, "http://unused", "")
        await replacement.payment_step()
        self.assertEqual((await self.repo.get_order(str(order_id)))["status"], "paid")
        self.assertEqual(self.posts, 1)

    async def test_replaying_payment_keeps_key_and_body(self):
        order_id = await self.ready_order()
        original = dict(await self.repo.next_order(("payment",)))
        self.assertEqual(await self.processing.settle(original), "paid")
        self.assertEqual(await self.processing.settle(original), "paid")
        self.assertEqual(self.keys[0], self.keys[1])
        self.assertTrue(self.keys[0][0])
        self.assertEqual(len(self.charges), 1)

    async def test_bank_decline_waits_for_stock_release(self):
        order_id = await self.ready_order()
        self.mode = "decline"
        await self.processing.payment_step()
        await self.processing.stock_step()
        self.assertEqual((await self.repo.get_order(str(order_id)))["status"], "pending")
        await self.repo.pool.execute("UPDATE purchases SET due_at=now() WHERE id=$1", order_id)
        await self.processing.stock_step()
        self.assertEqual((await self.repo.get_order(str(order_id)))["status"], "rejected")

    async def test_paid_and_event_are_atomic(self):
        order_id = await self.ready_order()

        await self.repo.pool.execute(
            "ALTER TABLE outgoing_events ADD CONSTRAINT test_reject CHECK(false) NOT VALID")
        try:
            with self.assertRaises(asyncpg.CheckViolationError):
                await self.repo.mark_paid(order_id)
        finally:
            await self.repo.pool.execute("ALTER TABLE outgoing_events DROP CONSTRAINT test_reject")
        self.assertEqual((await self.repo.get_order(str(order_id)))["status"], "pending")
        self.assertIsNone(await self.repo.next_event())

    async def test_outbox_reuses_event_after_failed_publication(self):
        order_id = await self.ready_order()
        await self.repo.mark_paid(order_id)
        await self.processing.event_step()
        await self.repo.pool.execute("UPDATE outgoing_events SET due_at=now()")
        await self.repo.mark_paid(order_id)
        self.publish_code = 202
        await self.processing.event_step()
        self.assertEqual(len(self.event_ids), 2)
        self.assertEqual(self.event_ids[0], self.event_ids[1])
        self.assertIsNone(await self.repo.next_event())

    async def test_concurrent_delivery_copies_credit_once(self):
        event = {"id": "delivery-1", "type": "delivery.confirmed",
                 "user_id": "customer", "bonus_points": 17}
        responses = await asyncio.gather(*[
            self.client.post("/webhooks/delivery", json=event) for _ in range(20)
        ])
        self.assertTrue(all(response.status == 204 for response in responses))
        for response in responses:
            await response.read()
        self.assertEqual(await self.repo.bonus("customer"), {"points": 17})
        self.assertEqual(await self.repo.bonus("unknown"), {"points": 0})

        from storage import Repository
        replacement = await Repository.open(os.environ["TEST_DATABASE_URL"])
        try:
            await replacement.credit_delivery(event)
            self.assertEqual(await replacement.bonus("customer"), {"points": 17})
        finally:
            await replacement.pool.close()


if __name__ == "__main__":
    unittest.main()
