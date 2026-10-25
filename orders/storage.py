import asyncio
import json
import logging
import random
import uuid
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

log = logging.getLogger(__name__)


def retry_seconds(failures):
    return min(0.2 * 2 ** min(failures, 4), 3.0) + random.uniform(0, 0.1)


class Repository:
    def __init__(self, pool):
        self.pool = pool

    @classmethod
    async def open(cls, database_url):
        for attempt in range(60):
            pool = None
            try:
                pool = await asyncpg.create_pool(
                    database_url, min_size=2, max_size=8,
                    timeout=2, command_timeout=2,
                )
                await pool.execute(Path(__file__).with_name("schema.sql").read_text("utf-8"))
                return cls(pool)
            except (OSError, asyncpg.PostgresError, asyncio.TimeoutError):
                if pool is not None:
                    await pool.close()
                if attempt == 59:
                    raise
                log.warning("Waiting for PostgreSQL")
                await asyncio.sleep(1)

    async def accept(self, body, amount):
        order_id = uuid.uuid4()
        await self.pool.execute(
            "INSERT INTO purchases(id,user_id,items,amount_cents) VALUES($1,$2,$3,$4)",
            order_id, body["user_id"], json.dumps(body["items"]), amount,
        )
        return {"id": str(order_id), "status": "pending"}

    async def get_order(self, order_id):
        try:
            order_id = uuid.UUID(order_id)
        except ValueError:
            return None
        row = await self.pool.fetchrow(
            "SELECT id,phase,amount_cents FROM purchases WHERE id=$1", order_id,
        )
        if row is None:
            return None
        status = row["phase"] if row["phase"] in ("paid", "rejected") else "pending"
        return {"id": str(row["id"]), "status": status, "amount_cents": row["amount_cents"]}

    async def next_order(self, phases):


        return await self.pool.fetchrow(
            "SELECT * FROM purchases WHERE phase=ANY($1::text[]) AND due_at<=now() "
            "ORDER BY due_at,created_at,id LIMIT 1", list(phases),
        )

    async def move(self, order_id, phase):
        await self.pool.execute(
            "UPDATE purchases SET phase=$2,failures=0,due_at=now() WHERE id=$1",
            order_id, phase,
        )

    async def remember_uncertain(self, order_id):

        await self.pool.execute("UPDATE purchases SET phase='uncertain' WHERE id=$1", order_id)

    async def retry_order(self, order, phase):
        await self.pool.execute(
            "UPDATE purchases SET phase=$2,failures=LEAST(failures+1,16),"
            "due_at=now()+$3*interval '1 second' WHERE id=$1",
            order["id"], phase, retry_seconds(order["failures"]),
        )

    async def mark_paid(self, order_id):
        event = {
            "id": str(uuid.uuid4()), "type": "order.paid", "order_id": str(order_id),
            "occurred_at": datetime.now(timezone.utc).isoformat(),
        }
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE purchases SET phase='paid',failures=0 WHERE id=$1", order_id,
                )
                await conn.execute(
                    "INSERT INTO outgoing_events(order_id,payload) VALUES($1,$2) "
                    "ON CONFLICT(order_id) DO NOTHING", order_id, json.dumps(event),
                )

    async def next_event(self):
        return await self.pool.fetchrow(
            "SELECT * FROM outgoing_events WHERE delivered_at IS NULL AND due_at<=now() "
            "ORDER BY due_at,order_id LIMIT 1",
        )

    async def event_delivered(self, order_id):
        await self.pool.execute(
            "UPDATE outgoing_events SET delivered_at=now() WHERE order_id=$1", order_id,
        )

    async def retry_event(self, event):
        await self.pool.execute(
            "UPDATE outgoing_events SET failures=LEAST(failures+1,16),"
            "due_at=now()+$2*interval '1 second' WHERE order_id=$1",
            event["order_id"], retry_seconds(event["failures"]),
        )

    async def credit_delivery(self, event):


        await self.pool.execute(
            "INSERT INTO delivery_credits(event_id,user_id,points) VALUES($1,$2,$3) "
            "ON CONFLICT(event_id) DO NOTHING",
            event["id"], event["user_id"], event["bonus_points"],
        )

    async def bonus(self, user_id):
        points = await self.pool.fetchval(
            "SELECT COALESCE(sum(points),0) FROM delivery_credits WHERE user_id=$1", user_id,
        )
        return {"points": int(points)}
