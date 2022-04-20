import asyncio
import json
import time

from helpers.enrich_data import enrich_lob_events, enrich_market_orders, enrich_raw
from helpers.util import preprocess

async def produce_messages(ws, raw_producer, normalised_producer, trades_producer, normalise):
    async for msg in ws:
        msg = await preprocess(msg, ws)
        raw_producer.produce(str(time.time()), msg)

        enriched = enrich_raw(json.loads(msg))
        lob_events, market_orders = normalise(enriched)
        enrich_lob_events(lob_events)
        enrich_market_orders(market_orders)

        for event in lob_events:
            normalised_producer.produce(str(time.time()), event)
        for trade in market_orders:
            trades_producer.produce(str(time.time()), trade)