import asyncio
import json
import time

from helpers.enrich_data import enrich_lob_events, enrich_market_orders, enrich_raw
from helpers.util import preprocess

n_produced = 0

async def produce_messages(ws, raw_producer, normalised_producer, trades_producer, normalise):
    global n_produced
    asyncio.create_task(monitor_productions())
    async for msg in ws:
        msg = await preprocess(msg, ws)
        await raw_producer.produce(str(time.time()), msg)

        enriched = enrich_raw(json.loads(msg))
        normalised_data = normalise(enriched)
        lob_events = normalised_data['lob_events']
        market_orders = normalised_data['market_orders']

        enrich_lob_events(lob_events)
        enrich_market_orders(market_orders)
        print(lob_events)

        for event in lob_events:
            await normalised_producer.produce(str(time.time()), event)
        for trade in market_orders:
            await trades_producer.produce(str(time.time()), trade)
        n_produced += 1

async def produce_message(message, raw_producer, normalised_producer, trades_producer, normalise):
    await raw_producer.produce(str(time.time()), message)

    enriched = enrich_raw(json.loads(message))
    normalised_data = normalise(enriched)
    lob_events = normalised_data['lob_events']
    market_orders = normalised_data['market_orders']

    enrich_lob_events(lob_events)
    enrich_market_orders(market_orders)

    for event in lob_events:
        await normalised_producer.produce(str(time.time()), event)

async def monitor_productions():
    while True:
        print(f"Total Messages Processed: {n_produced}")
        await asyncio.sleep(1)