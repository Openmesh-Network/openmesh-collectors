import asyncio
import json
import time

from helpers.enrich_data import enrich_lob_events, enrich_market_orders, enrich_raw
from helpers.util import preprocess

n_raw_produced = 0
n_normalised_produced = 0
n_trades_produced = 0

async def produce_messages(ws, raw_producer, normalised_producer, trades_producer, normalise):
    global n_raw_produced, n_normalised_produced, n_trades_produced, quote_no
    asyncio.create_task(monitor_productions())
    async for msg in ws:
        msg_dict = await preprocess(msg, ws) 
        tasks = []
        tasks.append(raw_producer.produce(str(time.time()), msg_dict))
        n_raw_produced += 1

        enriched = enrich_raw(msg_dict)
        normalised_data = normalise(enriched)
        lob_events = normalised_data['lob_events']
        market_orders = normalised_data['market_orders']

        enrich_lob_events(lob_events)
        enrich_market_orders(market_orders)

        if lob_events and len(lob_events) > 1:
            tasks.append(asyncio.create_task(normalised_producer.pipeline_produce('quote_no', lob_events)))
        elif lob_events:
            tasks.append(asyncio.create_task(normalised_producer.produce(lob_events[0]['quote_no'], lob_events[0])))
        n_normalised_produced += len(lob_events) if lob_events else 0

        if market_orders and len(market_orders) > 1:
            tasks.append(asyncio.create_task(trades_producer.pipeline_produce('order_id', market_orders)))
        elif market_orders:
            tasks.append(asyncio.create_task(trades_producer.produce(market_orders[0]['order_id'], market_orders[0])))
        n_trades_produced += len(market_orders) if market_orders else 0

        await asyncio.gather(*tasks)

async def produce_message(message, raw_producer, normalised_producer, trades_producer, normalise):
    message = json.loads(message)
    raw_producer.produce(str(time.time()), message)

    enriched = enrich_raw(message)
    normalised_data = normalise(enriched)
    lob_events = normalised_data['lob_events']
    market_orders = normalised_data['market_orders']

    enrich_lob_events(lob_events)
    enrich_market_orders(market_orders)

    for event in lob_events:
        normalised_producer.produce(str(time.time()), event)

async def monitor_productions():
    while True:
        print(f"Produced (Raw) (Normalised) (Trades): {n_raw_produced} {n_normalised_produced} {n_trades_produced}", flush=True)
        await asyncio.sleep(600)