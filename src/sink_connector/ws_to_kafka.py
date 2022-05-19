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
        raw_producer.produce(str(time.time()), msg_dict)
        n_raw_produced += 1

        enriched = enrich_raw(msg_dict)
        normalised_data = normalise(enriched)
        lob_events = normalised_data['lob_events']
        market_orders = normalised_data['market_orders']

        enrich_lob_events(lob_events)
        enrich_market_orders(market_orders)

        for event in lob_events:
            normalised_producer.produce(str(event['quote_no']), event)
            n_normalised_produced += 1
        for trade in market_orders:
            trades_producer.produce(str(trade['order_id']), trade)
            n_trades_produced += 1

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