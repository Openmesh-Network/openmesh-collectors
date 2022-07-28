import asyncio
import json
import time

from helpers.enrich_data import enrich_lob_events, enrich_market_orders, enrich_raw
from helpers.util import preprocess

n_raw_produced = 0
n_normalised_produced = 0
n_trades_produced = 0

async def produce_messages(ws, producer, normalise):
    global n_raw_produced, n_normalised_produced, n_trades_produced, quote_no
    asyncio.create_task(monitor_productions())
    pipe = producer.get_pipe()
    async for msg in ws:
        msg_dict = await preprocess(msg, ws) 
        producer.pipeline_produce_raw(pipe, str(time.time()), msg_dict)
        n_raw_produced += 1

        enriched = enrich_raw(msg_dict)
        normalised_data = normalise(enriched)
        lob_events = normalised_data['lob_events']
        market_orders = normalised_data['market_orders']

        enrich_lob_events(lob_events)
        enrich_market_orders(market_orders)

        for lob_event in lob_events:
            producer.pipeline_produce_normalised(pipe, str(time.time()), lob_event)
            n_normalised_produced += 1
        n_normalised_produced += len(lob_events) if lob_events else 0

        for market_order in market_orders:
            producer.pipeline_produce_trade(pipe, str(time.time()), market_order)
            n_trades_produced += 1
        n_trades_produced += len(market_orders) if market_orders else 0

        await pipe.execute()

async def produce_message(message, producer, normalise):
    message = json.loads(message)
    pipe = producer.get_pipe()
    #await raw_producer.produce(str(time.time()), message)
    producer.pipeline_produce_raw(pipe, str(time.time()), message)
    enriched = enrich_raw(message)
    normalised_data = normalise(enriched)
    lob_events = normalised_data['lob_events']
    market_orders = normalised_data['market_orders']

    enrich_lob_events(lob_events)
    enrich_market_orders(market_orders)

    for event in lob_events:
        producer.pipeline_produce_normalised(pipe, str(time.time()), event)

    await pipe.execute()

async def monitor_productions():
    while True:
        print(f"Produced (Raw) (Normalised) (Trades): {n_raw_produced} {n_normalised_produced} {n_trades_produced}", flush=True)
        await asyncio.sleep(600)