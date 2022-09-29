import asyncio
from uniswap_helpers.v2_event_subscriber import handle_events
from uniswap_helpers.v3_indicators_collector import collect_indicators
import logging

logging.basicConfig(level=logging.INFO)

# Main function creates event listeners for all the different smart contracts and desired event, and sets up the async loop
async def main():
    tasks = []
    logging.info("Starting collectors")
    logging.info("Starting event listeners")
    tasks.append(handle_events())
    logging.info("Starting indicators collector")
    tasks.append(collect_indicators())
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting by user request.\n")