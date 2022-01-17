# l3_data_collection
Data collection, enhancement, and metrics calculation.

## Summary
Repository containing code for QuantDAO's JDT data collection task. 

Data is collected from the exchanges listed below, normalised, LOB constructed, and metrics calculated.
1. OKex
2. Phemex
3. FTX
4. Kraken
5. Kucoin
6. Huobi
7. Deribit
8. Bitfinex
*Rayman: We need to choose 6 of these to work on.* 
*Rayman: Richard wants L3 Order Book granularity... But this isn't possible as the Websockets don't provide a proper order_id for each order. Therefore it's impossible to match trades data exactly to order book update data. We wouldn't know how to interpret the number of cancellations and orders within a single order book price level quantity change. Please clarify with him today (18/01/2022), as I won't be awake till pretty late*.

## Dependencies

These packages must be manually installed if not installed already.
```
pip install numpy
```

## Usage

TBC once implementation is better defined.