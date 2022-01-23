# l3_data_collection
Data collection, enhancement, and metrics calculation. Currently, data is retrieved and normalised from Okex, Kraken, Phemex, FTX, Kucoin, and Huobi. There are multiple implementations of the order book (NumPy table, dictionary) to maximise compatibility.

This branch separates data collection from every exchange into microservices. 

## Dependencies

All required dependencies can be installed from `requirements.txt`.
```
pip install -r requirements.txt
```

## Usage

For each exchange, go to the `src/[exchange]` directory and configure the `[exchange].ini` config file for desired ticker symbols. Then run `main.py` in the same directory to start the microservice.