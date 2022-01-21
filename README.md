# l3_data_collection
Data collection, enhancement, and metrics calculation. Currently, data is retrieved and normalised from Okex, Kraken, Phemex, FTX, Kucoin, and Huobi. There are multiple implementations of the order book (NumPy table, dictionary) to maximise compatibility.


## Dependencies

All required dependencies can be installed from `requirements.txt`.
```
pip install -r requirements.txt
```

## Usage

Configure symbols in `symbols/[exchange].ini`, and then run `main.py [exchange1, exchange2, ...]` to get normalised data from the desired exchanges. Alternatively, modify `config.ini` with the desired exchanges and run `main.py` without an argument. Modify the `dump` function in `normaliser/normaliser.py` to configure the desired outputs.
