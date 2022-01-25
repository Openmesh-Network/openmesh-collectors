# l3_data_collection
Data collection, enhancement, and metrics calculation. Currently, data is retrieved and normalised from Okex, Kraken, Phemex, FTX, Kucoin, and Huobi. There are multiple implementations of the order book (NumPy table, dictionary) to maximise compatibility.

This branch separates data collection from every exchange into microservices. 

## Background
This project is an MVP for GDA Fund's L3 Atom Open Data Initiative. It is a collection of data from multiple exchanges, and is intended to be used as a base for a future framework for L3 Atom. For ease of future modification, each exchange is segmented into distinct folders and the logic/implementation is contained therein. Data is collected and normalised into standardised table formats, as outlined in GDA's [medium article](https://gdafund.medium.com/open-crypto-data-initiative-1e096ccbf0e6) for L3 Atom.
## Install

All required dependencies can be installed from `requirements.txt`.
```
pip install -r requirements.txt
```

## Usage

Enter the folder of the desired exchange, and run `main.py` to begin the data collection and normalisation for that given exchange. Modify the `_dump` function in `normaliser.py` to change the output format.