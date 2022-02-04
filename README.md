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

In order to get the data streaming working on your local machine, you'll also need to install [docker and docker-compose](https://docs.docker.com/desktop/windows/install/), so that the local kafka cluster can be set up.
## Usage
### Kafka
Data is transferred from the exchange websockets to a local kafka cluster, with the symbol as the topic. In order to set this up on your local machine, you'll need to start the docker container for the cluster, which can be done by traversing to the root directory and running `sudo docker-compose up`. This will start the kafka cluster according to the configuration in `docker-compose.yml`, and hence the data pipeline will be established for use on your local machine.

Enter the folder of the desired exchange, and first run the relevant websocket script to begin collecting data from the exchange websocket and begin sending the data down the kafka pipeline. Afterwards, run `main.py` to begin the data normalisation for that given exchange. Modify the `_dump` function in `normaliser.py` to change the output format.

### Websocket Dissemination
Currently still in development. Configure `config.ini` in the root directory to set the address and port of the server, then run `python server.py` to start the server. To start a client, run the command `python client.py ws://<host>:<port>/` in a separate terminal instance. The server supports multiple clients connecting.

After connecting, the client will start accepting console input. To subscribe to the data feed (currently hard coded to pipe ByBit websocket data), type `sub` into the console input and press enter. To unsubscribe, console input `dc` on the client. To disconnect and exit, console input `exit`. You may subscribe and unsubscribe as many times as you wish. 

The server will dump its state onto the terminal once every second.