# l3_data_collection
Data collection, enhancement, and metrics calculation. Currently, data is retrieved and normalised from a variety of different crypto exchanges, including both centralised and decentralised sources. There are multiple implementations of the order book (NumPy table, dictionary) to maximise compatibility.

## Background
This project is an MVP for GDA Fund's L3 Atom Open Data Initiative. It is a collection of data from multiple exchanges, and is intended to be used as a base for a future framework for L3 Atom. For ease of future modification, each exchange is segmented into distinct folders and the logic/implementation is contained therein. Data is collected and normalised into standardised table formats, as outlined in GDA's [medium article](https://gdafund.medium.com/open-crypto-data-initiative-1e096ccbf0e6) for L3 Atom.

Below is a description of how you can recreate our process of setting up the MVP, but if you want to connect to the server we're personally running, see our [documentation](https://www.notion.so/gda-fund/L3-Atom-MVP-Websocket-1f1b691eb7824a948e8680cdb552c8e0). Everything after this point is for developers who are curious in our process -- connecting to the MVP yourself is as simple as connecting to a websocket.

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
Currently still in development. Configure `config.ini` in the root directory to set the address and port of the server, then run `python server.py` to start the server. To start a client, run the command `python client.py ws://<host>:<port>/` in a separate terminal instance. The server supports multiple clients connecting. To terminate the server, hit the Enter key.

After connecting, the client will start accepting console input. To subscribe to a data feed, type `/sub <topic>` into the console input and press enter. To unsubscribe, console input `/unsub <topic>` on the client. To disconnect and exit, console input `/exit` (Ctrl-C if that doesn't work). You may subscribe and unsubscribe as many times as you wish. 

The server will dump its state onto the terminal once every second.

Supported topics (topics are case sensitive):
- `BTCUSD`
- `BTCUSDT`
- `<exchange>` where `exchange` is any Kafka topic with the name `test-<exchange>-raw`
