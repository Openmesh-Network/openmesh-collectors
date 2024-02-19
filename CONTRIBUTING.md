# Table of Contents
1. [Contributing to Openmesh](#contributing-to-openmesh)
2. [We Develop with Github](#we-develop-with-github)
3. [All Code Changes Happen Through Pull Requests](#all-code-changes-happen-through-pull-requests)
4. [Any contributions you make will be under the MIT Software License](#any-contributions-you-make-will-be-under-the-mit-software-license)
5. [Use a Consistent Coding Style](#use-a-consistent-coding-style)
6. [License](#license)
7. [Local Development](#local-development)
    1. [Setup](#setup)
8. [Adding Additional Sources](#adding-additional-sources)
    1. [Off Chain](#off-chain)
    2. [On Chain](#on-chain)
9. [References](#references)

# Contributing to Openmesh
We love your input! We want to make contributing to this project as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## We Develop with Github
We use github to host code, to track issues and feature requests, as well as accept pull requests.

## All Code Changes Happen Through Pull Requests
Pull requests are the best way to propose changes to the codebase. We actively welcome your pull requests:

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, provide documentation
4. Ensure the test suite passes.
5. Issue the pull request

## Any contributions you make will be under the MIT Software License
In short, when you submit code changes, your submissions are understood to be under the same [MIT License](http://choosealicense.com/licenses/mit/) that covers the project. Feel free to contact the maintainers if that's a concern.

## Use a Consistent Coding Style
We try to follow the [PEP 8 standards](https://peps.python.org/pep-0008/) for our Python, and so consistent styling is important. If you notice that some of our code doesn't adhere to PEP 8, don't try and copy us, it probably wasn't for a good reason and was most likely laziness. To ensure good code quality, use a tool like [flake8](https://flake8.pycqa.org/en/latest/) before you submit your PR.

## License
By contributing, you agree that your contributions will be licensed under its MIT License.

## Local Development
### Setup
Install all needed dependencies via `pip install -r requirements.txt`. This project relies on Kafka for messaging, so if you want to run it locally, Kafka will need to be running. Eventually there will be a version of Openmesh designed to run on a single process for local development, but for now we'll require a Kafka setup. Everything required is included in the `docker-compose.yml` file, so you can run `docker-compose up` to get a local Kafka setup running. This will setup one instance of Zookeeper, 3 brokers, and Schema Registry.

Once Kafka is set up you need to tell the application how it can connect. Make a `.env` file in the `keys/` directory, and fill it in with the following:

If your Kafka requires SASL authentication:
```ini
KAFKA_BOOTSTRAP_SERVERS=<URL of your Kafka Broker(s)>
KAFKA_SASL_KEY=<Username for SASL Authentication>
KAFKA_SASL_SECRET=<Secret for SASL Authentication>

SCHEMA_REGISTRY_URL=<URL of your Schema Registry setup>
SCHEMA_REGISTRY_API_KEY=<Username for Schema Registry Authentication>
SCHEMA_REGISTRY_API_SECRET=<Secret for Schema Registry Authentication>
```

If your Kafka does not require SASL authentication:
```ini
KAFKA_BOOTSTRAP_SERVERS=<URL of your Kafka Broker(s)>

SCHEMA_REGISTRY_URL=<URL of your Schema Registry setup>
```

If you're using the `docker-compose.yml` setup, you can use the following values:

```ini
KAFKA_BOOTSTRAP_SERVERS=localhost:29092,localhost:39092,localhost:49092

SCHEMA_REGISTRY_URL=http://localhost:8081
```

That will be sufficient to ingest and process off-chain data, but to process on-chain data, you will also need access to an ethereum node. Once you have it, add it to the `.env` file as well:

```ini
ETHEREUM_NODE_HTTP_URL=<URL of your Ethereum node via HTTP>
ETHEREUM_NODE_WS_URL=<URL of your Ethereum node via Websockets>
ETHEREUM_NODE_SECRET=<Secret for Ethereum node authentication (if required)>
```

Both HTTP and Websockets are required.

Additionally, in `config.ini`, you'll want to set `num_replications` to be equal to the number of brokers you have running. By default, this is set to 3, which will work with the 3-broker setup in the `docker-compose.yml` file. If you're using a custom configuration with less brokers, make sure to set this value to be, at maximum, the number of brokers you have running.

Symbols take the form of `<base>.<quote>`, e.g. `BTC.USD`, `ETH.USD`, `BTC.EUR` for spots, and `<base>.<quote>-PERP` for perpetual futures, e.g. `BTC.USDT-PERP`. Openmesh supports every symbol listed on the given exchanges.

The entry point for running the application is `runner.py`, which can be used in one of the following ways:

```bash
python3 runner.py connector --source <exchange> --symbol <symbol>
python3 runner.py connector --source <blockchain>
python3 runner.py processor
```

Where the first two options run the raw data collector for the given exchange or blockchain, and the latter runs the Faust stream processor. A Dockerfile is also provided if you want to run the application in a Docker container, just make sure to load in the `.env` file you wrote earlier as a volume.

Note that unlike other data sources, blockchains won't require a `--symbol` argument when running the application, as it will collect data for the entire chain on its own. Individual DEXes, symbol pairs, e.t.c. are handled by the stream processor. Standard orderbook-style exchanges (including some DEXes like DyDx) will require a symbol when specified as a source.

If you want to run the full application, you'll want to have three processes running:

1. The raw data collection for a single source
2. The stream processor
3. A Kafka consumer on one of the normalised topics (e.g. `lob`)

From there, you'll start to see a standardised orderbook feed coming in at low latency. Note that the data will be in Avro, so you'll probably want to have some kind of deserializer to make it human-readable.

The following commands will set up the application with as little steps as possible, assuming you've set up the `.env` file to work with the docker-compose.yaml file:

In one terminal:
```bash
docker-compose up
```

In a second terminal:
```bash
python3 runner.py connector --source coinbase --symbol BTC.USD
```

In a third terminal:
```bash
python3 runner.py processor
```

Finally, install the Kafka cli tools, and run the following in a fourth terminal:
```bash
kafka-avro-console-consumer --topic lob_l3 --bootstrap-server localhost:29092 --property schema.registry.url=http://localhost:8081
```

You can of course run all of these in the background, but this will show you the full logs of what each process is doing. You will start to see Coinbase orderbook data coming in.

## Adding additional sources
### Off-Chain
Let's walk through an example of how an exchange is implemented. All exchanges have the same style and structure, so by diving into how one works, you can read through the others and get a good understanding of how each data source is structured. All off-chain sources are in `openmesh/off_chain/`, each with their own file. Let's go through Coinbase for this example.

```py
from openmesh.data_source import DataFeed
from openmesh.tokens import Symbol
from openmesh.feed import WSConnection, WSEndpoint, AsyncFeed
from yapic import json


class Coinbase(DataFeed):
    name = "coinbase"
    sym_field = 'product_id'
    ws_endpoints = {
        WSEndpoint("wss://ws-feed.pro.coinbase.com"): ["lob_l3", "ticker"]
    }

    ws_channels = {
        "lob_l3": "full",
        # Trade messages are sent in the full channel, but for consistency we keep it separate
        "trades_l3": "full",
        "ticker": "ticker"
    }

    symbols_endpoint = "https://api.pro.coinbase.com/products"
```

Each source extends the `DataFeed` class, which handles all of the back-end connection operations for us. What we need to do is define certain properties that `DataFeed`'s methods can use to handle the connection for our specific exchange. First we define the name, in this case, just `"coinbase"`. This is a unique identifier and appears in a bunch of places. By convention, this should always be lowercase. `sym_field` defines where we can acquire the symbol for each message. This is used for creating Kafka keys -- essentially we want messages for the same symbol to have the same key so that Kafka processes them in order. Since Coinbase always puts the symbol in a field accessed by `'product_id'`, that's what we set the sym_field as. What happens when this isn't the case? If you look in `openmesh/data_source.py`, you'll see that `sym_field` (and another field, `type_field`) are used in the following way:

```py
    @classmethod
    def _get_field(cls, msg, field):
        if field and isinstance(field, str):
            key = msg.get(field, None)
        else:
            try:
                key = msg[field]
            except (IndexError, KeyError):
                logging.warning(
                    f"Key field {field} not found in message")
                key = None
        return key

    # Overwrite this method if the exchange uses a different method of getting the msg symbol or msg type
    @classmethod
    def get_sym_from_msg(cls, msg):
        return cls._get_field(msg, cls.sym_field)

    @classmethod
    def get_type_from_msg(cls, msg):
        return cls._get_field(msg, cls.type_field)

    # TODO: Use this to simplify the standardisation -- this already retrieves the symbol from the data
    @classmethod
    def get_key(cls, message: dict) -> str:
        """
        Returns the key for the provided message
        :param message: Message to get the key for
        :type message: dict
        :return: Key for the message
        :rtype: str
        """
        s = cls.get_sym_from_msg(message)
        t = cls.get_type_from_msg(message)
        if s and t:
            key = f"{cls.name}_{s}_{t}"
            if isinstance(key, str):
                key = key.encode()
            return key
```

You can see that to get the key, `DataFeed` just calls the `get_sym_from_msg()` method, which by default just uses `sym_field`. If we have some custom logic to get the symbol (i.e., if the symbol can be in a different spot depending on the message type), we can just override the `get_sym_from_msg()` method. Indeed, since Coinbase's message types require some custom logic to process, that's what we do:

```py
@classmethod
    def get_type_from_msg(cls, msg):
        if msg['type'] in ('open', 'done', 'change'):
            return 'lob_l3'
        else:
            return msg['type']
```

Going back to the code before, next lets look at the `ws_endpoints` and `ws_channels` properties. 
`ws_endpoints` maps from an endpoint to connect to to a list of feeds to subscribe to on that endpoint.
`ws_channels` effectively maps from Openmesh's nomenclature to the exchange's, e.g. in this case, what we call `'lob_l3'`, Coinbase calls `'full'`, or, in other words, in order to get L3 limit order book data, we need to subscribe to the `'full'` channel over Coinbase's API.
Coinbase is unique in that L3 trade messages are also sent over the `'full'` channel (most exchanges have different channels for order book and trade events), so even though `'trades_l3'` isn't specified in `ws_endpoints`, we show the mapping for it in `ws_channels`.
Finally, `symbols_endpoint` is a HTTP endpoint we can query to get the full list of symbols from the exchange.
Sometimes, this might be a list of endpoints, which means that we have to query every endpoint in the list to get the full list of symbols. This endpoint is queried and the results are passed to a processing function, which we have to define for each exchange (since each exchange has different formatting for their symbols):

```py
    def normalise_symbols(self, sym_list: list) -> dict:
        ret = {}
        for symbol in sym_list:
            if symbol['status'] != 'online':
                continue
            s = symbol['id']
            base, quote = s.split("-")
            normalised_symbol = Symbol(base, quote)
            ret[normalised_symbol] = s
        return ret
```

This method takes in a list of symbols (which is just the return value from making a GET request to the endpoint(s) defined in `symbols_endpoint`), and returns a dictionary that maps from Openmesh's formatting for symbols (i.e. `<base>.<quote>`) to the exchange's formatting for symbols (i.e., in the case of Coinbase, `<base>-<quote>`). This is used to keep a standard symbol format across exchanges, and to allow for us to use a single format when subscribing to symbols across exchanges, even though those exchanges might use different formats themselves.

Finally, we have to define how we actually subscribe to the Coinbase channels we've specified:

```py
async def subscribe(self, conn: AsyncFeed, feeds: list, symbols):
  for feed in feeds:
      msg = json.dumps({
          "type": "subscribe",
          "product_ids": symbols,
          "channels": [self.get_channel_from_feed(feed)]
      })
      await conn.send_data(msg)

def auth(self, conn: WSConnection):
  pass
```

In `subscribe()`, we simply define how, given a list of feeds, symbols, and a reference to the connection, we subscribe to those channels and symbols. In the case of Coinbase, we send one message for each feed, passing in all of the symbols we want as a list. Since Coinbase's API is fully public, we don't need any authentication, and so our `auth()` method (which is called before `subscribe()`) just does nothing. Note that `auth()` does nothing by default, but we restate it here to make it explicit that no authentication is required. Congratulations, we can now start collecting raw data from Coinbase!

Processing the raw data happens completely seperately. The idea is that raw data collection and stream processing are completely decoupled for redundancy and scalability.

Stream processing is contained in `openmesh/stream_processing/`. For processing the data from Coinbase specifically, we enter [`openmesh/stream_processing/standardisers/coinbase.py`](openmesh/stream_processing/standardisers/coinbase.py). Like the raw data collection as contained in `openmesh/off_chain`, with each source having its own class extending `DataFeed`, each source has an equivalent `Standardiser`. You can view the source for `Standardiser` in [`openmesh/stream_processing/standardiser.py`](openmesh/stream_processing/standardiser.py). Essentially it handles the processing of raw data into a standard, consistent format, which obviously differs per exchange. 

Let's have a look at Coinbase's standardiser:

```py
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import Coinbase
from decimal import Decimal
from dateutil import parser
import logging


class CoinbaseStandardiser(Standardiser):
    exchange = Coinbase
```

The only property we have to declare is a reference to the exchange the standardiser is working on. From there it inherits all the other properties we need, including names, symbols, and helper methods. The rest of the code is to handle all the different types of messages that Coinbase can send over its API (at least, out of the feeds we've subscribed to):

```py
    async def _trade(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['size']),
            taker_side=message['side'],
            trade_id=str(message['trade_id']),
            maker_order_id=message['maker_order_id'],
            taker_order_id=message['taker_order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("trades_l3", **msg)

    async def _open(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['remaining_size']),
            side=message['side'],
            order_id=message['order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("lob_l3", **msg)

    async def _done(self, message):
        if 'price' not in message or not message['price']:
            return
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['remaining_size']),
            side=message['side'],
            order_id=message['order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("lob_l3", **msg)

    async def _change(self, message):
        if 'price' not in message or not message['price']:
            return
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['new_size']),
            side=message['side'],
            order_id=message['order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("lob_l3", **msg)

    async def _ticker(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            ask_price=Decimal(message['best_ask']),
            bid_price=Decimal(message['best_bid']),
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp'],
            ask_size=-1,
            bid_size=-1
        )
        await self.send_to_topic("ticker", **msg)

    async def handle_message(self, msg):
        if 'type' in msg:
            if msg['type'] == 'match' or msg['type'] == 'last_match':
                await self._trade(msg)
            elif msg['type'] == 'open':
                await self._open(msg)
            elif msg['type'] == 'done':
                await self._done(msg)
            elif msg['type'] == 'change':
                await self._change(msg)
            elif msg['type'] == 'batch_ticker' or msg['type'] == 'ticker':
                await self._ticker(msg)
            elif msg['type'] == 'received':
                pass
            elif msg['type'] == 'activate':
                pass
            elif msg['type'] == 'subscriptions':
                pass
            else:
                logging.warning(f"{self.id}: Unhandled message: {msg}")
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
```

Each standardiser ***must*** define an asynchronous function `handle_message(self, msg)`. This is what's called for every single message received over the stream. The typical structure will involve filtering out the message into different types and processing each case. To keep things simple let's look at just one type of message:

```py
      async def _trade(self, message):
        msg = dict(
            symbol=self.normalise_symbol(message['product_id']),
            price=Decimal(message['price']),
            size=Decimal(message['size']),
            taker_side=message['side'],
            trade_id=str(message['trade_id']),
            maker_order_id=message['maker_order_id'],
            taker_order_id=message['taker_order_id'],
            event_timestamp=int(parser.isoparse(
                message['time']).timestamp() * 1000),
            atom_timestamp=message['atom_timestamp']
        )
        await self.send_to_topic("trades_l3", **msg)
```

In the case of a `trade` message (recall, received over the `'full'` channel), we just extract the fields we need into a dictionary and send the message over to the `'trades_l3'` topic. Here's what that function does:

```py
      async def send_to_topic(self, feed: str, exchange=None, key_field='symbol', **kwargs):
        """
        Given a feed and arguments, send to the correct topic

        :param feed: The feed to send to
        :type feed: str
        :param kwargs: The arguments to use in the relevant Record
        :type kwargs: dict
        """
        source = exchange if exchange else self.id
        val = self.feed_to_record[feed](**kwargs, exchange=source)
        val.validate()
        await self.normalised_topics[feed].send(
            value=val,
            key=f"{source}_{kwargs[key_field]}"
        )
```

It takes the message we've constructed, converts it into a `Record` to be used for Kafka serialisation, and sends it off to the topic we've specified (in our case, `'trades_l3'`). Let's back up a second, though, and talk about what a `Record` is. For consuming and producing to Kafka we're using a library called [`Faust`](https://faust.readthedocs.io/en/latest/) which lets us define data structures for our messages to take the form of, called `Records`. Each of our topics (`'lob_l3'`, `'trades_l3'`, `'ticker'`, e.t.c.) have their own `Record`, which corresponds with Avro schemas you can find in the ['static'](static/schemas/) folder. By defining these data structures, we can enforce type validation (so if you accidentally convert a value to the wrong type when adding a new exchange, Faust will throw an error) and allow for serialisation of the data. For now, all data produced by one of the Standardisers is automatically serialised to Apache Avro, an efficient data format which preserves schema information and keeps the data compact. Let's take a look at what `Record` we're using for the `'trades_l3'` topic:

```py

class BaseCEXRecord(faust.Record):
    """
    Base record for all other records.

    :param exchange: The exchange the message is sourced from
    :type exchange: str
    :param symbol: The symbol the message is for
    :type symbol: str
    :param event_timestamp: The timestamp of the event (in milliseconds)
    :type event_timestamp: int
    :param atom_timestamp: The timestamp of when the message was received (in microseconds)
    :type atom_timestamp: int
    """
    exchange: str
    symbol: str
    event_timestamp: int
    atom_timestamp: int

class Trade(BaseCEXRecord, serializer='trades'):
    """
    Record for a trade without order IDs.

    :param price: The price of the trade
    :type price: Decimal
    :param size: The size of the trade
    :type size: Decimal
    :param taker_side: The side of the taker, either 'buy' or 'sell'
    :type taker_side: Literal['buy', 'sell']
    :param trade_id: The ID of the trade
    :type trade_id: str
    """
    price: Decimal
    size: Decimal
    taker_side: Literal['buy', 'sell']
    trade_id: str

  class TradeL3(Trade, serializer='trades_l3'):
    """
    Record for a trade with Order IDs.

    :param maker_order_id: The ID of the market maker order
    :type maker_order_id: str
    :param taker_order_id: The ID of the taker order
    :type taker_order_id: str
    """
    maker_order_id: str
    taker_order_id: str

```

So when we extract the values from the raw Coinbase message and create a `Record` object from it, that data is validated and serialised automatically. You can view the full list of `Record` definitions [here](openmesh/stream_processing/records.py).

We also define a key for the message, which is typically the data source followed by some unique identifier for that type of message. In the case of Coinbase, say we had a trade for BTC.USD, the key would then be `'coinbase_BTC.USD'`, letting Kafka parallelise the messages and distribute them to the correct partitions. This is important for performance, as it allows us to scale the system horizontally and distribute the load across multiple machines. As messages with the same keys are guaranteed to be processed in order, all BTC.USD trades on Coinbase will be processed sequentially.

### On-Chain
On-chain data collection follows a similar flow and structure to off-chain data collection, so you should read the [Off Chain](#off-chain) section above before you dive into this one.

On-chain data has a similar flow to off-chain data; i.e. `raw data collecting -> stream processing -> kafka consuming`, but there are a few differences:
 - Unlike CEXes, we consume data from entire blockchains at a time, i.e., we don't specify a `symbol` or an exchange to collect from. This is because the volumes are simply much lower
 - "Raw" blockchain data is intrinsically more structured than raw CEX data, so we predefine schemas for them, similar to the normalised topics like `lob`, `ticker`, e.t.c. For example, look at the schema for [Ethereum blocks](static/schemas/ethereum_blocks.avsc).

For this example, we'll look at adding Uniswap V3 data. As mentioned above, the raw Ethereum data is already being collected, we just need to define the specific processor to filter out what events correspond to activity on Uniswap V3. To understand this we need to do a quick deep dive on how Ethereum events and logs work. [This article](https://medium.com/mycrypto/understanding-event-logs-on-the-ethereum-blockchain-f4ae7ba50378) is a good starting point, but we'll go over the basics here.

Smart contracts on Ethereum have functions, and some of those functions emit "events" when they're called. These events are essentially logs that are stored on the blockchain, and can be used to track the state of the contract. For example, if you wanted to track the state of a token contract, you could listen for the `Transfer` event, which is emitted whenever a token is transferred. You can then use the data from the event to track the state of the token contract. In the case of Uniswap V3, we're interested in the `Swap` event, which is emitted whenever a swap occurs on the exchange. The `Swap` event has the following signature:

```solidity
event Swap(
    address indexed sender,
    address indexed recipient,
    int256 amount0,
    int256 amount1,
    uint160 sqrtPriceX96,
    uint128 liquidity,
    int24 tick
);
```

This means that when the `Swap` event is emitted, it will contain the following data:
 - `sender`: The address of the sender of the swap
 - `recipient`: The address of the recipient of the swap
 - `amount0`: The amount of token0 in the swap
 - `amount1`: The amount of token1 in the swap
 - `sqrtPriceX96`: The square root of the price of the swap
 - `liquidity`: The amount of liquidity in the swap
 - `tick`: The tick of the swap

This is everything we need to get full details of the swap, but there's one catch -- when given this event (also called a log), the entirety of the data is raw encoded in a `data` field. For example, look at the following log:

```json
{
  "atomTimestamp": 1673859038014502,
  "blockTimestamp": 1673859035000,
  "logIndex": 251,
  "transactionIndex": 105,
  "transactionHash": "0x930abc7d6dafdf0bcbbb1a9e814d3a360d984119627341d7718c767e63d21fef",
  "blockHash": "0x1ef5633ce51200c7f2b70a2ffe5d523e4787821b787afe89ffdac865345bc0a0",
  "blockNumber": 16418266,
  "address": "0x11b815efb8f581194ae79006d24e0d814b7697f6",
  "data": "0x00000000000000000000000000000000000000000000000021979f38ca407fe7ffffffffffffffffffffffffffffffffffffffffffffffffffffffff21eb06fa000000000000000000000000000000000000000000029261aad628fecce5ed2300000000000000000000000000000000000000000000000024759af0832ed494fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffce752",
  "topic0": "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
  "topic1": "0x00000000000000000000000068b3465833fb72a70ecdf485e0e4c7bd8665fc45",
  "topic2": "0x00000000000000000000000078387dae0367743e77137e100c94a2edd182a2e4",
  "topic3": null
}
```

`topic0` is a hash of the event signature, a way you can identify what the event is. `topic1`, `topic2`, and `topic3` are those arguments that use the `indexed` keyword. As you can see, the `data` field is just raw hex data. To make it readable, we need to decode it using something called an "Application Binary Interface", or ABI. Ethereum contracts deploy with one, and its essentially a dictionary that defines how we can transform the raw encoded data into something more useful. For an example, look at the [Uniswap V3 Pool ABI](static/abis/uniswap_v3_pool.json). The entry for the `Swap` event looks like this

```json
{
  "anonymous": false,
  "inputs": [
    {
      "indexed": true,
      "internalType": "address",
      "name": "sender",
      "type": "address"
    },
    {
      "indexed": true,
      "internalType": "address",
      "name": "recipient",
      "type": "address"
    },
    {
      "indexed": false,
      "internalType": "int256",
      "name": "amount0",
      "type": "int256"
    },
    {
      "indexed": false,
      "internalType": "int256",
      "name": "amount1",
      "type": "int256"
    },
    {
      "indexed": false,
      "internalType": "uint160",
      "name": "sqrtPriceX96",
      "type": "uint160"
    },
    {
      "indexed": false,
      "internalType": "uint128",
      "name": "liquidity",
      "type": "uint128"
    },
    {
      "indexed": false,
      "internalType": "int24",
      "name": "tick",
      "type": "int24"
    }
  ],
  "name": "Swap",
  "type": "event"
}
```

Using that as a guide, we can decode the raw event log and get the data we're looking for. Now, let's get into the code.

The off-chain standardisers sit in the base directory `openmesh/stream_processing/standardisers`. For on-chain data, each blockchain has its own directory which contain `LogHandler`s, processors for specific contract events. Let's look at Uniswap V3's `LogHandler`s, found in [`openmesh/stream_processing/standardisers/ethereum/log_handlers/uniswap_v3.py`](openmesh/stream_processing/standardisers/ethereum/log_handlers/uniswap_v3.py):

```python
from openmesh.stream_processing.standardisers.ethereum.log_handler import EthereumLogHandler
from yapic import json
from decimal import Decimal


class UniswapV3Handler(EthereumLogHandler):
    exchange = 'uniswap-v3'
    graph_endpoint: str = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
```

We start off by defining the exchange name as `uniswap-v3`, and providing an endpoint for the Uniswap V3 subgraph. We don't actually need it right now, but it's nice to have as a backup for querying Uniswap data if we're in a pinch.

```python
class UniswapV3SwapHandler(UniswapV3Handler):
    topic0 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
    event_name = "Swap"
    abi_name = 'uniswap_v3_pool'
    example_contract = '0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640'
```

For handling the swaps themselves, we define:
 - `topic0`: The hash of the event signature, used to identify the event as logs come in
 - `event_name`: The name of the event
 - `abi_name`: The name of the ABI file to use for decoding the event
 - `example_contract`: An example contract address. The web3 library we're using requires an actual contract address to help parsing events, so we just use one of the Uniswap V3 pools.

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    self.loaded_pool_data = False

def _load_pool_data(self):
    self.pool_data = json.loads(
        open('static/lists/uniswap_v3_pools.json').read())
    self.load_erc20_data()
    self.loaded_pool_data = True
```

In the `static/lists` directory, there are lists of tokens, pools, pairs e.t.c. that provide useful information for standardising data. For adding a new DEX, you might find it necessary to get some of this data yourself. The Graph can be a good place to find this information, but it can be from anywhere.

```python
    async def event_callback(self, event, blockTimestamp=None, atomTimestamp=None):
        if not self.loaded_pool_data:
            self._load_pool_data()
        args = event.args
        poolAddr = event.address
        pairDetails = self.pool_data.get(poolAddr, None)
        if pairDetails is None:
            return
        token0 = pairDetails['token0']
        token1 = pairDetails['token1']
        token0Sym = token0['symbol']
        token1Sym = token1['symbol']
        token0Id = token0['id']
        token1Id = token1['id']
        if args['amount0'] > 0:
            tokenBought = token0Sym
            tokenBoughtAddr = token0Id
            tokenSold = token1Sym
            tokenSoldAddr = token1Id
            amountBought = args['amount0']
            amountSold = abs(args['amount1'])
        else:
            tokenBought = token1Sym
            tokenBoughtAddr = token1Id
            tokenSold = token0Sym
            tokenSoldAddr = token0Id
            amountBought = args['amount1']
            amountSold = abs(args['amount0'])

        tokenBoughtDecimals = self.get_decimals(tokenBoughtAddr)
        tokenSoldDecimals = self.get_decimals(tokenSoldAddr)
        amountBought = Decimal(amountBought) / \
            Decimal(10 ** tokenBoughtDecimals)
        amountSold = Decimal(amountSold) / Decimal(10 ** tokenSoldDecimals)
        taker = args.recipient

        msg = dict(
            blockNumber=event.blockNumber,
            blockHash=event.blockHash,
            transactionHash=event.transactionHash,
            logIndex=event.logIndex,
            pairAddr=poolAddr,
            tokenBought=tokenBought,
            tokenBoughtAddr=tokenBoughtAddr,
            tokenSold=tokenSold,
            tokenSoldAddr=tokenSoldAddr,
            blockTimestamp=blockTimestamp,
            atomTimestamp=atomTimestamp,
            exchange=self.exchange,
            amountBought=amountBought,
            amountSold=amountSold,
            taker=taker,
        )

        await self.standardiser.send_to_topic('dex_trades', key_field='pairAddr', **msg)
```

Every `LogHandler`, like every `Standardiser`, must have an asynchronous `event_callback(self, event, blockTimestamp=None, atomTimestamp=None)` function defined. This is the function called whenever a log is received whose `topic0` is the same as the `LogHandler`'s `topic0`. The logic here is pretty straightforward and similar to the structure of an off-chain callback function; we simply extract the fields we want from the event data and construct a `Record` object to send to the `dex_trades` topic.

The last step is to modify the `__init__.py` file in the `log_handlers` directory to include the new `LogHandler`:

```python
from .uniswap_v3 import *
from .uniswap_v2 import *
from .dodo import *
from .curve import *
from .hashflow import *

log_handlers = [UniswapV3SwapHandler, UniswapV2SwapHandler,
                DodoexSellHandler, DodoexBuyHandler, DodoexSwapHandler, CurveSwapHandler, HashflowTradeHandler]
```

This will signal the Ethereum `Standardiser` to use our new `LogHandler` when it receives a log with the correct `topic0`.

## References
This document was adapted from the open-source contribution guidelines for [Facebook's Draft](https://github.com/facebook/draft-js/blob/a9316a723f9e918afde44dea68b5f9f39b7d9b00/CONTRIBUTING.md)
