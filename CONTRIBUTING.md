# Table of Contents
1. [Contributing to L3 Atom](#contributing-to-l3-atom)
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

# Contributing to L3 Atom
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
Install all needed dependencies via `pip install -r requirements.txt`. This project relies on Kafka for messaging, so if you want to run it locally, Kafka will need to be running. Eventually there will be a version of L3 Atom designed to run on a single process for local development (as well as tools to automatically set up Kafka + other tools for simulating a distributed setup), but for now you'll have to set up Kafka yourself. There are many ways to do so -- the easiest we've found is to follow [confluent's guide](https://docs.confluent.io/confluent-cli/current/install.html) to quickly get a cluster up and running. If you want to use the stream processing component of L3 Atom, you'll also have to set up Schema Registry and the necessary [Avro schemas](static/schemas/) so that the stream processing layer knows how to SerDe data. Installing Confluent also installs Schema Registry, so you can set it up there.

Once you've set up the necessary externalities, you need to tell the app where it can connect to Kafka. Make a `.env` file in `keys/`, and fill it in with the following:

If your Kafka requires SASL authentication:
```ini
KAFKA_BOOTSTRAP_SERVERS=<URL of your Kafka Broker(s)>
KAFKA_SASL_KEY=<Username for SASL Authentication>
KAFKA_SASL_SECRET=<Secret for SASL Authentication>

SCHEMA_REGISTRY_URL=<URL of your Schema Registry setup>
SCHEMA_REGISTRY_API_KEY=<Username for Schema Registry Authentication>
SCHEMA_REGISTRY_API_SECRET=<Secret for Schema Registry Authentication>
```

If your Kafka does not require authentication:
```ini
KAFKA_BOOTSTRAP_SERVERS=<URL of your Kafka Broker(s)>

SCHEMA_REGISTRY_URL=<URL of your Schema Registry setup>
```

That will be sufficient to ingest and process off-chain data, but to process on-chain data, you will also need access to an ethereum node, preferably Infura. Once you have it, add it to the `.env` file as well:

```ini
ETHEREUM_NODE_HTTP_URL=<URL of your Ethereum node via HTTP>
ETHEREUM_NODE_WS_URL=<URL of your Ethereum node via Websockets>
ETHEREUM_NODE_SECRET=<Secret for Ethereum node authentication (if required)>
```

Both HTTP and Websockets are required.

Additionally, in `config.ini`, you'll want to set `num_replications` to be equal to the number of brokers you have running. Most likely, in a local development environment, you'll only be running 1. If `num_replications` is greater than the number of brokers, an error will be thrown as the program will be unable to create the necessary Kafka topics.

Symbols take the form of `<base>.<quote>`, e.g. `BTC.USD`, `ETH.USD`, `BTC.EUR` for spots, and `<base>.<quote>-PERP` for perpetual futures, e.g. `BTC.USDT-PERP`. L3 Atom supports every symbol listed on the given exchanges.

The entry point for running the application is `runner.py`, which can be used in of the following ways:

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

Eventually a more "mini" version of L3 Atom will be developed which supports smaller use cases that can be handled on one machine. The current application is designed for a more distributed setup as the data volumes are simply too high for one machine to handle.

## Adding additional sources
### Off-Chain
Let's walk through an example of how an exchange is implemented. All exchanges have the same style and structure, so by diving into how one works, you can read through the others and get a good understanding of how each data source is structured. All off-chain sources are in `l3_atom/off_chain/`, each with their own file. Let's go through Coinbase for this example.

```py
from l3_atom.data_source import DataFeed
from l3_atom.tokens import Symbol
from l3_atom.feed import WSConnection, WSEndpoint, AsyncFeed
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

Each source extends the `DataFeed` class, which handles all of the back-end connection operations for us. What we need to do is define certain properties that `DataFeed`'s methods can use to handle the connection for our specific exchange. First we define the name, in this case, just `"coinbase"`. This is a unique identifier and appears in a bunch of places. By convention, this should always be lowercase. `sym_field` defines where we can acquire the symbol for each message. This is used for creating Kafka keys -- essentially we want messages for the same symbol to have the same key so that Kafka processes them in order. Since Coinbase always puts the symbol in a field accessed by `'product_id'`, that's what we set the sym_field as. What happens when this isn't the case? If you look in `l3_atom/data_source.py`, you'll see that `sym_field` (and another field, `type_field`) are used in the following way:

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

Going back to the code before, next lets look at the `ws_endpoints` and `ws_channels` properties. `ws_endpoints` maps from an endpoint to connect to to a list of feeds to subscribe to on that endpoint. `ws_channels` effectively maps from L3 Atom's nomenclature to the exchange's, e.g. in this case, what we call `'lob_l3'`, Coinbase calls `'full'`, or, in other words, in order to get L3 limit order book data, we need to subscribe to the `'full'` channel over Coinbase's API. Coinbase is unique in that L3 trade messages are also sent over the `'full'` channel (most exchanges have different channels for order book and trade events), so even though `'trades_l3'` isn't specified in `ws_endpoints`, we show the mapping for it in `ws_channels`. Finally, `symbols_endpoint` is a HTTP endpoint we can query to get the full list of symbols from the exchange. Sometimes, this might be a list of endpoints, which means that we have to query every endpoint in the list to get the full list of symbols. This endpoint is queried and the results are passed to a processing function, which we have to define for each exchange (since each exchange has different formatting for their symbols):

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

This method takes in a list of symbols (which is just the return value from making a GET request to the endpoint(s) defined in `symbols_endpoint`), and returns a dictionary that maps from L3 Atom's formatting for symbols (i.e. `<base>.<quote>`) to the exchange's formatting for symbols (i.e., in the case of Coinbase, `<base>-<quote>`). This is used to keep a standard symbol format across exchanges, and to allow for us to use a single format when subscribing to symbols across exchanges, even though those exchanges might use different formats themselves.

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

Stream processing is contained in `l3_atom/stream_processing/`. For processing the data from Coinbase specifically, we enter [`l3_atom/stream_processing/standardisers/coinbase.py`](l3_atom/stream_processing/standardisers/coinbase.py). Like the raw data collection as contained in `l3_atom/off_chain`, with each source having its own class extending `DataFeed`, each source has an equivalent `Standardiser`. You can view the source for `Standardiser` in [`l3_atom/stream_processing/standardiser.py`](l3_atom/stream_processing/standardiser.py). Essentially it handles the processing of raw data into a standard, consistent format, which obviously differs per exchange. 

Let's have a look at Coinbase's standardiser:

```py
from l3_atom.stream_processing.standardiser import Standardiser
from l3_atom.off_chain import Coinbase
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

So when we extract the values from the raw Coinbase message and create a `Record` object from it, that data is validated and serialised automatically. You can view the full list of `Record` definitions [here](l3_atom/stream_processing/records.py).

We also define a key for the message, which is typically the data source followed by some unique identifier for that type of message. In the case of Coinbase, say we had a trade for BTC.USD, the key would then be `'coinbase_BTC.USD'`, letting Kafka parallelise the messages and distribute them to the correct partitions. This is important for performance, as it allows us to scale the system horizontally and distribute the load across multiple machines. As messages with the same keys are guaranteed to be processed in order, all BTC.USD trades on Coinbase will be processed sequentially.

### On Chain
TODO

## References
This document was adapted from the open-source contribution guidelines for [Facebook's Draft](https://github.com/facebook/draft-js/blob/a9316a723f9e918afde44dea68b5f9f39b7d9b00/CONTRIBUTING.md)
