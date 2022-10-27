# L3 Atom
A massive open source cryptocurrency cloud data lake.

## Table of Contents
- [Background](#background)
- [Setup](#setup)
- [Schemas](#schemas)

## Background
This repository hosts the codebase for L3 Atom's exchange collectors -- Python processes which connect to cryptocurrency exchanges via Websocket and continuously collect market data from them. The collected data is processed in a cloud-native data lake, which will be made available to the public. It works by connecting to a variety of different market data APIs, sending that raw data over Kafka to another process which standardises it into consistent schemas, publishing that back into Kafka topics separated by event type.

## Setup
Clone the repository and install the necessary dependencies with `pip`. This project relies on Kafka for messaging, so if you want to run it locally, you're going to have to set up Kafka. There are many ways to do so -- the easiest we've found is to follow [confluent's guide](https://docs.confluent.io/confluent-cli/current/install.html) to quickly get a cluster up and running. If you want to use the stream processing component of L3 Atom, you'll also have to set up Schema Registry and the necessary [Avro schemas](#schemas) so that the stream processing layer knows how to SerDe data. Installing Confluent also installs Schema Registry, so you can set it up there.

Once you've set up the necessary externalities, you need to tell the app where it can connect to Kafka. Make a `.env` file in `keys/`, and fill it in with the following:

If your Kafka requires SASL authentication:
```
KAFKA_BOOTSTRAP_SERVERS=<URL of your Kafka Broker(s)>
KAFKA_SASL_KEY=<Username for SASL Authentication>
KAFKA_SASL_SECRET=<Secret for SASL Authentication>

SCHEMA_REGISTRY_URL=<URL of your Schema Registry setup>
SCHEMA_REGISTRY_API_KEY=<Username for Schema Registry Authentication>
SCHEMA_REGISTRY_API_SECRET=<Secret for Schema Registry Authentication>
```

If your Kafka does not require authentication:
```
KAFKA_BOOTSTRAP_SERVERS=<URL of your Kafka Broker(s)>

SCHEMA_REGISTRY_URL=<URL of your Schema Registry setup>
```

In `config.ini`, you can specify what symbols to subscribe to. We have some default options, but the codebase will work for any symbol supported by the exchange.

Once these are setup, the entry point for running the application is `runner.py`, which can be used in the following two ways:

```
python3 runner.py connector <exchange>
python3 runner.py processor
```

Where the first option runs the raw data collector for the given exchange, and the latter runs the Faust stream processor. A Dockerfile is also provided if you want to run the application in a Docker container, just make sure to load in the `.env` file you wrote earlier as a volume.

If you want to run the full application, you'll want to have three processes running:

1. The raw data collection for a single exchange
2. The stream processor
3. A Kafka consumer on one of the normalised topics (e.g. `lob`)

From there, you'll start to see a standardised orderbook feed coming in at low latency. Note that the data will be in Avro, so you'll probably want to have some kind of deserializer to make it human-readable.

## Schemas

### L3 Lob Event
```
{
    "type": "record",
    "name": "L3_LOB",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "price",
            "type": "double"
        },
        {
            "name": "size",
            "type": "double"
        },
        {
            "name": "side",
            "type": "string"
        },
        {
            "name": "order_id",
            "type": "string"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

### L3 Trade Event
```
{
    "type": "record",
    "name": "L3_Trades",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "price",
            "type": "double"
        },
        {
            "name": "size",
            "type": "double"
        },
        {
            "name": "taker_side",
            "type": "string"
        },
        {
            "name": "trade_id",
            "type": "string"
        },
        {
            "name": "maker_order_id",
            "type": "string"
        },
        {
            "name": "taker_order_id",
            "type": "string"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

### Ticker
```
{
    "type": "record",
    "name": "Ticker",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "bid_price",
            "type": "double"
        },
        {
            "name": "bid_size",
            "type": "double"
        },
        {
            "name": "ask_price",
            "type": "double"
        },
        {
            "name": "ask_size",
            "type": "double"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

### Lob Event
```
{
    "type": "record",
    "name": "LOB",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "price",
            "type": "double"
        },
        {
            "name": "size",
            "type": "double"
        },
        {
            "name": "side",
            "type": "string"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

### Trade Event
```
{
    "type": "record",
    "name": "Trade",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "price",
            "type": "double"
        },
        {
            "name": "size",
            "type": "double"
        },
        {
            "name": "taker_side",
            "type": "string"
        },
        {
            "name": "trade_id",
            "type": "string"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

### Candlestick
```
{
    "type": "record",
    "name": "Candle",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "start",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "end",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "interval",
            "type": "string"
        },
        {
            "name": "trades",
            "type": "int"
        },
        {
            "name": "closed",
            "type": "boolean"
        },
        {
            "name": "o",
            "type": "double"
        },
        {
            "name": "h",
            "type": "double"
        },
        {
            "name": "l",
            "type": "double"
        },
        {
            "name": "c",
            "type": "double"
        },
        {
            "name": "v",
            "type": "double"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

### Funding Rate
```
{
    "type": "record",
    "name": "funding_rate",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "mark_price",
            "type": "double"
        },
        {
            "name": "funding_rate",
            "type": "double"
        },
        {
            "name": "next_funding_time",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "predicted_rate",
            "type": "double"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

### Open Interest
```
{
    "type": "record",
    "name": "open_interest",
    "namespace": "com.acme.avro",
    "fields": [
        {
            "name": "exchange",
            "type": "string"
        },
        {
            "name": "symbol",
            "type": "string"
        },
        {
            "name": "open_interest",
            "type": "double"
        },
        {
            "name": "event_timestamp",
            "type": {
                "type": "long",
                "logicalType": "timestamp-millis"
            }
        },
        {
            "name": "atom_timestamp",
            "type": "long"
        }
    ]
}
```

