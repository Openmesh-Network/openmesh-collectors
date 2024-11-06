# Openmesh Collectors

![Openmesh logo](./static/images/logo.svg)

Openmesh collectors turns streams from [many data sources](/##Supported-Data-Sources) into a single unified format to be used internally.

Currently the main data sources are Web3 financial data.

## Dependencies

- Docker
- Python

## Libraries

- Apache Kafka

## How to run

All the information for running collectors locally is in our [contributor guide](CONTRIBUTING.md).
It'll show you how to set up the environment and help you better understand how you can contribute to the project.

## Supported Data Sources

For off-chain data, the following exchanges are supported:

- Binance
- Binance Futures
- Bitfinex
- Bybit
- Coinbase
- Deribit
- Gemini
- Kraken
- Kraken Futures
- Phemex

For on-chain data, the following DeFi protocols are supported:

**Ethereum**

- Curve Finance
- Dodoex
- Hashflow
- Sushiswap
- Uniswap V2
- Uniswap V3

## Free Managed Service

Openmesh is also hosted as a **free** managed service where you can tap into crypto market data via our Websocket API, or analyze historical data via our query tool.
View the documentation [here](https://open-mesh.gitbook.io/l3a-v3-documentation-2.0/products/unified-api).

## Examples of Data

### Binance Trade

```json
{
  "exchange": "binance",
  "symbol": "BTC.USDT",
  "price": "18200.46",
  "size": "0.00987",
  "taker_side": "sell",
  "trade_id": "2465019970",
  "maker_order_id": "17236243366",
  "taker_order_id": "17236243463",
  "event_timestamp": "2023-01-12T11:44:40.270000Z",
  "atom_timestamp": 1673523880325488
}
```

### Uniswap V2 Swap

```json
{
  "exchange": "uniswap-v2",
  "atomTimestamp": 1673523937320352,
  "blockTimestamp": "2023-01-12T11:45:35Z",
  "maker": null,
  "taker": "0xD6f3E785D2C8Fd698A2B341DDC860f291aB7fF61",
  "tokenBought": "DAI",
  "tokenSold": "WETH",
  "tokenBoughtAddr": "0x6b175474e89094c44da98b954eedeac495271d0f",
  "tokenSoldAddr": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
  "amountBought": "83.629106541068626336000000",
  "amountSold": "0.060000000000000000000000",
  "logIndex": 163,
  "transactionHash": "0x098b29872b5a2bc4aaf93fc7cbbdaced677657c051338b54494352d9cffe1fcb",
  "blockHash": "0xbcde35a84ed4985e8eafbc8409cda523268d1eece33b8ec5e7195a53897c66d4",
  "blockNumber": 16390510,
  "pairAddr": "0xa478c2975ab1ea89e8196811f51a7b7ade33eb11"
}
```

### Ethereum Block

```json
{
  "atomTimestamp": 1673523998673950,
  "number": 16390515,
  "hash": "0xc874e1704208666e624402f876cf2c0e6ef770f3135b259bb30508d0706942b3",
  "parentHash": "0x46a16ce33a2ba476762212e418171f43bf083f6060e3fc74efc5ab46eb619ce3",
  "nonce": "0x0000000000000000",
  "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
  "logsBloom": "0x00200050a1000000121806008900000202024020000008a020010200040004140001092044081420800018000018010052000040088028002000000010a02000014000000020000828020048110000284094000068400000000004008002a20012040800020000010040800000000809c02000000080441041808011a2088820100003140080180004480000001010008000828129048208402000c0001200000200054240006201200000c40002100800200000203001801020000000880000a1009006000108100002200000000000001102400000005000000902000020001030b02a04400100000112800000080002120000400040400000004048001000",
  "transactionsRoot": "0xb5801367d4d95058a24ff7aec1f5711e01cb8d4130c88eda7444d0b685e751c6",
  "stateRoot": "0x8fa22d1026db5f7f615392cc83290ea46c91b8a72e75a9397dee77ad88d40b52",
  "receiptsRoot": "0xd19d05a65e76d2133af12d7d32cbec58ffe8616924ead5c7c7571e3d07aafd73",
  "miner": "0x2f9fc79066bb3ffb072bcf4e26b635436d026ce5",
  "difficulty": 0,
  "totalDifficulty": "58750003716598355984384",
  "extraData": "0x",
  "size": 6491,
  "gasLimit": "30000000",
  "gasUsed": "1755498",
  "blockTimestamp": "2023-01-12T11:46:35Z"
}
```
