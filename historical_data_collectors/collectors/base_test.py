#We use the binance data collector to test the methods that need self as an argument but are defined in the BaseDataCollector
from .binance_data_collector import BinanceDataCollector


def test_normalize_to_l2():
    """Tests the normalize_to_l2 function of BaseDataCollector. We pass 2 sample trades and check the output is as expected to test."""
    #sample trades
    trades = [
        {'info': {'a': '24476193', 'p': '15.15700000', 'q': '32.95000000', 'f': '37759517', 'l': '37759517', 'T': '1714974751879', 'm': False, 'M': True}, 'timestamp': 1714974751879, 'datetime': '2024-05-06T05:52:31.879Z', 'symbol': 'LPT/USDT', 'id': '24476193', 'order': None, 'type': None, 'side': 'buy', 'takerOrMaker': None, 'price': 15.157, 'amount': 32.95, 'cost': 499.42315, 'fee': None, 'fees': []},
        {'info': {'a': '24475973', 'p': '15.18200000', 'q': '0.64000000', 'f': '37759206', 'l': '37759206', 'T': '1714974461875', 'm': True, 'M': True}, 'timestamp': 1714974461875, 'datetime': '2024-05-06T05:47:41.875Z', 'symbol': 'LPT/USDT', 'id': '24475973', 'order': None, 'type': None, 'side': 'sell', 'takerOrMaker': None, 'price': 15.182, 'amount': 0.64, 'cost': 9.71648, 'fee': None, 'fees': []}
        ]

    data_collector = BinanceDataCollector()
    l2_trades = data_collector.normalize_to_l2(trades, 'Binance')

    first_trade = l2_trades[0]

    #check the normalised trades are correctly formatted
    assert first_trade[0] == 'Binance' 
    assert first_trade[1] == 'LPT/USDT'
    assert first_trade[2] == 15.157
    assert first_trade[3] == 32.95
    assert first_trade[4] == 'buy'
    assert first_trade[5] == '24476193'
    assert first_trade[6] == 1714974751879

    second_trade = l2_trades[1]

    assert second_trade[0] == 'Binance' 
    assert second_trade[1] == 'LPT/USDT'
    assert second_trade[2] == 15.182
    assert second_trade[3] == 0.64
    assert second_trade[4] == 'sell'
    assert second_trade[5] == '24475973'
    assert second_trade[6] == 1714974461875
