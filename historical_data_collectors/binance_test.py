# # import pytest
# from .binance_data_collector import BinanceDataCollector
# import datetime
# from types import MethodType


# # # Test case
# # def test_my_function(mocker):
# #     # Mock the my_function using pytest-mock
# #     mock_function = mocker.patch('__main__.my_function')
    
# #     # Configure side_effect to return different values each time it's called
# #     mock_function.side_effect = ['response1', 'response2', 'response3']

# #     # Test the function
# #     assert my_function() == 'response1'
# #     assert my_function() == 'response2'
# #     assert my_function() == 'response3'


# def test_fetch_and_write_symbol_trades():
    
#     data_collector = BinanceDataCollector()


#     arg_date_format = "%Y/%m/%d"

#     #inclusive
#     start_date = datetime.datetime.strptime('2024/01/01', arg_date_format).date()

#     #exclusive
#     end_date = datetime.datetime.strptime('2024/01/02', arg_date_format).date()

#     # start_time = int(
#     #     datetime.datetime.combine(start_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)
#     # end_time = int(
#     #     datetime.datetime.combine(end_date, datetime.datetime.min.time(), tzinfo=utc_timezone).timestamp() * 1000)

#     data_collector.total_fetched_trades = []
#     def collect_trades(self, trades):

#         self.total_fetched_trades.append(trades)        

#     data_collector.write_to_database = MethodType(collect_trades, data_collector)

#     data_collector.write_to_database = collect_trades
#     data_collector.fetch_and_write_symbol_trades('LPT/USDT', start_date, end_date)

#     print(data_collector.total_fetched_trades)

#     #replace the fetch_trades function with a mock

#     #replace the write to db function with a mock. mock can be a function that just returns the trades to be written.