import pytest
import json

from src.orderbooks.l3 import L3Lob
from src.orderbooks.orderbook import LobUpdateError
from src.orderbooks.lob_enums import *

class TestL3Lob():
    @pytest.fixture
    def orderbook(self):
        return L3Lob()

    @pytest.fixture
    def event(self):
        event = {
            'lob_action': UNKNOWN,
            'side': BID,
            'price': 10000.0,
            'size': 2.5,
            'order_id': "1"
        }
        return event
    
    @pytest.fixture
    def insert_event(self, orderbook, event):
        event['lob_action'] = INSERT
        orderbook.handle_event(event)
    
    def assert_order_exists(self, sc: dict, side: int, level: str, order_pos: int, 
            size: float, order_id: str):
        book = sc['bids'] if side == BID else sc['asks']
        assert level in book.keys()
        orders = book[level]
        assert isinstance(orders, list)
        assert len(orders) > order_pos 
        assert "size" in orders[order_pos].keys() and "order_id" in orders[order_pos].keys()
        assert orders[order_pos]['size'] == size
        assert orders[order_pos]['order_id'] == order_id
    
    def test_Snapshot_NoEvents_ReturnsEmptyBidsAndAsks(self, orderbook):
        snapshot = orderbook.snapshot()
        expected = json.dumps({'bids': {}, 'asks': {}})
        assert snapshot == expected, f"Orderbook was {snapshot} when it should have no levels."

    def test_Snapshot_OneBidEvent_CorrectlyReturnsSnapshot(self, orderbook, insert_event):
        snapshot = orderbook.snapshot()
        expected = json.dumps({'bids': {"10000.0": [{'size': 2.5, 'order_id': "1"}]}, 'asks': {}})
        assert snapshot == expected

    def test_Snapshot_OneBidOneAskEvent_CorrectlyReturnsSnapshot(self, orderbook, event, insert_event):
        event['side'] = ASK
        event['price'] = 10001.0
        event['size'] = 5.0
        event['order_id'] = "2"
        orderbook.handle_event(event)
        snapshot = orderbook.snapshot()
        expected = json.dumps({'bids': {"10000.0": [{'size': 2.5, 'order_id': "1"}]}, 'asks': {"10001.0": [{'size': 5.0, 'order_id': "2"}]}})
        assert snapshot == expected

    def test_HandleEvent_EventMissingLobactionField_RaiseKeyError(self, orderbook, event):
        del event['lob_action']
        with pytest.raises(KeyError) as e:
            orderbook.handle_event(event)

    def test_HandleEvent_EventMissingSideField_RaiseKeyError(self, orderbook, event):
        del event['side']
        with pytest.raises(KeyError) as e:
            orderbook.handle_event(event)

    def test_HandleEvent_EventMissingPriceField_RaiseKeyError(self, orderbook, event):
        del event['price']
        with pytest.raises(KeyError) as e:
            orderbook.handle_event(event)

    def test_HandleEvent_EventMissingSizeField_RaiseKeyError(self, orderbook, event):
        del event['size']
        with pytest.raises(KeyError) as e:
            orderbook.handle_event(event)

    def test_HandleEvent_EventMissingOrderIdField_RaiseKeyError(self, orderbook, event):
        del event['order_id']
        with pytest.raises(KeyError) as e:
            orderbook.handle_event(event)

    def test_HandleEvent_InsertEvent_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 0, 2.5, "1")

    def test_HandleEvent_InsertMultiplePriceLevels_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['price'] = "10001.0"
        event['size'] = 5.0
        event['order_id'] = "2"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 0, 2.5, "1")
        self.assert_order_exists(sc, BID, "10001.0", 0, 5.0, "2")

    def test_HandleEvent_InsertMultipleOrdersAtSameLevel_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['size'] = 5.0
        event['order_id'] = "2"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 0, 2.5, "1")
        self.assert_order_exists(sc, BID, "10000.0", 1, 5.0, "2")

    def test_HandleEvent_InsertEventBothSides_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['price'] = "10001.0"
        event['size'] = 5.0
        event['side'] = ASK
        event['order_id'] = "2"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 0, 2.5, "1")
        self.assert_order_exists(sc, ASK, "10001.0", 0, 5.0, "2")
    
    def test_HandleEvent_UpdateEvent_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['lob_action'] = UPDATE
        event['size'] = 5.0
        event['order_id'] = "1"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 0, 5.0, "1")
    
    def test_HandleEvent_UpdateEventToLargerSize_ResetsPositionInQueue(self, orderbook, event, insert_event):
        event['size'] = 5.0
        event['order_id'] = "2"
        orderbook.handle_event(event)
        event['lob_action'] = UPDATE
        event['size'] = 7.5
        event['order_id'] = "1"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 1, 7.5, "1")

    def test_HandleEvent_UpdateEventToDifferentPrice_ResetsPositionInQueue(self, orderbook, event, insert_event):
        event['size'] = 5.0
        event['order_id'] = "2"
        orderbook.handle_event(event)
        event['size'] = 3.0
        event['order_id'] = "3"
        event['price'] = 10001.0
        orderbook.handle_event(event)
        event['lob_action'] = UPDATE
        event['size'] = 1.0
        event['price'] = 10001.0
        event['order_id'] = "1"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10001.0", 0, 3.0, "3")
        self.assert_order_exists(sc, BID, "10001.0", 1, 1.0, "1")
        self.assert_order_exists(sc, BID, "10000.0", 0, 5.0, "2")
    
    def test_HandleEvent_UpdateEventToSmallerSize_KeepsPositionInQueue(self, orderbook, event, insert_event):
        event['size'] = 5.0
        event['order_id'] = "2"
        orderbook.handle_event(event)
        event['lob_action'] = UPDATE
        event['size'] = 1.0 
        event['order_id'] = "1"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 0, 1.0, "1")

    def test_HandleEvent_RemoveEvent_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['lob_action'] = REMOVE
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        assert len(sc['bids'].keys()) == 0

    def test_HandleEvent_RemoveEventMultipleOrders_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['size'] = 5.0
        event['lob_action'] = INSERT
        event['order_id'] = "2"
        orderbook.handle_event(event)
        event['size'] = 7.5
        event['lob_action'] = INSERT
        event['order_id'] = "3"
        orderbook.handle_event(event)
        event['lob_action'] = REMOVE
        event['order_id'] = "2"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        self.assert_order_exists(sc, BID, "10000.0", 0, 2.5, "1")
        self.assert_order_exists(sc, BID, "10000.0", 1, 7.5, "3")
    
    def test_HandleEvent_InsertEventAtExistingPriceLevel_RaisesLobUpdateError(self, orderbook, event, insert_event):
        with pytest.raises(LobUpdateError):
            orderbook.handle_event(event)
    
    def test_HandleEvent_UpdateNoneExistingEvent_RaisesLobUpdateError(self, orderbook, event):
        event['lob_action'] = UPDATE
        with pytest.raises(LobUpdateError):
            orderbook.handle_event(event)
    
    def test_HandleEvent_RemoveNoneExistingEvent_RaisesLobUpdateError(self, orderbook, event):
        event['lob_action'] = REMOVE
        with pytest.raises(LobUpdateError):
            orderbook.handle_event(event)

    def test_HandleEvent_RemoveExistingOrder_ShouldNotDependOnPriceInEvent(self, orderbook, event, insert_event):
        event['lob_action'] = REMOVE
        event['price'] = -1
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot())
        assert len(sc['bids']) == 0

    def test_HandleEvent_RemoveOrderThatDoesntExist_ShouldIgnoreMessage(self, orderbook, event, insert_event):
        event['lob_action'] = REMOVE
        event['price'] = -1
        event['order_id'] = "2"
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot())
        assert len(sc['bids']) == 0