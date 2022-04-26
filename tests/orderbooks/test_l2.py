import pytest
import json

from src.orderbooks.l2 import L2Lob
from src.orderbooks.orderbook import LobUpdateError
from src.orderbooks.lob_enums import *

class TestL2Lob():
    @pytest.fixture
    def orderbook(self):
        return L2Lob()

    @pytest.fixture
    def event(self):
        event = {
            'lob_action': UNKNOWN,
            'side': BID,
            'price': 10000.0,
            'size': 2.5,
        }
        return event
    
    @pytest.fixture
    def insert_event(self, orderbook, event):
        event['lob_action'] = INSERT
        orderbook.handle_event(event)
    
    def test_Snapshot_NoEvents_ReturnsEmptyBidsAndAsks(self, orderbook):
        snapshot = orderbook.snapshot()
        expected = json.dumps({'bids': {}, 'asks': {}})
        assert snapshot == expected, f"Orderbook was {snapshot} when it should have no levels."

    def test_Snapshot_OneBidEvent_CorrectlyReturnsSnapshot(self, orderbook, insert_event):
        snapshot = orderbook.snapshot()
        expected = json.dumps({'bids': {"10000.0": 2.5}, 'asks': {}})
        assert snapshot == expected

    def test_Snapshot_OneBidOneAskEvent_CorrectlyReturnsSnapshot(self, orderbook, event, insert_event):
        event['side'] = ASK
        event['price'] = 10001.0
        event['size'] = 5.0
        orderbook.handle_event(event)
        snapshot = orderbook.snapshot()
        expected = json.dumps({'bids': {"10000.0": 2.5}, 'asks': {"10001.0": 5.0}})
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

    def test_HandleEvent_InsertEvent_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        assert "10000.0" in sc['bids'].keys()
        assert sc['bids']["10000.0"] == 2.5

    def test_HandleEvent_InsertMultiplePriceLevels_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['price'] = "10001.0"
        event['size'] = 5.0
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        assert "10000.0" in sc['bids'].keys()
        assert sc['bids']["10000.0"] == 2.5
        assert "10001.0" in sc['bids'].keys()
        assert sc['bids']["10001.0"] == 5.0

    def test_HandleEvent_InsertEventBothSides_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['price'] = "10001.0"
        event['size'] = 5.0
        event['side'] = ASK
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        assert "10000.0" in sc['bids'].keys()
        assert sc['bids']["10000.0"] == 2.5
        assert "10001.0" in sc['asks'].keys()
        assert sc['asks']["10001.0"] == 5.0
    
    def test_HandleEvent_UpdateEvent_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['lob_action'] = UPDATE
        event['size'] = 5.0
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        assert "10000.0" in sc['bids'].keys()
        assert sc['bids']["10000.0"] == 5.0

    def test_HandleEvent_RemoveEvent_CorrectlyUpdatesLob(self, orderbook, event, insert_event):
        event['lob_action'] = REMOVE
        orderbook.handle_event(event)
        sc = json.loads(orderbook.snapshot(), parse_float=float, parse_int=int)
        assert len(sc['bids'].keys()) == 0
    
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