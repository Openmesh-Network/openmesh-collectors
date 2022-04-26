from abc import ABC, abstractmethod


class LobUpdateError(ValueError):
    pass

class Orderbook(ABC):
    """
    Defining methods for an orderbook management class so it's easier 
    to write unittests.
    """

    @abstractmethod
    def handle_event(event: dict):
        """Update the LOB with the provided event.

        Processes and adds an event into the orderbook based on the data 
        schema in our documentation. The event must be in the form of a 
        dictionary and include the following fields:
          - lob_action
          - side
          - price
          - size
          - order_id (for L3 Orderbooks)
        
        Parameters
        ----------
        event : dict
            Dictionary describing the LOB event to update the orderbook
            with with the fields described above.

        Raises
        ------
        KeyError
            If one of the required fields isn't present in the dictionary.
        LobUpdateError
            If the event cannot be processed in the current LOB state.
        """
        pass
    
    @abstractmethod
    def snapshot() -> str:
        """Returns a snapshot of the current LOB state as a dictionary json string.

        For L2: 
        {
            'bids': {
                [price]: [size]
                ...
            },
            'asks': {
                [price]: [size]
                ...
            }
        }

        For L3: 
        {
            'bids': {
                price: [{'size': size, 'order_id': order_id}, ...]
                ...
            },
            'asks': {
                price: [{'size': size, 'order_id': order_id}, ...]
                ...
            }
        }

        Returns
        -------
        str
            For L2, maps a price to a float. For L3, maps a price to a 
            list of orders as dicts containing their size and order ID.
        """
        pass