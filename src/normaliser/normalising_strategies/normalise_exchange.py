
class NormaliseExchange():
    """
    Abstract class for strategy design pattern implementation.
    """

    def normalise(self, data) -> dict:
        """
        Normalise the raw websocket dictionary data into a format acceptable by 
        the data tables.

        Returns a dictionary with the keys "lob_event" and "market_orders", which are 
        lists containing dictionaries suitable to be passed into the normalised tables:
        e.g: 
            normalised = {
                "lob_events" = [{<event>}, {<event>}, ...],
                "market_orders" = [{<order>}, {<order>}, ...]
            }

        :param data: Raw websocket dictionary data to be normalised.
        """
        raise NotImplementedError()