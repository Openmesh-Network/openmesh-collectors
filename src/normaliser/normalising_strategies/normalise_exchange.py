
class NormaliseExchange():
    """
    Abstract class for strategy design pattern implementation.
    """

    def normalise(self, data) -> dict:
        """
        Normalise the raw websocket dictionary data into a format acceptable by 
        the data tables.

        You can add any other private method to your implementation of the class.
        Make sure that normalise is the ONLY public method in your implementation.
        (Prefix private methods with an underscore).

        Returns a dictionary with the keys "lob_event" and "market_orders", which are 
        lists containing dictionaries suitable to be passed into the normalised tables:
        e.g: 
            normalised = {
                "lob_events" = [{<event>}, {<event>}, ...],
                "market_orders" = [{<order>}, {<order>}, ...]
            }
        """
        raise NotImplementedError()