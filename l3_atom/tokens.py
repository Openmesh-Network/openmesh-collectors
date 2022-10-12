class Symbol:

    """
    Class to handle symbols.

    :param seperator: Seperator to use when joining tokens to other information
    :type seperator: str
    :param token_seperator: Seperator to use when joining tokens
    :type token_seperator: str
    :param base: Base token
    :type base: str
    :param quote: Quote token
    :type quote: str
    :param symbol_type: Type of symbol (spot, future, option), defaults to 'spot'
    :type symbol_type: str, optional
    :param strike_price: Strike price of the option, defaults to None
    :type strike_price: float, optional
    :param option_type: Type of option (call, put), defaults to None
    :type option_type: str, optional
    :param expiry_date: Expiry date of the option, defaults to None
    :type expiry_date: datetime, optional
    """

    seperator = '-'
    token_seperator = '.'

    def __init__(self, base: str, quote: str, symbol_type='spot', strike_price=None, option_type=None, expiry_date=None):

        self.quote = quote
        self.base = base
        self.type = symbol_type
        self.option_type = option_type
        self.strike_price = strike_price
        self.expiry_date = expiry_date

    def __repr__(self) -> str:
        """Returns a string representation of the symbol"""
        return self.normalised

    def __str__(self) -> str:
        """Returns a string representation of the symbol"""
        return self.normalised

    def __eq__(self, other) -> bool:
        """Returns True if the symbols are equal"""
        if isinstance(other, Symbol):
            return self.normalised == other.normalised
        elif isinstance(other, str):
            return self.normalised == other

    def __hash__(self) -> int:
        """Computes a hash value for the string of the symbol"""
        return hash(self.normalised)

    @property
    def normalised(self) -> str:
        """Returns the normalised symbol"""
        if self.base == self.quote:
            base = self.base
        else:
            base = f"{self.base}{self.token_seperator}{self.quote}"
        if self.type == 'spot':
            return base
        if self.type == 'option':
            return f"{base}{self.seperator}{self.strike_price}{self.seperator}{self.expiry_date}{self.seperator}{self.option_type}"
        if self.type == 'futures':
            return f"{base}{self.seperator}{self.expiry_date}"
        if self.type == 'perpetual':
            return f"{base}{self.seperator}PERP"
        raise ValueError(f"Unsupported symbol type: {self.type}")
