from datetime import datetime as dt, timezone
from typing import Dict, Tuple, Union

class Symbol:

    seperator = '-'
    token_seperator = '.'

    def __init__(self, base: str, quote: str, symbol_type='spot', strike_price=None, option_type=None, expiry_date=None):

        self.quote = quote
        self.base = base
        self.type = symbol_type
        self.option_type = option_type
        self.strike_price = strike_price

        if expiry_date:
            self.expiry_date = self.date_format(expiry_date)

    def __repr__(self) -> str:
        return self.normalised

    def __str__(self) -> str:
        return self.normalised

    def __eq__(self, other) -> bool:
        return self.normalised == other.normalised

    def __hash__(self) -> int:
        return hash(self.normalised)

    @property
    def normalised(self) -> str:
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