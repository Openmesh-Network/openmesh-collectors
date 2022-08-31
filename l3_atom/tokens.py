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
        return self.normalized

    def __str__(self) -> str:
        return self.normalized

    def __eq__(self, other) -> bool:
        return self.normalized == other.normalized

    def __hash__(self) -> int:
        return hash(self.normalized)

    @staticmethod
    def month_code(month: str) -> str:
        ret = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
        return ret[int(month) - 1]

    @staticmethod
    def date_format(date):
        if isinstance(date, (int, float)):
            date = dt.fromtimestamp(date, tz=timezone.utc)
        if isinstance(date, dt):
            year = str(date.year)[2:]
            month = Symbol.month_code(date.month)
            day = date.day
            return f"{year}{month}{day}"

        if len(date) == 4:
            year = str(dt.utcnow().year)[2:]
            date = year + date
        if len(date) == 6:
            year = date[:2]
            month = Symbol.month_code(date[2:4])
            day = date[4:]
            return f"{year}{month}{day}"
        if len(date) == 9 or len(date) == 7:
            year, month, day = date[-2:], date[2:5], date[:2]
            months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
            month = Symbol.month_code(months.index(month) + 1)
            return f"{year}{month}{day}"

        raise ValueError(f"Unable to parse expiration date: {date}")

    @property
    def normalized(self) -> str:
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