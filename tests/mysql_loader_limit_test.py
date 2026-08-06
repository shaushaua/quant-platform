from datetime import date
from decimal import Decimal

from quant_platform.data.mysql_loader import MySQLLoader


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.params = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, sql, params):
        self.sql = " ".join(sql.split())
        self.params = params

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.rows[0] if self.rows else None


class FakeConnection:
    def __init__(self, rows):
        self.fake_cursor = FakeCursor(rows)

    def ping(self, reconnect=True):
        return None

    def cursor(self):
        return self.fake_cursor


def make_loader(rows):
    loader = MySQLLoader()
    loader._conn = FakeConnection(rows)
    return loader


def test_get_limit_prices_uses_actual_mkt_limit_schema():
    loader = make_loader([
        ("000001", "XSHE", date(2026, 8, 6), Decimal("11.000"),
         Decimal("9.000"), Decimal("10.000")),
        ("600000", "XSHG", date(2026, 8, 6), Decimal("12.100"),
         Decimal("9.900"), Decimal("11.000")),
    ])

    prices = loader.get_limit_prices("20260806")

    assert set(prices) == {"000001.XSHE", "600000.XSHG"}
    assert prices["600000.XSHG"].high_limit == 12.1
    assert prices["600000.XSHG"].low_limit == 9.9
    cursor = loader._conn.fake_cursor
    assert "SELECT TICKER_SYMBOL, EXCHANGE_CD, TRADE_DATE" in cursor.sql
    assert "LIMIT_UP_PRICE, LIMIT_DOWN_PRICE, PRE_CLOSE_PRICE" in cursor.sql
    assert cursor.params == ("2026-08-06",)


def test_get_limit_price_filters_explicit_exchange():
    loader = make_loader([
        ("600000", "XSHG", date(2026, 8, 6), Decimal("12.100"),
         Decimal("9.900"), Decimal("11.000")),
    ])

    price = loader.get_limit_price("600000.SH", "2026-08-06")

    assert price.code == "600000.XSHG"
    cursor = loader._conn.fake_cursor
    assert "EXCHANGE_CD = %s" in cursor.sql
    assert cursor.params == ("600000", "2026-08-06", "XSHG")


def test_get_all_securities_uses_ticker_and_exchange():
    loader = make_loader([
        ("000001", "XSHE"),
        ("600000", "XSHG"),
    ])

    codes = loader.get_all_securities("20260806")

    assert codes == ["000001.XSHE", "600000.XSHG"]
    cursor = loader._conn.fake_cursor
    assert "SELECT DISTINCT TICKER_SYMBOL, EXCHANGE_CD" in cursor.sql
    assert cursor.params == ("2026-08-06",)
