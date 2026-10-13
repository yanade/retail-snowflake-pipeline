from datetime import date

import pytest

from lookups import Store, stores_open_on

PAR_OUT = Store(5, "OUTLET", "EUR", date(2017, 5, 12), date(2025, 12, 31))


def test_closed_store_trades_on_its_last_day():
    assert stores_open_on([PAR_OUT], date(2025, 12, 31)) == [PAR_OUT]


def test_closed_store_does_not_trade_after_it():
    with pytest.raises(RuntimeError):
        stores_open_on([PAR_OUT], date(2026, 1, 1))


def test_unknown_opened_date_counts_as_open():
    store = Store(9, "ONLINE", "GBP", None, None)
    assert stores_open_on([store], date(2026, 10, 13)) == [store]
