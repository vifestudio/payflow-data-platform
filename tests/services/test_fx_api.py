"""FX rates service: deterministic rates, query validation."""
from datetime import date

import pytest
from fastapi.testclient import TestClient

from tests.conftest import load_module


@pytest.fixture(scope="module")
def fx():
    return load_module("fx_api", "sources/fx-api/main.py")


@pytest.fixture(scope="module")
def client(fx):
    return TestClient(fx.app)


def test_rate_is_deterministic_across_calls(fx):
    d = date(2026, 3, 15)
    assert fx.rate_for_date("EUR", d) == fx.rate_for_date("EUR", d)


def test_rate_stays_within_one_and_a_half_percent_of_base(fx):
    for currency, base in fx.BASE_RATES.items():
        for day in range(1, 29):
            rate = fx.rate_for_date(currency, date(2026, 2, day))
            assert abs(rate / base - 1) <= 0.015 + 1e-6


def test_rate_changes_from_one_day_to_the_next(fx):
    rates = {fx.rate_for_date("EUR", date(2026, 4, day)) for day in range(1, 11)}
    assert len(rates) > 1


def test_currencies_get_different_offsets(fx):
    d = date(2026, 5, 5)
    ratios = {c: fx.rate_for_date(c, d) / base for c, base in fx.BASE_RATES.items()}
    assert len({round(v, 6) for v in ratios.values()}) > 1


def test_rates_endpoint_lists_every_currency_against_usd(client, fx):
    body = client.get("/rates", params={"on": "2026-06-01"}).json()
    assert {r["currency"] for r in body} == set(fx.BASE_RATES)
    assert all(r["base"] == "USD" and r["date"] == "2026-06-01" for r in body)


def test_rates_endpoint_defaults_to_today(client):
    body = client.get("/rates").json()
    assert {r["date"] for r in body} == {str(date.today())}


def test_rates_endpoint_rejects_a_bad_date(client):
    assert client.get("/rates", params={"on": "not-a-date"}).status_code == 422


def test_history_returns_one_row_per_currency_per_day(client, fx):
    body = client.get("/rates/history", params={"days": 3}).json()
    assert len(body) == 3 * len(fx.BASE_RATES)
    assert len({r["date"] for r in body}) == 3


@pytest.mark.parametrize("days", [0, -1, 366])
def test_history_rejects_out_of_range_days(client, days):
    assert client.get("/rates/history", params={"days": days}).status_code == 422


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}
