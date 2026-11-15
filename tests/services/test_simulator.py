"""Fraud-risk model of the simulator and the historical seed."""
import numpy as np
import pytest

from tests.conftest import load_module

MODULES = ["sim_main", "bulk_seed"]


@pytest.fixture(scope="module")
def sim_main():
    return load_module("sim_main", "simulator/main.py")


@pytest.fixture(scope="module")
def bulk_seed():
    return load_module("bulk_seed", "simulator/bulk_seed.py")


@pytest.fixture
def mod(request, sim_main, bulk_seed, monkeypatch):
    """Each test runs against both generators with a small, uncapped base rate."""
    m = {"sim_main": sim_main, "bulk_seed": bulk_seed}[request.param]
    monkeypatch.setattr(m, "FRAUD_RATE", 0.01)
    return m


def both(fn):
    return pytest.mark.parametrize("mod", MODULES, indirect=True)(fn)


@both
def test_riskier_categories_get_higher_probability(mod):
    p = {m: mod.fraud_probability(m) for m in ["Steam", "Apple", "Netflix", "Airbnb", "Amazon", "Uber", "McDonald's", "Shell"]}
    assert p["Steam"] > p["Apple"] > p["Netflix"] > p["Airbnb"] > p["Amazon"] > p["Uber"] > p["McDonald's"] > p["Shell"]


@both
def test_night_hours_multiply_risk_by_two_and_a_half(mod):
    day = mod.fraud_probability("Amazon", hour=14)
    night = mod.fraud_probability("Amazon", hour=3)
    assert night == pytest.approx(day * 2.5)


@both
def test_hour_boundaries_of_the_night_window(mod):
    assert mod.fraud_probability("Amazon", hour=5) > mod.fraud_probability("Amazon", hour=6)
    assert mod.fraud_probability("Amazon", hour=0) == mod.fraud_probability("Amazon", hour=5)


@both
def test_new_accounts_are_riskier_than_established_ones(mod):
    ages = [10, 60, 200, 400]
    probs = [mod.fraud_probability("Amazon", account_age_days=a) for a in ages]
    assert probs == sorted(probs, reverse=True)
    assert probs[0] / probs[-1] == pytest.approx(3.0 / 0.8)


@both
def test_unknown_merchant_is_treated_as_baseline_retail(mod):
    assert mod.fraud_probability("Totally New Shop") == mod.fraud_probability("Amazon")


@both
def test_probability_is_capped(mod, monkeypatch):
    monkeypatch.setattr(mod, "FRAUD_RATE", 1.0)
    assert mod.fraud_probability("Steam", hour=2, account_age_days=5) == mod.MAX_FRAUD_RATE


@both
def test_every_merchant_has_a_category_with_a_multiplier(mod):
    assert set(mod.MERCHANT_VARIANTS) == set(mod.MERCHANT_CATEGORY)
    assert set(mod.MERCHANT_CATEGORY.values()) <= set(mod.CATEGORY_MULTIPLIER)


def test_stream_and_seed_share_the_same_risk_shape(sim_main, bulk_seed, monkeypatch):
    """The live stream must reinforce the same pattern the bootstrap established."""
    monkeypatch.setattr(sim_main, "FRAUD_RATE", 0.01)
    monkeypatch.setattr(bulk_seed, "FRAUD_RATE", 0.01)
    assert sim_main.CATEGORY_MULTIPLIER == bulk_seed.CATEGORY_MULTIPLIER
    assert sim_main.MERCHANT_CATEGORY == bulk_seed.MERCHANT_CATEGORY
    assert sim_main.MAX_FRAUD_RATE == bulk_seed.MAX_FRAUD_RATE
    for merchant in sim_main.MERCHANT_CATEGORY:
        for hour in (2, 14):
            for age in (10, 60, 200, 500):
                assert sim_main.fraud_probability(merchant, hour, age) == bulk_seed.fraud_probability(merchant, hour, age)


@both
def test_amount_anomalies_are_orders_of_magnitude_larger(mod, monkeypatch):
    monkeypatch.setattr(mod, "rng", np.random.default_rng(0))
    normal = np.median([mod.generate_amount(False, None) for _ in range(2000)])
    anomaly = np.median([mod.generate_amount(True, "amount_anomaly") for _ in range(2000)])
    assert anomaly > normal * 10
    # Other fraud patterns keep ordinary amounts.
    card_testing = np.median([mod.generate_amount(True, "card_testing") for _ in range(2000)])
    assert card_testing < anomaly / 10


def test_fraud_pattern_follows_the_merchant_category(bulk_seed):
    digital = {bulk_seed.category_pattern_for("Steam") for _ in range(200)}
    travel = {bulk_seed.category_pattern_for("Airbnb") for _ in range(200)}
    assert digital <= {"card_testing", "amount_anomaly"}
    assert travel <= {"geo_impossible", "amount_anomaly"}


def test_hourly_multiplier_is_one_of_the_documented_values(sim_main):
    assert sim_main.hourly_multiplier() in {0.3, 1.0, 2.5}
