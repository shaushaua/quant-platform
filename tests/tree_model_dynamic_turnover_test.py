import math

from quant_platform.inference.turnover_budget import (
    dynamic_optimizer_max_turnover,
)


def test_full_investment_keeps_model_turnover_constraint():
    assert dynamic_optimizer_max_turnover(1.0, today_cash_flow=1, total_asset=100) == 0.35
    assert dynamic_optimizer_max_turnover(0.95, today_cash_flow=1, total_asset=100) == 0.35


def test_normal_cash_shortfall_within_base_budget_stays_at_base():
    assert dynamic_optimizer_max_turnover(0.90, today_cash_flow=1, total_asset=100) == 0.35
    assert dynamic_optimizer_max_turnover(0.67, today_cash_flow=1, total_asset=100) == 0.35


def test_large_deposit_adds_cash_deployment_capacity():
    got = dynamic_optimizer_max_turnover(
        0.30, today_cash_flow=4_900_000, total_asset=7_000_000)
    assert math.isclose(got, 0.72, rel_tol=0, abs_tol=1e-12)


def test_zero_position_can_build_full_portfolio():
    assert dynamic_optimizer_max_turnover(
        0, today_cash_flow=7_000_000, total_asset=7_000_000) == 1.0


def test_invalid_account_snapshot_falls_back_to_base_constraint():
    assert dynamic_optimizer_max_turnover(
        float("nan"), today_cash_flow=1, total_asset=100) == 0.35
    assert dynamic_optimizer_max_turnover(
        -1, today_cash_flow=1, total_asset=100) == 0.35


def test_explicit_one_turnover_override_remains_one():
    assert dynamic_optimizer_max_turnover(
        0.30, today_cash_flow=1, total_asset=100, base_turnover=1.0) == 1.0


def test_non_finite_configuration_falls_back_safely():
    got = dynamic_optimizer_max_turnover(
        0.50,
        today_cash_flow=1,
        total_asset=2,
        base_turnover=float("nan"),
        buffer=float("nan"),
    )
    assert math.isclose(got, 0.52, rel_tol=0, abs_tol=1e-12)


def test_budget_only_adds_capacity_needed_to_deploy_cash():
    assert math.isclose(
        dynamic_optimizer_max_turnover(
            0.6499, today_cash_flow=1, total_asset=1),
        0.3701,
        rel_tol=0,
        abs_tol=1e-12,
    )


def test_budget_is_continuous_at_dynamic_boundary():
    below = dynamic_optimizer_max_turnover(
        0.670001, today_cash_flow=1, total_asset=1)
    boundary = dynamic_optimizer_max_turnover(
        0.67, today_cash_flow=1, total_asset=1)
    above = dynamic_optimizer_max_turnover(
        0.669999, today_cash_flow=1, total_asset=1)
    assert below == 0.35
    assert boundary == 0.35
    assert math.isclose(above, 0.350001, rel_tol=0, abs_tol=1e-12)


def test_filtered_current_weights_drive_the_budget():
    # The broker account may be 90% invested while only 60% of current weights
    # remain in the optimizer universe. The budget must cover the actual 40%
    # optimizer gap, not the broker-level 10% cash ratio.
    assert math.isclose(
        dynamic_optimizer_max_turnover(0.60, today_cash_flow=1, total_asset=1),
        0.42,
        rel_tol=0,
        abs_tol=1e-12,
    )


def test_no_deposit_or_withdrawal_never_relaxes_turnover():
    assert dynamic_optimizer_max_turnover(
        0.30, today_cash_flow=0, total_asset=100) == 0.35
    assert dynamic_optimizer_max_turnover(
        0.30, today_cash_flow=-4_000_000, total_asset=100) == 0.35
    assert dynamic_optimizer_max_turnover(
        0.30, today_cash_flow=float("nan"), total_asset=100) == 0.35


def test_small_deposit_cannot_unlock_unrelated_existing_cash():
    assert math.isclose(
        dynamic_optimizer_max_turnover(
            0.30, today_cash_flow=100_000, total_asset=1_000_000),
        0.45,
        rel_tol=0,
        abs_tol=1e-12,
    )
