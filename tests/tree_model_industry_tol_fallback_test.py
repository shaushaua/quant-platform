import importlib
import sys
import types


def _reload_tree_model_inference(monkeypatch, base="0.05", fallbacks="0.03,0.10,0.08,0.10,0.30"):
    monkeypatch.setenv("OPTIMIZER_INDUSTRY_EXPOSURE_TOL", base)
    monkeypatch.setenv("OPTIMIZER_INDUSTRY_EXPOSURE_TOL_FALLBACKS", fallbacks)
    fake_calc = types.ModuleType("calc_predict_tree_model")
    fake_calc.POSITION_COLUMNS = []
    fake_calc.build_optimizer_exposure_frame = lambda *args, **kwargs: None
    fake_calc.extract_current_position_weights = lambda *args, **kwargs: []
    fake_calc.normalize_benchmark_weights = lambda *args, **kwargs: None
    fake_calc.normalize_daily_feature_czhou1_keys = lambda *args, **kwargs: None
    fake_calc.normalize_date_column = lambda *args, **kwargs: None
    fake_calc.normalize_universe_codes = lambda *args, **kwargs: None
    fake_calc.predict_tree_model = lambda *args, **kwargs: None
    fake_calc.resolve_signal_date = lambda *args, **kwargs: None
    monkeypatch.setitem(sys.modules, "calc_predict_tree_model", fake_calc)
    import quant_platform.inference.tree_model_inference as tmi

    return importlib.reload(tmi)


def test_industry_tol_candidates_are_strict_first_then_relaxed(monkeypatch):
    tmi = _reload_tree_model_inference(monkeypatch)

    assert tmi._optimizer_industry_tol_candidates() == [0.05, 0.08, 0.10, 0.30]


def test_industry_tol_candidates_keep_base_when_fallbacks_invalid(monkeypatch):
    tmi = _reload_tree_model_inference(monkeypatch, base="0.05", fallbacks="bad,-1,0.03")

    assert tmi._optimizer_industry_tol_candidates() == [0.05]


def test_retryable_error_detection_is_limited_to_optimizer_failures(monkeypatch):
    tmi = _reload_tree_model_inference(monkeypatch)

    assert tmi._is_optimizer_retryable_error(RuntimeError("solver status infeasible"))
    assert tmi._is_optimizer_retryable_error(RuntimeError("optimizer_retryable: no positions"))
    assert not tmi._is_optimizer_retryable_error(KeyError("ID_QI"))
    assert not tmi._is_optimizer_retryable_error(ValueError("daily_basic schema mismatch"))
