from __future__ import annotations

import pytest

from alphavault.ai.tag_validate import (
    AiTagValidationError,
    validate_assertion_row,
    validate_topic_prompt_v3_ai_result,
)
from alphavault.ai.topic_prompt_v3 import TOPIC_PROMPT_VERSION


def test_topic_prompt_v3_ok_stock_code() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "stock:600519.SH",
                "action": "trade.buy",
                "action_strength": 2,
                "confidence": 0.8,
                "stock_codes": ["600519.SH"],
                "stock_names": ["贵州茅台"],
                "industries": ["白酒"],
                "commodities": [],
                "indices": [],
            }
        ]
    }
    validate_topic_prompt_v3_ai_result(parsed)


def test_topic_prompt_v3_ok_stock_code_hk() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "stock:0700.HK",
                "action": "trade.watch",
                "action_strength": 1,
                "confidence": 0.6,
                "stock_codes": ["0700.HK"],
            }
        ]
    }
    validate_topic_prompt_v3_ai_result(parsed)


def test_topic_prompt_v3_ok_stock_code_us() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "stock:AMZN.US",
                "action": "trade.watch",
                "action_strength": 1,
                "confidence": 0.6,
                "stock_codes": ["AMZN.US"],
            }
        ]
    }
    validate_topic_prompt_v3_ai_result(parsed)


def test_topic_prompt_v3_ok_stock_name_without_codes() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "stock:长电",
                "action": "trade.watch",
                "action_strength": 1,
                "confidence": 0.6,
                # stock_codes missing -> treated as []
                "stock_names": ["长电"],
                "industries": [],
                "commodities": [],
                "indices": [],
            }
        ]
    }
    validate_topic_prompt_v3_ai_result(parsed)


def test_topic_prompt_v3_reject_stock_name_with_codes() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "stock:长电",
                "action": "trade.watch",
                "action_strength": 1,
                "confidence": 0.6,
                "stock_codes": ["600519.SH"],
            }
        ]
    }
    with pytest.raises(AiTagValidationError):
        validate_topic_prompt_v3_ai_result(parsed)


def test_topic_prompt_v3_reject_stock_multiple_values() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "stock:长电,紫金",
                "action": "trade.watch",
                "action_strength": 1,
                "confidence": 0.6,
            }
        ]
    }
    with pytest.raises(AiTagValidationError):
        validate_topic_prompt_v3_ai_result(parsed)


def test_topic_prompt_v3_reject_stock_code_without_suffix() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "stock:600519",
                "action": "trade.watch",
                "action_strength": 1,
                "confidence": 0.6,
            }
        ]
    }
    with pytest.raises(AiTagValidationError):
        validate_topic_prompt_v3_ai_result(parsed)


def test_topic_prompt_v3_reject_list_value_with_comma() -> None:
    parsed = {
        "items": [
            {
                "topic_key": "industry:电力",
                "action": "view.bullish",
                "action_strength": 1,
                "confidence": 0.6,
                "industries": ["电力,黄金"],
            }
        ]
    }
    with pytest.raises(AiTagValidationError):
        validate_topic_prompt_v3_ai_result(parsed)


def test_db_row_ok_stock_name_empty_codes() -> None:
    row = {
        "topic_key": "stock:长电",
        "action": "trade.watch",
        "action_strength": 1,
        "confidence": 0.6,
        "stock_codes_json": "[]",
        "stock_names_json": '["长电"]',
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
    }
    validate_assertion_row(row, prompt_version=TOPIC_PROMPT_VERSION)


def test_db_row_reject_stock_name_with_codes() -> None:
    row = {
        "topic_key": "stock:长电",
        "action": "trade.watch",
        "action_strength": 1,
        "confidence": 0.6,
        "stock_codes_json": '["600519.SH"]',
        "stock_names_json": "[]",
        "industries_json": "[]",
        "commodities_json": "[]",
        "indices_json": "[]",
    }
    with pytest.raises(AiTagValidationError):
        validate_assertion_row(row, prompt_version=TOPIC_PROMPT_VERSION)
