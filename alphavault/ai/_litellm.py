from __future__ import annotations

import warnings


def _import_litellm():
    _suppress_pydantic_serializer_warning()
    try:
        import litellm  # type: ignore
    except Exception as exc:
        raise RuntimeError("litellm_not_installed") from exc

    if hasattr(litellm, "suppress_debug_info"):
        # Prevent LiteLLM from printing "Give Feedback / Get Help" debug info on exceptions.
        litellm.suppress_debug_info = True

    return litellm


PYDANTIC_SERIALIZER_WARNING_MESSAGE_RE = r"^Pydantic serializer warnings:"
PYDANTIC_MAIN_MODULE_RE = r"^pydantic\.main$"


def _suppress_pydantic_serializer_warning() -> None:
    """
    Silence a noisy warning from pydantic serializer.

    It shows up when LiteLLM returns a usage dict that doesn't exactly match the
    expected pydantic model type.
    """
    warnings.filterwarnings(
        "ignore",
        message=PYDANTIC_SERIALIZER_WARNING_MESSAGE_RE,
        category=UserWarning,
        module=PYDANTIC_MAIN_MODULE_RE,
    )


def _resolve_litellm_model_name(model_name: str, base_url: str) -> str:
    resolved_model_name = str(model_name or "").strip()
    if not resolved_model_name:
        return ""
    if "/" in resolved_model_name:
        return resolved_model_name
    if str(base_url or "").strip():
        return f"openai/{resolved_model_name}"
    return resolved_model_name
