from __future__ import annotations

from typing import Any, List, Optional

from alphavault.ai._litellm import _import_litellm


def _response_attr(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _extract_text_from_response_content(content: Any) -> str:
    parts: List[str] = []

    def walk(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, str):
            text = node.strip()
            if text:
                parts.append(text)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)
            return

        text_value = _response_attr(node, "text")
        if isinstance(text_value, str):
            text = text_value.strip()
            if text:
                parts.append(text)

        content_value = _response_attr(node, "content")
        if content_value is not None and content_value is not node:
            walk(content_value)

    walk(content)
    return "\n".join(parts).strip()


def _extract_ai_text(response: Any, *, _seen_ids: Optional[set[int]] = None) -> str:
    if _seen_ids is None:
        _seen_ids = set()
    current_id = id(response)
    if current_id in _seen_ids:
        return ""
    _seen_ids.add(current_id)

    output_text = _response_attr(response, "output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    nested_response = _response_attr(response, "response")
    if nested_response is not None and nested_response is not response:
        nested_text = _extract_ai_text(nested_response, _seen_ids=_seen_ids)
        if nested_text:
            return nested_text

    output = _response_attr(response, "output")
    extracted_output_text = _extract_text_from_response_content(output)
    if extracted_output_text:
        return extracted_output_text

    choices = _response_attr(response, "choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        message = _response_attr(first_choice, "message")
        if message is not None:
            content = _response_attr(message, "content")
            extracted_choice_text = _extract_text_from_response_content(content)
            if extracted_choice_text:
                return extracted_choice_text

    if isinstance(response, dict):
        raw_choices = response.get("choices")
        if isinstance(raw_choices, list) and raw_choices:
            first_choice = raw_choices[0]
            if isinstance(first_choice, dict):
                message = first_choice.get("message")
                if isinstance(message, dict):
                    extracted_choice_text = _extract_text_from_response_content(
                        message.get("content")
                    )
                    if extracted_choice_text:
                        return extracted_choice_text
    return ""


def _extract_stream_text_delta(chunk: Any) -> str:
    choices = _response_attr(chunk, "choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        delta = _response_attr(first_choice, "delta")
        content = _response_attr(delta, "content")
        if isinstance(content, str) and content:
            return content

    event_type = str(_response_attr(chunk, "type") or "").strip().lower()
    delta = _response_attr(chunk, "delta")
    if "delta" in event_type:
        if isinstance(delta, str) and delta:
            return delta
        delta_text = _response_attr(delta, "text")
        if isinstance(delta_text, str) and delta_text:
            return delta_text
        delta_content = _response_attr(delta, "content")
        if isinstance(delta_content, str) and delta_content:
            return delta_content
    return ""


def _collect_streamed_ai_text(stream_response: Any, *, api_mode: str) -> str:
    chunks: List[Any] = []
    text_parts: List[str] = []

    for chunk in stream_response:
        chunks.append(chunk)
        text_delta = _extract_stream_text_delta(chunk)
        if text_delta:
            text_parts.append(text_delta)

    streamed_text = "".join(text_parts).strip()
    if streamed_text:
        return streamed_text

    if api_mode == "completion" and chunks:
        try:
            litellm = _import_litellm()
            rebuilt = litellm.stream_chunk_builder(chunks)
            rebuilt_text = _extract_ai_text(rebuilt)
            if rebuilt_text:
                return rebuilt_text
        except Exception:
            pass

    for chunk in reversed(chunks):
        chunk_text = _extract_ai_text(chunk)
        if chunk_text:
            return chunk_text
    return ""
