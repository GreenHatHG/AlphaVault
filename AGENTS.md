# 约定（给 Codex / AI）

## Streamlit 参数（重要）

- 不要再用 `use_container_width`：它在 2025-12-31 之后会被删。
- 统一用 `width`：
  - `use_container_width=True` → `width="stretch"`
  - `use_container_width=False` → `width="content"`
- 自检：改 UI 代码后跑一次 `rg -n "use_container_width"`，结果必须是空。

