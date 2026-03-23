# 约定（给 Codex / AI）

## Streamlit 参数（重要）

- 不要再用 `use_container_width`：它在 2025-12-31 之后会被删。
- 统一用 `width`：
  - `use_container_width=True` → `width="stretch"`
  - `use_container_width=False` → `width="content"`
- 自检：改 UI 代码后跑一次 `rg -n "use_container_width"`，结果必须是空。

## Turso / libsql 写入（重要）

- 不要用 `engine.begin()` 来写 Turso（`sqlite+libsql`）。
  - 原因：有些 `libsql` / `libsql_experimental` 版本在 `commit()` / `rollback()` 可能触发 Rust `panic`（`Option::unwrap(None)`），进程会直接死。
- 写入统一用“autocommit 连接”，避免走 DBAPI 的 `commit()`：
  - 用 `alphavault/db/turso_db.py` 里的 `turso_connect_autocommit(engine)`
- 注意：就算你没手动 `rollback()`，`SQLAlchemy` 在 `conn.close()`（比如 `with conn:` 结束）也可能自动触发一次 `rollback` 做清理；`turso_connect_autocommit(engine)` 已在内部 patch 这条路径，避免触发 `libsql` 的 `panic`。
- 多条 SQL 需要“一起成功”的场景：用 `turso_savepoint(conn)` 包住（当前 libsql/Turso 下底层走 SQL `BEGIN/COMMIT/ROLLBACK`，不走 DBAPI `commit()`）。
- 自检：改 DB 写入代码后跑一次 `rg -n "engine\\.begin\\("`，尽量结果为 0（特别是 Turso 写入路径）。
