# Manage Mode Unification Plan (Draft)

## Goals
- Main mode generates signals only; no DB access.
- Manage mode is the single source for execution, DB writes, reconciliation, and Telegram alerts.
- Manual entries/exits are detected in manage mode and recorded/alerted there.

## High-Level Flow
- Main mode: create signal -> enqueue request only.
- Manage mode: consume queue -> execute order -> update DB/state -> send alert.
- Restart recovery: manage mode rehydrates queue from on-disk spool and/or DB backlog.

## Queue Design (No DB Access From Main Mode)
- Use an in-memory map for fast lookup + an append-only spool file for durability.
- Main mode writes requests to the spool file and memory map only.
- Manage mode tails the spool, validates requests, and marks status in memory.
- Periodic reconciliation loads unprocessed requests from the spool.

## Request Lifecycle
- Status: `pending` -> `executing` -> `done` or `failed`.
- Dedup key: (symbol, side, request_ts, strategy_id) to avoid duplicate executions.
- Failure handling: retry with backoff or mark `failed` and notify.

## Manual Entry/Exit Handling
- New entry detection: position qty transitions 0 -> >0.
- DCA detection: qty increases while already >0.
- Manual entry alert: use existing entry alert format, engine `MANUAL`, reason `manual_entry`.
- Manual DCA alert: reuse existing DCA alert format.
- Manual exit alert: existing exit alert flow (already in manage mode).

## Data/State Updates (Manage Mode Only)
- Create trade log and entry events upon execution or manual entry detection.
- Update state/in_pos, last_entry, order IDs, and DB records in manage mode.
- Remove or disable duplicate execution/recording from main mode.

## Reconciliation Strategy
- On startup: parse spool, enqueue pending requests.
- Periodic: verify queue vs DB/open positions; enqueue missing pending items.
- Ensure idempotent execution to prevent duplicate orders.

## Open Questions
- Spool file format: JSON lines with minimal fields (symbol, side, ts, strategy_id, params).
- Retention: how long to keep completed requests before cleanup.
- Backoff/timeout policy for stuck `executing` requests.
