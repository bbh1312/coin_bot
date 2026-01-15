# DB 기반 주문/리포트 로직 흐름 정리

이 문서는 현재 코드 구조(`engine_runner.py`, `executor.py`, 관리모드 포함)를 기준으로,
DB 기록을 중심으로 한 주문/청산/리포트 흐름과 DB 누락 시 보정 흐름을 정리한다.

## 1) 핵심 테이블 개요

- `engine_signals`: 엔진 시그널 발생 기록 (근거/메타 포함)
- `orders`: 주문 생성/상태 변경 기록
- `fills`: 체결(부분체결 포함) 기록
- `positions`: 포지션 스냅샷 또는 재계산 결과
- (선택) `balance_snapshots`, `pnl_realized`, `price_snapshots`

`fills`가 1차 사실이며, 리포트/손익은 `fills` 기반으로 재계산한다.

## 2) 정상 흐름 (DB 정상 적재)

1. 엔진 시그널 생성
   - `engine_runner.py`에서 시그널 발생
   - `engine_signals`에 기록 (심볼, 사이드, 엔진, 사유, 메타)

2. 주문 생성
   - `executor.py`에서 주문 전송
   - 응답을 `orders`에 기록 (order_id, client_order_id, qty, price, status=NEW)

3. 체결 수신
   - WS/REST 체결 이벤트 수신
   - `fills`에 기록 (fill_id, order_id, qty, price, fee, ts)
   - `orders` 상태 업데이트 (PARTIALLY_FILLED / FILLED)

4. 포지션 갱신
   - `fills` 누적 기반으로 포지션 재계산
   - `positions` 스냅샷 업데이트

5. 관리모드 청산
   - 관리모드가 `engine_signals` 또는 `positions` 기반으로 청산 결정
   - 청산 주문 생성 → `orders` 기록
   - 체결 수신 → `fills` 기록
   - `positions` 갱신 및 리포트 반영

## 3) DB 누락 발생 시 보정 흐름

A. 시그널은 있는데 주문 기록 누락
- 관리모드가 현재 주문/포지션을 조회
- 거래소 주문 상태 확인 후 `orders` 복원 기록
- `engine_signals`과 주문 매핑 보정

B. 주문은 있는데 체결 기록 누락
- 관리모드가 주문 ID 기준 체결 내역 조회
- 누락된 체결을 `fills`에 적재
- `positions` 재계산 및 갱신

C. 포지션은 열려 있는데 시그널/주문 기록 없음
- 관리모드가 현재 포지션 조회
- 복원 이벤트로 `engine_signals`/`orders` 최소 기록 생성
- 이후 체결 내역 수집 → `fills` 적재
- 리포트 계산은 `fills` 기반으로 정정

D. 청산 완료됐는데 DB 미반영
- 관리모드가 포지션=0 확인
- 마지막 주문/체결 조회 후 `orders`/`fills` 보정
- `positions` 스냅샷을 닫힘 상태로 업데이트
- 필요 시 `engine_signals`에 auto_exit/manual_close 기록

## 4) 운영 원칙

- idempotency: `order_id`, `fill_id`는 유니크 키로 중복 저장 방지
- 소스 우선순위: WS(실시간) → REST(보정)
- 체결 기준 PnL: `fills`가 1차 사실, `positions`/리포트는 재계산 결과
- 타임스탬프: 거래소 ts를 기준으로 저장

## 5) 요약 흐름 (한 줄 버전)

시그널 생성 → `engine_signals` 기록 → 주문 생성 → `orders` 기록 → 체결 수신 → `fills` 기록 → `positions` 갱신 → 관리모드 청산 → 주문/체결 기록 → `positions` 정리.

DB 누락 시에는 관리모드가 거래소 상태를 기준으로 역으로 복원 기록한다.

## 6) DB 훅 위치 매핑 (현재 코드 기준)

- 시그널 생성
  - 엔진별 ENTRY_READY 분기 직전이 정확한 기록 지점
  - 공통 후보: `engine_runner.py`의 `_log_trade_entry()` 직전/직후
  - `engine_signals`에 심볼/사이드/엔진/사유/메타 기록

- 주문 생성 (Entry/Exit/DCA)
  - `executor.py`에서 실제 주문 응답을 받는 위치가 1차 사실
  - Entry: `short_market()`, `short_limit()`, `long_market()`
  - Exit: `close_short_market()`, `close_long_market()`
  - DCA: `dca_short_if_needed()`, `dca_long_if_needed()`
  - Cancel: `cancel_open_orders()`, `cancel_stop_orders()`, `cancel_conditional_by_side()`
  - `orders` 테이블 기록은 이 지점에서 수행

- 체결 기록 (fills)
  - WS 체결 스트림이 없으므로 주문 응답에 포함된 filled/average/fee 기반 기록 + REST 보정 필요
  - 관리모드/백필 루틴에서 `fetch_my_trades`로 체결 복원

- 청산/리포트 갱신
  - 공통 종료 처리: `engine_runner.py`의 `_close_trade()`
  - 이 지점에서 `positions`/`pnl` 업데이트 훅을 둔다

- 엔진 라벨/시그널 보정
  - `engine_runner.py`의 `_append_entry_event()`가 현재 엔진 매핑 소스
  - `engine_signals`와 병행 기록하거나 대체 가능

## 7) 관리모드 흐름 + DB 보정 로직

관리모드는 2가지 흐름이 있다.

- 메인 관리 루프: `engine_runner.py`의 `_run_manage_cycle()`
- WS 관리모드: `manage_ws.py` (테스트용, 별도 실행)

### 7-1) 메인 관리 루프 흐름

1. `state` 기준 `in_pos` 심볼 순회
2. 거래소 포지션 확인
3. 포지션 소멸 시 수동 청산 처리 → `_close_trade()`
4. 자동 청산 조건 충족 시 시장가 청산 → `_close_trade()`
5. DCA 조건 충족 시 추가 진입

DB 관점 보정 지점:
- 포지션 소멸 시 `orders/fills` 누락 여부 확인
- 청산 주문 발생 시 `orders` 기록 + 체결 backfill
- `_close_trade()`에서 `positions`/`pnl` 업데이트

### 7-2) WS 관리모드 흐름

1. `entry_events` 로그를 읽어 엔진 매핑 (`_drain_entry_events`)
2. 포지션/가격 체크 → TP/SL/수동 청산 판단
3. 청산 시 시장가 주문 + `_close_trade()`
4. 텔레그램/리포트 반영

DB 관점 보정 지점:
- `entry_events` 로딩 시 `engine_signals` 보정 가능
- 청산 시 `orders/fills` 누락 복원 후 `positions` 갱신

### 7-3) DB 누락 보정 시나리오

- Case 1: 포지션 존재 + entry 기록 없음
  - `orders` 복원(열린 주문/주문 조회)
  - `fills` backfill (`fetch_my_trades`)
  - `engine_signals` 복원(엔진 라벨 추정)

- Case 2: 주문은 있는데 체결 기록 없음
  - 주문 상태 확인 후 체결 내역 조회 → `fills` 적재

- Case 3: 청산 완료됐는데 DB 미반영
  - 포지션=0 확인 → `orders/fills` 복원 후 `_close_trade()` 처리

- Case 4: 엔진 라벨/시그널 누락
  - `entry_events` 또는 `state.meta.reason` 기반으로 `engine_signals` 보정

## 8) 훅 삽입 위치 (라인 기준)

- 엔트리/시그널 기록 핵심
  - `engine_runner.py:1037` `_log_trade_entry()` -> `engine_signals` 기록 지점
  - `engine_runner.py:4801` `_append_entry_event()` -> 엔진 라벨/매핑 소스

- 청산/리포트 최종 처리
  - `engine_runner.py:1126` `_close_trade()` -> `positions`/`pnl` 갱신 훅

- 메인 관리모드 루프 (수동/자동 청산 감지)
  - `engine_runner.py:3910` `_run_manage_cycle()`
  - `engine_runner.py:3958` 수동 청산 처리 분기
  - `engine_runner.py:4052`, `engine_runner.py:4121` 숏 TP/SL 청산
  - `engine_runner.py:4278`, `engine_runner.py:4332` 롱 TP/SL 청산

- 엔트리 주문 발생(대표 지점)
  - `engine_runner.py:806`, `engine_runner.py:848`, `engine_runner.py:2184`, `engine_runner.py:2266`
  - `engine_runner.py:2459`, `engine_runner.py:2518`, `engine_runner.py:2791`, `engine_runner.py:2984`
  - `engine_runner.py:7238`, `engine_runner.py:7428`, `engine_runner.py:7618`

- 주문/청산 API 훅(1차 사실)
  - `executor.py:624` `short_market()`
  - `executor.py:705` `short_limit()`
  - `executor.py:789` `long_market()`
  - `executor.py:866` `close_short_market()`
  - `executor.py:885` `close_long_market()`
  - `executor.py:903` `cancel_open_orders()`
  - `executor.py:920` `cancel_stop_orders()`
  - `executor.py:1001` `cancel_conditional_by_side()`
  - `executor.py:1077` `dca_short_if_needed()` (내부에서 `short_market()` 호출)
  - `executor.py:1124` `dca_long_if_needed()` (내부에서 `long_market()` 호출)

- WS 관리모드(보정/청산)
  - `manage_ws.py:100` `_drain_entry_events()`
  - `manage_ws.py:254` `_manual_close_long()`
  - `manage_ws.py:303` `_manual_close_short()`
  - `manage_ws.py:352` `_handle_long_tp()`
  - `manage_ws.py:413` `_handle_short_tp()`
  - `manage_ws.py:474` `_handle_long_sl()`
  - `manage_ws.py:533` `_handle_short_sl()`

## 9) 보정 루틴 상세 흐름 (관리모드/배치)

1. 동기화 커서 준비
   - DB에 `last_sync_ts` 또는 `last_trade_id` 저장
   - 심볼별 커서가 있으면 중복 체결 방지에 유리

2. 거래소 스냅샷 수집
   - `fetch_positions()` 포지션 스냅샷
   - `fetch_open_orders()` 미체결 주문
   - `fetch_orders()` 또는 `fetch_my_trades()` 체결 내역

3. `orders` 복원 (upsert)
   - `order_id` 유니크 키
   - 상태(NEW/PARTIALLY_FILLED/FILLED/CANCELED) 최신값으로 갱신

4. `fills` 복원 (backfill)
   - `trade_id`(fill_id) 유니크 키
   - 누락 체결만 적재

5. `positions` 재계산/갱신
   - `fills` 누적 기반 avg entry / size 계산
   - 거래소 포지션과 불일치 시 `reconcile_status` 표시

6. 청산 누락 보정
   - DB는 open인데 거래소 포지션=0 -> `orders/fills` 보정 후 청산 처리
   - 거래소 포지션 존재인데 DB에 없음 -> 복원 엔트리 생성

7. 엔진 라벨/시그널 보정
   - `entry_events` 또는 `state.meta.reason` 기반으로 `engine_signals` 복원

8. 동기화 커서 저장
   - 마지막 처리한 `trade_id`/`ts` 저장

## 10) DB 스키마 확정안 (SQLite 기준)

- `engine_signals`
  - `id` PK, `ts`, `symbol`, `side`, `engine`, `reason`, `meta_json`
  - 인덱스: `(ts, symbol)`

- `orders`
  - `order_id` PK, `ts`, `symbol`, `side`, `order_type`, `status`, `price`, `qty`, `engine`, `client_order_id`, `raw_json`
  - 인덱스: `(ts, symbol)`, `(symbol, status)`

- `fills`
  - `fill_id` PK, `ts`, `order_id`, `symbol`, `side`, `price`, `qty`, `fee`, `fee_asset`, `raw_json`
  - 인덱스: `(ts, symbol)`, `(order_id)`

- `positions`
  - `id` PK, `ts`, `symbol`, `side`, `qty`, `avg_entry`, `unreal_pnl`, `realized_pnl`, `source`
  - 인덱스: `(ts, symbol)`

- `cancels`
  - `id` PK, `ts`, `order_id`, `symbol`, `side`, `reason`, `raw_json`
  - 인덱스: `(ts, symbol)`
