# Atlas RS Fail Short 엔진 설명

## 개요
- 목적: Atlas 약세/중립 환경에서 **단기 반등 실패(페이드)**가 확인된 경우만 숏 진입 신호를 생성.
- 진입 방식: **LTF 확정봉 기준**(마지막 완성봉)에서만 신호를 발행.
- 포지션 관리: TP/SL은 공통 모듈에서 처리, 엔진은 신호만 생성.

## 데이터/타임프레임
- LTF: `15m` (config: `ltf_tf=15m`)
- LTF 데이터 윈도우: `ltf_limit=200`
- LTF는 `iloc[:-1]`로 **미완성봉 제외** 후 계산.

## Atlas 게이트 (진입 전 필터)
Atlas 게이트는 `compute_global_gate()` + `compute_atlas_local()` 결과로 평가.

필수 조건:
- `atlas.regime` 존재
- `atlas.score` 존재
- `atlas.dir` 존재 (BULL/BEAR/NEUTRAL)

제외 조건:
- `regime == bull_extreme` 이고 `score >= 80` → **차단**
- `regime`가 허용 집합에 없으면 **차단**
  - 허용: `{bear, range, chaos_range, chaos_vol}`
- `dir != BEAR` 이면서 `score > 60` → **차단**
  - 즉, `BEAR`가 아니면 고득점(>60)일 때 차단

주의:
- `min_score=50`은 **진입 필수 조건이 아니라 리스크 태그 용도**
- `rs_z`, `rs_z_slow` 등은 **직접 차단 조건에 없음**

## 기본 데이터 조건
- ATR/EMA/RSI/BB/MACD/볼륨 SMA 계산 가능해야 함
- 최소 데이터 길이: `max(ema_len, atr_len+2, rsi_len+2, bb_len+2, macd_slow+macd_signal+2, vol_sma_len+2)`
- ATR 비율 필터: `atr_now / close_now >= 0.002` (`atr_min_ratio`)
- 거래대금 필터: `min_quote_volume=0` (기본 OFF)

## 위치/셋업 조건 (Fade 위치)
- EMA 기준: `ema_len=20`
- ATR 기준: `atr_len=14`

조건:
1) **가격 위치**
- `close_now > ema20`
- `high_now >= ema20 + (atr * 0.35)` (`fade_atr_mult`)

2) **EMA20 대비 고점 거리 범위**
- `high_now - ema20`가 `atr*0.7` 이상 (`fade_atr_min_mult`)
- `high_now - ema20`가 `atr*1.3` 이하 (`fade_atr_max_mult`)

## BB 터치 (옵션)
- 기본값: `bb_touch_required = False` (기본 OFF)
- ON일 때 조건:
  - `high_now >= bb_upper - (bb_width * 0.1)` (`bb_upper_eps=0.1`)

## RSI 필터
- `rsi_len = 14`
- 범위: `45 <= rsi_now <= 60` (`rsi_min`, `rsi_max`)
- 과열 차단: `rsi_now >= 70` (`rsi_block`) → **차단**

## 트리거 조건 (반등 실패 확인)
아래 4개 중 **정확히 2개**를 만족해야 진입 (`trigger_min=2`, `trigger_exact=True`).

- 윗꼬리 비중: `upper_wick_ratio >= 0.55` (`wick_ratio_min`)
- RSI 하락: `rsi_now < rsi_prev`
- MACD 약화: `macd_hist_now < macd_hist_prev`
- 음봉: `close_now < open_now`

## 진입 조건 요약
- Atlas 게이트 통과
- ATR/BB/RSI/EMA 등 데이터 조건 충족
- 위치 조건(EMA20 상방 + ATR 거리 범위) 충족
- RSI 범위 충족 (45~60, 70 이상 차단)
- 트리거 4개 중 **정확히 2개** 만족

→ 모두 만족 시 `ENTRY_READY` 발생

## 쿨다운/중복 방지
- 심볼별 쿨다운: `cooldown_minutes = 45`
- 같은 캔들 기반 중복 진입 방지: `fade_id = last_ts:fade`로 1회 제한

## 시그널 메타 (핵심 필드)
- `engine`: `atlas_rs_fail_short`
- `side`: `SHORT`
- `timeframe`: `15m`
- `entry_price`: 확정봉 종가
- `risk_tags`:
  - `SCORE_LOW` (score < 50)
  - `RS_EXTREME` (rs_z <= -3.0)
  - `VOL_HIGH` (vol_ratio >= 10)
  - `BB_TOUCH` (BB 접촉)
  - `MACD_DECAY` (히스토그램 감소)
- `tech`:
  - `ema20`, `atr`, `rsi`, `macd_hist`, `bb_upper/bb_lower`, `wick_ratio`
  - `confirm_type`, `trigger_bits`, `high_minus_ema20`

## 참고 사항
- HTF/MTF 별도 게이트는 없음 (LTF 단일 로직)
- `size_mult`는 현재 1.0 고정
- 실제 주문 실행 전에는 공통 게이트(포지션 중복/락/최대포지션/실주문 ON/OFF 등)가 추가로 적용됨
