# Atlas Engine (Swaggy Atlas Gate)

이 문서는 현재 코드에 구현된 Atlas 엔진의 동작과 수치 기준을 정리한 것이다.  
기준값은 `engines/atlas/atlas_engine.py` 및 `engines/swaggy_atlas_lab/config.py` 기준이다.

## 1) 사용 데이터/타임프레임

- 기준 심볼: `BTC/USDT:USDT` (`ref_symbol`)
- 로컬 심볼/지표 계산: 15m OHLCV (최소 200봉 요청, 마지막 봉 제외)
- 글로벌 게이트(레짐 판단): 15m EMA20/EMA60 + ATR14 기반
- RS/상관/베타 계산: 15m 로그수익률

## 2) 글로벌 게이트 (market regime)

구간: 15m EMA20/EMA60, ATR14 기준

- EMA20 > EMA60 그리고 `trend_norm >= 0.35` -> `regime=bull`
  - `allow_long=1`, `allow_short=0`, `long_requires_exception=0`
- EMA20 < EMA60 그리고 `trend_norm <= -0.35` -> `regime=bear`
  - `allow_short=1`, `allow_long=0`, `long_requires_exception=1`
- 그 외:
  - `abs(trend_norm) <= 0.20` 이고 `atr_ratio < 0.85` -> `regime=chaos_range`
    - `allow_long=0`, `allow_short=0`, 롱/숏 모두 예외 필요
  - 나머지 -> `regime=chaos_vol`
    - `allow_long=1`, `allow_short=1`, 롱/숏 모두 예외 필요

계산식:
- `trend_norm = (EMA20 - EMA60) / denom`
  - denom = ATR14 (존재 시), 아니면 last_close
- `atr_ratio = ATR14 / SMA(ATR14, 20)`

TTL 캐시:
- 글로벌 게이트는 `ttl_sec=600` 동안 캐시 재사용

## 3) 로컬 RS/상관/베타 계산

기본 파라미터:
- `rs_n=12`, `rs_n_slow=36`
- `corr_m=32`, `corr_m_slow=96`

계산:
- `rs_fast = sum(alt_rets[-rs_n:]) - sum(btc_rets[-rs_n:])`
- `rs_z_fast = rs_fast / (sigma_btc_fast * sqrt(rs_n))`
- `corr_fast = corr(alt_rets[-corr_m:], btc_rets[-corr_m:])`
- `beta_fast = cov(alt_rets, btc_rets) / var(btc_rets)` (클램프 -3~+3)
- slow도 동일한 방식으로 `rs_slow`, `rs_z_slow`, `corr_slow`, `beta_slow`
- `vol_ratio = last_volume / SMA(volume, 20)`

## 4) RS/독립성/거래량 통과 기준

RS 통과:
- `rs_pass` = `rs_fast >= 0.0060` 또는 `rs_z_fast >= 0.75`
- `rs_pass_slow`도 동일 (`rs_slow`, `rs_z_slow`)
- 최종 RS 통과: `rs_pass_fast or rs_pass_slow`
- 강한 RS: fast, slow 모두 `rs_strong>=0.0120` 또는 `rs_z_strong>=1.25`

독립성 통과:
- `indep_pass`:
  - `corr <= 0.55` 또는 `beta <= 0.80` (fast/slow 중 하나)
- `indep_strong`:
  - `corr <= 0.35` 또는 `beta <= 0.60` (fast/slow 모두)
- 위험 독립성:
  - `corr >= 0.75` 또는 `beta >= 1.10` (fast/slow 모두)

거래량 통과:
- `vol_ratio >= 1.3` (pass)
- override 조건에서만 `vol_ratio >= 1.5`

## 5) 예외(롱) 허용 조건

예외 스코어:
- 통과 항목: RS, INDEP, VOL, SWAGGY(강도/거리/트리거)
- 스코어 >= 3 (`exception_min_score=3`)이면 롱 예외 허용

SWAGGY 강도 조건(로컬 decision.debug):
- `trigger in ("RECLAIM","REJECTION")`
- `strength >= 0.50`
- `dist_pct <= cap`
  - cap = `max_dist_reclaim` if trigger=RECLAIM else `max_dist`

Chaos regime 추가 조건:
- `regime=chaos_vol`이면 `strength >= 0.60`

Override 조건:
- `rs_fast >= 0.015`
- `vol_ratio >= 1.5`
- `strength >= 0.55`
- `dist_pct <= 0.009`

롱 사이즈 멀티:
- `regime=bear`이면 기본 `0.3`
  - `rs_strong & indep_strong`이면 `0.5`
  - `indep_risk`면 `0.25`
- 그 외 `0.5`, `indep_risk`면 `0.25`

## 6) 숏 품질 필터

기본:
- `vol_ratio >= 1.0` 아니면 숏 차단 (reason=VOL_TOO_LOW)

`regime=bear`일 때:
- corr/beta 존재 + (corr>=0.30 and beta>=0.80)면 OK
- corr가 낮으면(rs_fast 또는 rs_slow <= -0.01)면 OK
- 그 외는 `RS_WEAK` 또는 `CORR_WEAK`

`regime=chaos_range`일 때:
- `vol_ratio >= 1.2` 미만이면 차단

차단 시 subreason:
- `VOL_LOW`, `METRIC_MISSING`, `RSZ_BAD`, `CORR_BAD`, `BETA_BAD`

## 7) 롱 품질 강제 필터 (bear/chaos)

조건:
- `rs_z >= 1.2`
- `beta <= 0.55`
- `vol_ratio >= 1.0`
- 트리거 강도:
  - `RECLAIM >= 0.55`
  - `RETEST >= 0.50`
  - `REJECTION >= 0.60`
  - `SWEEP`는 항상 실패

불충족 시 롱 차단:
`RSZ_LOW`, `BETA_HIGH`, `VOL_LOW`, `SWAGGY_WEAK`

## 8) 테스트 설정 (atlas_test)

`atlas_test/atlas_config.py` 기준:
- `htf_tf=1h`, `ltf_tf=15m`
- `supertrend_period=10`, `supertrend_mult=3.0`
- `atr_period=14`, `atr_sma_period=50`, `atr_mult=1.2`
- `vol_sma_period=20`, `vol_mult=1.3`
- `strong_score_threshold=70`, `instant_score_min=85`
- `summary_top_n=5`, `fabio_universe_top_n=40`
- `min_quote_volume=30,000,000`
- `poll_sec=15`

## 9) Swaggy Atlas Lab (참고 타임프레임)

`engines/swaggy_atlas_lab/config.py`:
- HTF: `1h`, `4h`
- MTF: `15m`
- LTF: `5m`
- D1: `1d`
  
Atlas 계산은 15m 기반이지만, Swaggy Atlas Lab은 1h/4h/15m/5m/1d를 함께 사용한다.
