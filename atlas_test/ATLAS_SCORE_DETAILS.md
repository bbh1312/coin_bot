# Atlas Score Details (atlas_test)

이 문서는 atlas_test의 점수 산정에 쓰이는 4가지 항목의 상세 계산을 정리한다.
기준 코드는 `engines/atlas/atlas_engine.py`와 `atlas_test/atlas_engine.py`이다.
아래에는 운영 중 자주 보이는 `n/a`/고정 패턴의 원인도 함께 설명한다.

## 1) Regime (시장 상태)

판단 위치
- `engines/atlas/atlas_engine.py`의 `compute_swaggy_global()`

입력 데이터
- 레퍼런스 심볼: `cfg.ref_symbol` (기본 BTC/USDT:USDT)
- 15m 캔들 200개를 사용
- 최근 완결 캔들 기준으로 계산 (`df.iloc[:-1]`)

계산 요소
- EMA20, EMA60 (15m 종가 기준)
- ATR14 (15m 기준)
- ATR 비율: `atr_ratio = ATR14 / ATR14의 최근 20개 평균`
- 추세 정규화:
  - `trend_norm = (EMA20 - EMA60) / denom`
  - `denom`은 ATR14가 유효하면 ATR14, 아니면 종가

Regime 분기
- Bull
  - `EMA20 > EMA60` AND `trend_norm >= cfg.trend_norm_bull`
  - 기본값: `trend_norm_bull = 0.35`
- Bear
  - `EMA20 < EMA60` AND `trend_norm <= cfg.trend_norm_bear`
  - 기본값: `trend_norm_bear = -0.35`
- Chaos Range
  - `abs(trend_norm) <= cfg.trend_norm_flat`
  - AND `atr_ratio < cfg.chaos_atr_low`
  - 기본값: `trend_norm_flat = 0.20`, `chaos_atr_low = 0.85`
- Chaos Vol
  - 위 조건에 해당하지 않으면 기본적으로 `chaos_vol`

Regime 점수 반영 (atlas_test)
- bull/bear: +25
- chaos_vol: +15
- 그 외(예: chaos_range): +5

## 2) RS (상대강도)

판단 위치
- `engines/atlas/atlas_engine.py`의 `compute_swaggy_local()`
- 점수 반영은 `atlas_test/atlas_engine.py`의 `_score_from_atlas()`

계산 개요
- 15m 로그수익률을 사용
- 빠른 구간과 느린 구간을 각각 계산
  - fast: `rs_n`, `corr_m`
  - slow: `rs_n_slow`, `corr_m_slow`
- 레퍼런스(BTC)와 대상 심볼의 로그수익률을 비교

핵심 값
- `rs_fast = sum(alt_fast.rs_window) - sum(btc_fast.rs_window)`
- `rs_slow = sum(alt_slow.rs_window) - sum(btc_slow.rs_window)`
- `rs_z_fast = rs_fast / (sigma_fast * sqrt(rs_n))`
  - `sigma_fast = std(btc_fast.rets[-corr_m:])`
- `rs_z_slow = rs_slow / (sigma_slow * sqrt(rs_n_slow))`

통과 조건 (점수 +25)
- 아래 중 하나라도 충족하면 RS 통과
  - `rs >= cfg.rs_pass`
  - `rs_z >= cfg.rs_z_pass`
  - `rs_slow >= cfg.rs_pass`
  - `rs_z_slow >= cfg.rs_z_pass`

기본 임계값 (AtlasSwaggyConfig)
- `rs_pass = 0.0060`
- `rs_z_pass = 0.75`
- `rs_n = 12`, `corr_m = 32`
- `rs_n_slow = 36`, `corr_m_slow = 96`

## 3) 독립성 (상관/베타)

판단 위치
- `engines/atlas/atlas_engine.py`의 `compute_swaggy_local()`
- 점수 반영은 `atlas_test/atlas_engine.py`의 `_score_from_atlas()`

계산 개요
- 대상 심볼과 레퍼런스(BTC)의 로그수익률을 사용
- fast/slow 구간 각각 상관계수와 베타를 계산

핵심 값
- 상관계수:
  - `corr_fast = corrcoef(alt_fast.rets, btc_fast.rets)`
  - `corr_slow = corrcoef(alt_slow.rets, btc_slow.rets)`
- 베타:
  - `beta_fast = cov(alt_fast.rets, btc_fast.rets) / var(btc_fast.rets)`
  - `beta_slow = cov(alt_slow.rets, btc_slow.rets) / var(btc_slow.rets)`
- `beta_fast`는 [-3, 3] 범위로 클램프됨

통과 조건 (점수 +25)
- 아래 중 하나라도 충족하면 독립성 통과
  - `corr <= cfg.indep_corr`
  - `beta <= cfg.indep_beta`
  - `corr_slow <= cfg.indep_corr`
  - `beta_slow <= cfg.indep_beta`

기본 임계값 (AtlasSwaggyConfig)
- `indep_corr = 0.55`
- `indep_beta = 0.80`

## 4) 거래량 (Volume Ratio)

판단 위치
- `engines/atlas/atlas_engine.py`의 `compute_swaggy_local()`
- 점수 반영은 `atlas_test/atlas_engine.py`의 `_score_from_atlas()`

계산 방식
- 15m 기준 최근 20개 거래량 평균 대비 현재 거래량 비율
- `vol_ratio = last_volume / mean(volume[-20:])`

통과 조건 (점수 +25)
- `vol_ratio >= cfg.vol_pass`

기본 임계값 (AtlasSwaggyConfig)
- `vol_pass = 1.3`

## 운영 이슈: rs/corr/beta/vol이 n/a로 고정되는 이유

증상
- 요약에서 RS/상관/베타/거래량이 `n/a`
- 점수가 25(또는 15/5)에서 고정되는 패턴

원인
- `compute_swaggy_local()`는 `long_requires_exception != 1`일 때 조기 리턴한다.
- bull/normal 구간에서는 `long_requires_exception`이 0인 경우가 많아 로컬 지표 계산이 생략된다.
- 이때 `allow_long/allow_short`만 채워지고 `rs/corr/beta/vol`은 계산되지 않는다.
- `atlas_test`는 이 로컬 값을 그대로 점수 계산에 넣기 때문에 RS/독립성/거래량 점수가 항상 미부여로 남는다.

관련 코드
- `engines/atlas/atlas_engine.py`의 `compute_swaggy_local()`
- `atlas_test/atlas_engine.py`의 `_score_from_atlas()`

완화 방향
- 로컬 지표를 항상 계산하도록 `compute_swaggy_local()`의 early return 조건을 완화
- 또는 `atlas_test`에서 점수 산정 시 로컬 계산을 강제로 수행하도록 별도 경로 추가
