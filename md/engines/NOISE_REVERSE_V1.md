1) 엔진 구조 요약

  - 1m 기준 "가짜 돌파 추격 + 과열 이격 + 거래량 스파이크"에서 진입
  - 공통 진입금액/텔레그램/웹 설정/진입금지/쿨다운 로직을 그대로 사용
  - 진입은 시장가, TP/SL은 공통 auto_exit 모듈(ENGINE_EXIT_OVERRIDES)로 처리
  - 확정봉만 사용 (마지막 미완성 1m 봉 제외)
  - 실시간 모드(realtime only)에서만 동작
  - 심볼 계산은 병렬(5워커), 주문/상태 업데이트는 순차

  ---

  ## 2) 시간프레임

  - LTF: 1m

  ---

  ## 3) 유니버스

  - anti_alpha_v1과 동일: adv_trend_universe 사용
  - 상한: 30개 (현재 메인 설정 기준)

  ---

  ## 4) 진입 로직 (핵심)

  ### A. 지표/필터
  - 최근 100봉 고점/저점 돌파 확인
  - MA20 이격도 필터 (disparity)
  - 거래량 스파이크: vol >= SMA20 * 5

  ### B. 진입 조건
  - LONG:
    - high > 최근 100봉 high
    - close > MA20 * (1 + disparity_pct)
    - vol_spike 충족
  - SHORT:
    - low < 최근 100봉 low
    - close < MA20 * (1 - disparity_pct)
    - vol_spike 충족

  ### C. 확정봉
  - 마지막 1m 봉 제외 (미완성 봉 제거)

  ---

  ## 5) 공통 제한 (라이브)

  - 심볼+방향 중복 진입 방지 (entry_guard)
  - 동일 방향 기존 포지션 보유 시 차단
  - exit_cooldown 시간 내 재진입 차단
  - 동시 포지션 제한 (MAX_OPEN_POSITIONS)
  - 관리자 활성화 여부(_admin_is_active) 확인

  ---

  ## 6) 청산 로직

  - 공통 auto_exit 사용
  - 기본 TP/SL: 2% / 2% (ENGINE_EXIT_OVERRIDES)
  - 텔레그램 명령으로 조정 가능
    - /engine_exit NOISE_REVERSE_V1 LONG tp sl
    - /engine_exit NOISE_REVERSE_V1 SHORT tp sl

  ---

  ## 7) 로그 형식 (대표)

  - NOISE_REVERSE_CYCLE_START
  - NOISE_REVERSE_DEBUG (최대 3개)
  - NOISE_REVERSE_ENTRY
  - NOISE_REVERSE_CYCLE_END
  - NOISE_REVERSE_GATES

  예)
  2026-01-30 17:42:11 NOISE_REVERSE_CYCLE_START cycle_id=None universe=30
  2026-01-30 17:42:12 NOISE_REVERSE_DEBUG sym=BTC/USDT:USDT close=82450.7 ma20=81023.1 hi100=83210.0 lo100=79020.0 vol=44.02 vol_ma=90.45 fails=vol_spike,disparity
  2026-01-30 17:42:13 NOISE_REVERSE_CYCLE_END elapsed=1.75s checked=30 entries=0 skips=0 no_signal=30 no_data=0
  2026-01-30 17:42:13 NOISE_REVERSE_GATES vol_spike=24 disparity=0 break=0 nan=0

  ---

  ## 8) 주요 설정 위치

  - 실매매 로직: engine_runner.py (_run_noise_reverse_v1_cycle)
  - 백테스트: engines/noise_reverse_v1/run_backtest.py
  - 유니버스/공통 설정: engines/universe.py, engines/rsi/config.py
  - 주요 파라미터(ENV):
    - NOISE_REVERSE_LOOKBACK (기본 100)
    - NOISE_REVERSE_MA_LEN (기본 20)
    - NOISE_REVERSE_VOL_SMA_LEN (기본 20)
    - NOISE_REVERSE_VOL_SPIKE_MULT (기본 5.0)
    - NOISE_REVERSE_DISPARITY_PCT (기본 0.025)
  - TP/SL: ENGINE_EXIT_OVERRIDES (state.json)
