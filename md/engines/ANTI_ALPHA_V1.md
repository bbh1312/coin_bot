1) 엔진 구조 요약

  - 1m 기반 추세 확인 + 모멘텀 필터를 통과한 시그널을 "반대로" 진입하는 엔진
  - 공통 진입금액/텔레그램/웹 설정/진입금지/쿨다운 로직을 그대로 사용
  - 진입은 시장가, TP/SL은 공통 auto_exit 모듈(ENGINE_EXIT_OVERRIDES)로 처리
  - 확정봉만 사용 (마지막 미완성 1m 봉 제외)

  ---

  ## 2) 시간프레임

  - LTF: 1m

  ---

  ## 3) 유니버스

  - 공통 유니버스(shared_universe) 기반 + 저변동성 후보 추가
  - 실제 사용 유니버스: adv_trend_universe (공통 + low volatility)
  - 상한: 30개 (현재 메인 설정 기준)

  ---

  ## 4) 진입 로직 (핵심)

  ### A. 기본 지표
  - EMA200 (ANTI_ALPHA_EMA_LEN, 기본 200)
  - RSI14 (ANTI_ALPHA_RSI_LEN, 기본 14)
  - 거래량 SMA20 (ANTI_ALPHA_VOL_SMA_LEN, 기본 20)

  ### B. 시그널 조건
  - EMA200과의 거리 최소치(EMA_DIST_MIN) 통과
  - 연속 캔들(streak) 조건 통과
  - 거래량 스파이크(vol_spike) 통과
  - RSI 방향성/추세(직전 RSI 대비 상승/하락) 조건 통과
  - 바디 비율(body_pct) 통과
  - 전봉 고/저 돌파(break) 통과

  ### C. 방향 결정
  - 위 조건이 통과되면 "추세 방향"으로 시그널 생성
  - 실제 진입은 반대로 수행 (anti-alpha):
    - 조건상 LONG 시그널이면 실제는 SHORT
    - 조건상 SHORT 시그널이면 실제는 LONG

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
    - /engine_exit ANTI_ALPHA_V1 LONG tp sl
    - /engine_exit ANTI_ALPHA_V1 SHORT tp sl

  ---

  ## 7) 로그 형식 (대표)

  - ANTI_ALPHA_CYCLE_START
  - ANTI_ALPHA_DEBUG (최대 3개)
  - ANTI_ALPHA_ENTRY
  - ANTI_ALPHA_CYCLE_END
  - ANTI_ALPHA_GATES

  예)
  2026-01-30 17:42:11 ANTI_ALPHA_CYCLE_START cycle_id=None universe=30
  2026-01-30 17:42:12 ANTI_ALPHA_DEBUG sym=BTC/USDT:USDT regime=below_ema close=82450.7 ema=82726.1 ema_dist=0.0033 rsi=36.07 prev=40.56 vol=44.02 vol_ma=90.45 body=0.0007 fails=streak,vol_spike,rsi,body
  2026-01-30 17:42:13 ANTI_ALPHA_CYCLE_END elapsed=1.89s checked=30 entries=0 skips=0 no_signal=30 no_data=0
  2026-01-30 17:42:13 ANTI_ALPHA_GATES ema_dist=1 streak=14 vol_spike=15 rsi=18 momentum=6 body=18 break=10

  ---

  ## 8) 주요 설정 위치

  - 실매매 로직: engine_runner.py (_run_anti_alpha_cycle)
  - 백테스트: engines/anti_alpha_v1/run_backtest.py
  - 유니버스/공통 설정: engines/universe.py, engines/rsi/config.py
  - TP/SL: ENGINE_EXIT_OVERRIDES (state.json)
