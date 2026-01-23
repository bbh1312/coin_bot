DTFX 엔진 개요

  - 구조: 유동성 스윕 → MSS(시장구조 전환) → 리트레이스 존(OB만) 터치 → ENTRY_READY
  - 방향: 롱/숏 각각 별도 FSM
  - 기본 타임프레임(라이브 엔진): LTF=5m, MTF=15m, HTF=1h (engine_runner.py)
  - 백테스트 러너 기본: LTF=5m, MTF=15m, HTF=1h (engines/dtfx/backtest_runner.py)

  ———

  1) 유니버스 필터

  - 최소 24h 거래대금: MIN_QVOL_24H_USDT = 3,000,000
  - 유니버스 상위 개수: UNIVERSE_TOP_N = 60
  - 앵커(major) 심볼: BTC, ETH (ANCHOR_SYMBOLS)
  - 경계: LOW_LIQUIDITY_QVOL_24H_USDT = 50,000,000 (저유동성 기준)
  - 소스: engines/dtfx/config.py

  ———

  2) 스윕(Sweep) 조건
  스윙 기준: swing_n = 2 (프랙탈 스윙 확정)

  - 롱(매도측 스윕): 캔들 저가가 최근 스윙로우를 아래로 찌르고, 종가는 스윙로우 위
  - 숏(매수측 스윕): 캔들 고가가 최근 스윙하이를 위로 찌르고, 종가는 스윙하이 아래
  - 스윕 깊이 상한: sweep_depth_atr_max = 0.6 * ATR
  - LTF별 스윕 최소 깊이:
      - 1m: 0.25 * ATR 또는 0.03% 이상
      - 5m: 0.15 * ATR 또는 0.02% 이상
  - 강한 돌파(스윕 무효) 판정:
      - Deadband: max(tick_size * 3, ATR * 0.10)
      - 강한 돌파 조건: close가 deadband를 넘어가고, body ratio ≥ 0.60 또는 3봉 연속 돌파
  - 소스: engines/dtfx/detectors/liquidity.py, engines/dtfx/config.py

  ———

  3) MSS(시장 구조 전환)

  - MSS 확인: 스윙하이/스윙로우를 종가 기준으로 돌파
  - 최소 몸통 조건: body >= ATR * min_body_atr_mult
      - 내부 기본값: mss_body_atr_min = 0.2 (엔진 init에서 setdefault)
  - MSS soft/confirm 데드밴드:
      - soft: deadband * 0.25 (deadband = max(tick_size * 3, ATR * strongbreakout_deadband_atr_mult))
      - confirm: deadband * 0.60 (deadband = max(tick_size * 3, ATR * strongbreakout_deadband_atr_mult))
  - soft → confirm 제한:
      - 최대 6봉 또는 1800초
  - soft 존 생성: 비활성화 (mss_soft_zone_enabled=False)
  - 진입 안전장치: soft 존은 confirm 충족 후 ENTRY 가능 (mss_soft_confirm_required=True)
  - 소스: engines/dtfx/detectors/structure.py, engines/dtfx/dtfx_long.py, engines/dtfx/
    dtfx_short.py, engines/dtfx/config.py

  ———

  4) 리트레이스 존(OB Only)

  - OB: MSS 직전 마지막 반대색 캔들 (OB만 사용)
  - FVG: 비활성화
  - OTE: 비활성화
  - 존 필터:
      - 존 높이 과대: zone_height > ATR * 0.9 → 스킵
      - 존 거리 과대: dist_to_zone_mid > ATR * 5.0 → 스킵
  - 소스: engines/dtfx/detectors/zones.py, engines/dtfx/dtfx_long.py, engines/dtfx/
    dtfx_short.py

  ———

  5) 진입(ENTRY_READY) 조건 – 수치 포함

  공통 구조:

  - MSS 방향 일치
  - HTF 필터: 1h EMA60 기준 (롱: close > EMA60 & EMA60 기울기 ≥ 0, 숏: close < EMA60 & 기울기 ≤ 0)
  - 존 터치(TOUCH) 또는 존 침투(PIERCE)
  - 터치 봉에서 반대색 캔들 요구 (롱: 음봉, 숏: 양봉)
  - 윗꼬리/아랫꼬리 비중, 몸통 ATR 기준, 종가 위치 비율 등 필터 통과

  롱 진입 (DTFXLongEngine)

  - MSS 방향: up
  - 존 터치 기준: 저가가 존에 닿음
  - 반대색 캔들: 음봉
  - 꼬리 비율(아랫꼬리):
      - 앵커(BTC/ETH): >= 0.45
      - 기타: >= 0.45
  - 몸통 ATR: body >= ATR * 0.25 (실제 유효값, config 끝에서 0.25로 override됨)
  - 종가 비율: (close - low) / range >= 0.60
  - 풀클로즈 요구: 기본 False
  - 도지: range <= eps면 차단
  - 소스: engines/dtfx/dtfx_long.py, engines/dtfx/config.py

  숏 진입 (DTFXShortEngine)

  - MSS 방향: down
  - 존 터치 기준: 고가가 존에 닿음
  - 반대색 캔들: 양봉
  - 꼬리 비율(윗꼬리):
      - 앵커(BTC/ETH): >= 0.45
      - 기타: >= 0.45
  - 몸통 ATR: body >= ATR * 0.25
  - 종가 비율: (high - close) / range >= 0.60
  - 풀클로즈 요구: 기본 False
  - 도지: range <= eps면 차단
  - 소스: engines/dtfx/dtfx_short.py, engines/dtfx/config.py

  ———

  6) 쿨다운

  - 진입 후 cooldown_bars = 3봉
  - 소스: engines/dtfx/dtfx_long.py, engines/dtfx/dtfx_short.py

  ———

  참고(파라미터 정의만 있고 미사용)

  - zone_pierce_atr_mult = 0.1은 현재 코드에서 참조되지 않음 (rg 기준).


