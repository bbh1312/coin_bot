1) 엔진 구조 요약

  - SwaggySignalEngine가 진입 시그널 생성
  - Atlas 평가로 hard gate 적용
  - **Over-extension(추격 방지)**로 추가 차단
  - 최종 통과 시 시장가 진입, TP/SL은 공통 모듈이 처리

  ———

  ## 2) 시간프레임

  - LTF: 5m
  - MTF: 15m
  - HTF: 1h
  - HTF2: 4h

  ———

  ## 3) Swaggy 진입 기준 (핵심 조건)

  ### A. 레벨 생성/터치

  - 1h 기반 VP/스윙 레벨 생성
  - 레벨 터치 조건:
      - touch_pct_min (레짐별):
          - bull: 0.0010
          - bear: 0.0012
          - range: 0.0016
      - touch_atr_mult: 0.15
      - touch_eps: 0.0015
      - touch_eps_atr_mult: 0.05
      - touch_eps_pct: 0.0005

  ### B. 트리거(최소 1개)

  - reclaim / rejection / sweep / retest 중 하나 발생
  - 각 트리거 최소 강도:
      - sweep: 0.55
      - reclaim: 0.40
      - rejection: 0.50
      - retest: 0.45

  ### C. strength 컷

  - weak_cut: 0.32
  - 레짐별 최소 strength:
      - bull: 0.40
      - bear: 0.40
      - range: 0.45

  ### D. 레짐 필터

  - 기본 allow_countertrend = False
  - range 숏 허용 여부: range_short_allowed = False

  ### E. 필터들

  - LVN gap 필터 통과
  - 확장봉 필터:
      - expansion_atr_mult = 1.5
      - ATR 길이 touch_atr_len = 14
  - 레벨 거리 제한:
      - max_dist = 0.005
      - level_max_dist_pct = 0.06

  ———

  ## 4) Over-extension(추격 방지) 블록

  추격 방지 기준 (EMA20, ATR14):

  - LONG: (price - EMA20) / ATR
  - SHORT: (EMA20 - price) / ATR
  - dist > overext_atr_mult 이면 ENTRY_READY라도 CHASE
      - 기본값: overext_atr_mult = 1.5
      - EMA 길이: overext_ema_len = 20

  CHASE 상태에서 dist가 다시 낮아지면 WAIT_TRIGGER로 복귀.
  - 로그 기록 시 overext_dist_at_touch/entry는 **절대값(항상 양수)** 기준으로 저장

  ———

  ## 5) Atlas Hard Gate (실매매 모드)

  - BTC 15m 기반 글로벌 게이트 (EMA20/EMA60 + ATR)
  - 알트 vs BTC 상대강도(rs/rs_z), 상관/베타, 볼륨비율 평가
  - hard 모드: atlas.pass_hard == True일 때만 진입 허용

  ———

  ## 6) 공통 제한

  - 심볼별 in_position 체크
  - 최근 진입 쿨다운
  - 엔트리 락/중복 방지
  - 동시 포지션 제한 MAX_OPEN_POSITIONS

  ———

  ## 7) 실매매 진입 방식

  - 신호 확정봉 기준 시장가 진입
  - 엔진명: SWAGGY_ATLAS_LAB
  - 엔트리 금액: policy.final_usdt → _resolve_entry_usdt()로 계산
  - TP/SL은 **공통 모듈(auto_exit)**이 처리 (엔진별 override 설정 가능)
