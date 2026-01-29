1) 엔진 구조 요약

  - SwaggyLab v2 시그널 생성 + Atlas 정책 게이트 적용
  - Atlas는 HARD 모드 기본 (pass_hard True일 때만 통과)
  - 통과 시 시장가 진입, TP/SL은 공통 auto_exit 모듈이 처리
  - 진입 품질/레벨/레짐/오버익스텐션 정보를 상세 로그로 남김

  ---

  ## 2) 시간프레임

  - LTF: 5m
  - MTF: 15m
  - HTF: 1h
  - HTF2: 4h
  - D1: 1d (오버익스텐션/레짐 보조)

  ---

  ## 3) Swaggy v2 진입 파이프라인 (요약)

  ### A. 레벨 생성/터치
  - 1h 기반 VP/스윙 레벨 생성
  - 레짐별 터치 최소 조건
  - 레벨 거리/클러스터 필터 적용

  ### B. 트리거 조합
  - reclaim / rejection / sweep / retest 중 1개 이상
  - 트리거별 최소 강도 컷 적용

  ### C. strength / regime 컷
  - weak_cut 이하 차단
  - bull/bear/range 레짐별 최소 strength 컷 적용
  - countertrend 기본 차단 (allow_countertrend=False)

  ### D. 필터
  - LVN gap / 확장봉 / 레벨 거리 제한 등

  ### E. 오버익스텐션(추격 방지)
  - EMA20, ATR14 기반 거리
  - dist > overext_atr_mult 이면 ENTRY_READY라도 CHASE로 전환
  - CHASE 상태에서 dist가 내려오면 WAIT_TRIGGER 복귀

  ---

  ## 4) Atlas Hard Gate (핵심)

  ### 글로벌 게이트
  - BTC 15m 기반 HTF 게이트
  - EMA20/EMA60 + ATR 기반으로 bull/bear/chaos 레짐 판별

  ### 로컬 게이트 (ALT vs BTC)
  - 상대강도(RS/RS_z) + 상관/베타 + 볼륨비 평가
  - 방향 게이트: LONG=상대강도 Bull, SHORT=Bear

  ### 하드 통과 조건
  - RS 통과는 필수
  - RS/INDEP/VOL 중 점수 >= exception_min_score 필요
  - 현재 설정값: exception_min_score = 2

  ### 정책 적용
  - HARD 모드
    - pass_hard True -> allow
    - pass_hard False -> HARD_BLOCK

  ---

  ## 5) 진입 금액 / 정책 결과

  - base_usdt는 entry_usdt(퍼센트) 기반
  - 정책 결과(policy.final_usdt)로 최종 진입 금액 계산

  ---

  ## 6) 공통 제한

  - 심볼별 in_position 체크
  - 최근 진입 쿨다운
  - 엔트리 락/중복 방지
  - 동시 포지션 제한 (MAX_OPEN_POSITIONS)

  ---

  ## 7) 로그 형식 (대표 예시)

  - SWAGGY_ATLAS_LAB_V2_PHASE
  - SWAGGY_ATLAS_LAB_V2_POLICY
  - SWAGGY_ATLAS_LAB_V2_SKIP
  - SWAGGY_ATLAS_LAB_V2_ENTRY

  예)
  2026-01-27 21:31:18 SWAGGY_ATLAS_LAB_V2_POLICY sym=ZEC/USDT:USDT side=SHORT action=HARD_BLOCK atlas_reasons=ATLAS_DIR_BLOCK
  2026-01-27 21:31:25 SWAGGY_ATLAS_LAB_V2_ENTRY sym=PIPPIN/USDT:USDT side=SHORT sw_strength=0.187 ...

  ---

  ## 8) 주요 설정 위치

  - Swaggy v2: engines/swaggy_atlas_lab_v2/config.py
  - Atlas eval: engines/swaggy_atlas_lab_v2/atlas_eval.py
  - 정책 적용: engines/swaggy_atlas_lab_v2/policy.py

