# 멀티계정(단일 프로세스) 운영 설계 메모

## 목표
- 단일 프로세스로 다수 계정을 동시에 운용.
- 시그널 계산은 공통 1회.
- 주문/청산/알림은 계정별 설정에 따라 분기.
- 텔레그램은 계정별 봇 토큰 사용.

## 요구사항 요약
- 계정정보: BINANCE_API_KEY, BINANCE_API_SECRET 저장.
- 사용자별 설정: 진입금액 %, 실주문 On/Off, 자동청산 On/Off.
- 공통 설정: 그 외 전략 파라미터는 전체 동일.
- 알림: 계정별 텔레그램 봇으로 발송.

## 추천 구조
### 1) 저장소(권장: SQLite)
- accounts
  - id, name
  - api_key, api_secret
  - is_active
- account_settings
  - account_id
  - entry_pct
  - dry_run
  - auto_exit
- telegram_bots
  - account_id
  - bot_token
  - chat_id
  - last_update_id

#### 스키마 구체화(초안)
- accounts
  - id INTEGER PRIMARY KEY
  - name TEXT UNIQUE NOT NULL
  - api_key TEXT NOT NULL
  - api_secret TEXT NOT NULL
  - is_active INTEGER NOT NULL DEFAULT 1
  - created_at TEXT, updated_at TEXT
- account_settings
  - account_id INTEGER PRIMARY KEY REFERENCES accounts(id)
  - entry_pct REAL NOT NULL
  - dry_run INTEGER NOT NULL DEFAULT 1
  - auto_exit INTEGER NOT NULL DEFAULT 0
  - max_positions INTEGER NOT NULL DEFAULT 7
  - leverage INTEGER NOT NULL DEFAULT 10
  - margin_mode TEXT NOT NULL DEFAULT "cross"
  - exit_cooldown_h REAL NOT NULL DEFAULT 2.0
  - long_tp_pct REAL NOT NULL DEFAULT 3.0
  - long_sl_pct REAL NOT NULL DEFAULT 3.0
  - short_tp_pct REAL NOT NULL DEFAULT 3.0
  - short_sl_pct REAL NOT NULL DEFAULT 3.0
  - dca_enabled INTEGER NOT NULL DEFAULT 1
  - dca_pct REAL NOT NULL DEFAULT 2.0
  - dca1_pct REAL NOT NULL DEFAULT 30.0
  - dca2_pct REAL NOT NULL DEFAULT 30.0
  - dca3_pct REAL NOT NULL DEFAULT 30.0
  - created_at TEXT, updated_at TEXT
- telegram_bots
  - account_id INTEGER PRIMARY KEY REFERENCES accounts(id)
  - bot_token TEXT NOT NULL
  - chat_id TEXT NOT NULL
  - last_update_id INTEGER NOT NULL DEFAULT 0
  - created_at TEXT, updated_at TEXT

인덱스/제약: accounts.name UNIQUE, telegram_bots.account_id UNIQUE.

보안: DB 파일 권한 최소화(예: chmod 600). 필요 시 마스터키로 암호화 옵션.

### 2) 런타임 모델
- AccountContext
  - exchange(ccxt 인스턴스)
  - settings(entry_pct, dry_run, auto_exit)
  - telegram(bot_token, chat_id)
  - account_state(포지션 캐시, 쿨다운 등)

#### 런타임 데이터 구조(초안)
- GlobalContext
  - signals(공통 시그널 결과)
  - universe(공통 유니버스)
  - gate_cache(공통 게이트/리전 계산 결과)
  - cycle_ts, cycle_id
- AccountContext
  - exchange(ccxt 인스턴스)
  - settings(계정별 오버라이드)
  - telegram(bot_token, chat_id)
  - account_state
    - open_positions_cache
    - entry_cooldown_ts
    - exit_cooldown_ts
    - entry_guard
    - order_map(entry_order_id -> meta)
    - last_error

### 3) 실행 흐름
1. 공통 시그널 계산(현 구조 유지).
2. 계정 리스트 순회.
3. 계정별 설정에 따라 주문/청산 실행.
4. 텔레그램 알림은 계정별 봇으로 발송.

#### 실행 흐름 상세(초안)
0. start: DB에서 활성 계정 로드, AccountContext 생성.
1. 공통 스캔: 유니버스/시그널/게이트 계산(1회).
2. 계정별 필터링:
   - 계정 설정에 따라 사이즈/레버리지/오토청산/쿨다운 적용.
3. 주문 단계:
   - dry_run=False인 계정만 주문.
   - 동일 시그널 중복 진입 방지(계정별 entry_guard).
4. 관리 단계:
   - 계정별 포지션 상태 조회 및 auto_exit/DCA 평가.
   - 실패 시 계정별 백오프 적용.
5. 알림 단계:
   - 계정별 텔레그램 봇으로 주문/청산/경고 메시지 전송.
6. 루프 지속:
   - 공통 cycle 타이밍 유지, 계정별 오류는 격리.

### 4) 텔레그램 명령(계정별 봇)
- 허용 명령만 처리:
  - /dryrun on|off
  - /entrypct 10
  - /autoexit on|off
  - /status
- chat_id 화이트리스트 기반 인증.

### 5) 주문/청산 분기 규칙
- 실주문: dry_run == False 인 계정만 주문.
- 자동청산: auto_exit == True 인 계정만 청산.
- 그 외 설정은 공통값 유지.

#### 설정 범위(초안)
- 공통 설정(전 계정 동일):
  - 시그널 로직/지표 파라미터
  - 유니버스 필터(qVol, topN, anchors)
  - 엔진 ON/OFF(전역)
- 계정별 설정(오버라이드 가능):
  - entry_pct
  - dry_run
  - auto_exit
  - max_positions
  - leverage, margin_mode
  - exit_cooldown_h
  - long/short TP/SL
  - DCA on/off + DCA 비율

### 6) 리스크/운영
- 계정별 ccxt 인스턴스 분리.
- 계정 수 증가 시 레이트리밋 캐시/백오프 필요.
- 계정별 예외 격리(한 계정 오류가 전체 루프를 죽이지 않게).

## 다음 작업 후보
- DB 스키마/초기화 스크립트 작성
- 계정 등록/수정 CLI 혹은 텔레그램 등록 플로우
- 계정별 텔레그램 polling 루프 추가
- AccountContext 적용으로 executor/engine_runner 분리

## 계정별 분기 적용 설계(초안)
### 1) 분기 포인트(현재 구조 기준)
- 주문 진입: long_market, short_market 호출부
- 청산: close_long_market, close_short_market 호출부
- DCA: dca_long_if_needed, dca_short_if_needed 호출부
- 잔고/포지션: get_available_usdt, count_open_positions, list_open_position_symbols 등
- 알림: send_telegram 호출부(전역 BOT_TOKEN/CHAT_ID)

### 2) Executor 분리 설계
- 목표: 계정별 ccxt 인스턴스/설정을 가진 실행자 인스턴스화
- 방식:
  - executor.py에 AccountExecutor 클래스 추가
  - 기존 전역 함수 로직을 메서드로 이관/재사용
  - AccountExecutor 생성 시: api_key, api_secret, dry_run, leverage, margin_mode, dca 설정 주입
- 장점: 주문/청산 로직 재사용 + 계정별 분리

### 3) 텔레그램 분리 설계
- 목표: 계정별 봇 토큰/챗 아이디로 알림 분기
- 방식:
  - TelegramClient(token, chat_id) 도입
  - send_telegram 호출부를 account.telegram.send(...) 형태로 변경

### 4) 엔진 러너 분기 흐름
0. DB에서 활성 계정 로드 → AccountContext 생성
1. 공통 시그널 계산(현 구조 유지)
2. 계정별 루프:
   - 계정 설정(사이즈/레버리지/쿨다운/auto_exit/DCA) 적용
   - 주문/청산/관리 실행(계정별 executor)
   - 알림 전송(계정별 telegram)
3. 상태 저장:
   - 계정별 state 파일 분리(state_{account_id}.json) 권장

### 5) 적용 순서(추천)
1) executor.py에 AccountExecutor 추가
2) engine_runner 주문/청산/DCA 호출부를 executor 인스턴스로 치환
3) TelegramClient 분리 및 알림 경로 교체
4) 계정별 state 로딩/저장 분리

### 6) 리스크 체크리스트
- 전역 exchange/설정 참조 잔존 여부 점검
- 계정별 쿨다운/entry_guard 격리 확인
- 계정별 TP/SL/DCA 적용 여부 확인
- 텔레그램 알림 계정별 분리 확인

## 적용 설계 상세(코드 작성 없이 변경 포인트 정리)
### A) executor.py 변경 포인트(설계)
- 새 클래스: AccountExecutor
  - __init__(api_key, api_secret, settings, dry_run)
  - 내부 exchange 생성(기존 전역 exchange 로직 재사용)
  - 기존 함수 로직을 메서드로 이동:
    - short_market, long_market
    - close_short_market, close_long_market
    - get_available_usdt
    - list_open_position_symbols, count_open_positions
    - dca_short_if_needed, dca_long_if_needed
  - 계정별 캐시 분리:
    - 포지션 TTL 캐시
    - 잔고 TTL 캐시
    - 백오프 타이머
- 전역 함수 유지 여부:
  - 기존 전역 함수는 단일 계정 호환 유지용 래퍼로 남기고,
    내부에서 "default_executor"를 호출하도록 설계

### B) engine_runner.py 분기 설계(변경 포인트)
- 계정 로딩:
  - DB에서 활성 계정 목록 로드 → AccountContext 리스트 생성
- 상태 분리:
  - state/{account_id}.json로 계정별 로드/저장
  - 기존 state 구조는 유지하되 파일만 분리
- 주문/청산 호출부 변경:
  - short_market/long_market → acct.executor.short_market/long_market
  - close_short_market/close_long_market → acct.executor.close_*
  - dca_* → acct.executor.dca_*
- 관리 루프 변경:
  - per-account loop로 전환(계정별 상태만 관리)
  - 계정별 max_positions/exit_cooldown 적용
- 알림 변경:
  - send_telegram → acct.telegram.send

### C) 변경 영향 범위(요약)
- 영향 큰 파일: engine_runner.py, executor.py, (텔레그램 유틸 파일 분리 시 신규 파일)
- 최소 변경 방식:
  - 시그널 계산/유니버스/엔진 로직은 그대로 유지
  - 주문/청산/알림 경로만 계정별 인스턴스로 치환

### D) 단계별 검증 시나리오(설계)
1) 단일 계정 모드로 기존 동작 유지 여부 확인
2) 2계정 이상 모의거래에서 주문/알림 분리 확인
3) 계정별 TP/SL/DCA 적용 차이 확인

## 추가 명시 사항
- 시그널 유효시간: 병렬처리로 계정별 동시 적용(지연 최소화)
- 텔레그램 명령 처리 범위: 계정별 상태만 응답
- 레이트리밋 정책: 계정별 독립 백오프

## 병렬처리 방식 후보(간단 비교)
- 스레드(Thread)
  - 장점: 기존 동기 코드 재사용 용이, 적용 난이도 낮음
  - 단점: CPU 작업에는 부적합, 공유 상태 잠금 필요
- 프로세스(Process)
  - 장점: GIL 영향 없음, 계정별 격리 우수
  - 단점: IPC 비용/복잡도 증가, 메모리 사용 증가
- 비동기(Async)
  - 장점: 네트워크 IO 효율적, 요청 병렬화에 유리
  - 단점: 코드 구조 변경 폭 큼, 동기 라이브러리 감싸기 필요

권장: 초기 적용은 Thread로 시작 → 필요 시 Async/Process로 확장

## Thread 기반 적용 예시(설계)
- 구성
  - 공통 시그널 계산은 메인 스레드에서 수행
  - 계정별 주문/청산/알림은 스레드 풀에서 병렬 처리
- 간단 흐름
  1) main thread: signals = compute_signals()
  2) thread pool: for acct in accounts -> apply_signals(acct, signals)
  3) 각 스레드: 계정별 state 로드/적용/저장
  4) 에러는 계정별로 로깅하고 전체 루프는 유지
