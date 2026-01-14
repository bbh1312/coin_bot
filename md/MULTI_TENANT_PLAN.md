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

보안: DB 파일 권한 최소화(예: chmod 600). 필요 시 마스터키로 암호화 옵션.

### 2) 런타임 모델
- AccountContext
  - exchange(ccxt 인스턴스)
  - settings(entry_pct, dry_run, auto_exit)
  - telegram(bot_token, chat_id)
  - account_state(포지션 캐시, 쿨다운 등)

### 3) 실행 흐름
1. 공통 시그널 계산(현 구조 유지).
2. 계정 리스트 순회.
3. 계정별 설정에 따라 주문/청산 실행.
4. 텔레그램 알림은 계정별 봇으로 발송.

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

### 6) 리스크/운영
- 계정별 ccxt 인스턴스 분리.
- 계정 수 증가 시 레이트리밋 캐시/백오프 필요.
- 계정별 예외 격리(한 계정 오류가 전체 루프를 죽이지 않게).

## 다음 작업 후보
- DB 스키마/초기화 스크립트 작성
- 계정 등록/수정 CLI 혹은 텔레그램 등록 플로우
- 계정별 텔레그램 polling 루프 추가
- AccountContext 적용으로 executor/engine_runner 분리
