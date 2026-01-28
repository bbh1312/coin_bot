# Advanced Trend Follower (Triple-Check)

목표: 장기 추세(EMA200) 방향만 진입하고, MFI/ADX로 과열/횡보 필터링 후 SuperTrend 신호로 진입한다.

## 핵심 개념
- 추세 필터: 4h EMA200 기준
  - 가격 > EMA200 → 롱만 고려
  - 가격 < EMA200 → 숏만 고려
- 신호 트리거: 15m SuperTrend (ATR=10, Mult=3.0)
- 모멘텀/강도 필터
  - MFI(14): 롱은 80 미만, 숏은 20 초과
  - ADX(14): 20 초과에서만 진입

## 진입 조건 (실매매/백테스트 공통)
롱:
- 15m close > 4h EMA200
- MFI(14) < 80
- ADX(14) > 20
- SuperTrend 추세가 LONG

숏:
- 15m close < 4h EMA200
- MFI(14) > 20
- ADX(14) > 20
- SuperTrend 추세가 SHORT

## 청산 로직 (실매매)
- SL: 진입 직후 거래소 STOP_MARKET 주문으로 SuperTrend 라인에 스탑 설정
- TP1: 1.5R 지점에서 50% 부분청산
- TP1 이후: 남은 포지션 SL을 진입가(BE)로 이동
- TP2: SuperTrend 반전 시 전량 청산

## 리스크/사이징
- Risk per Trade: 가용 USDT의 2% (기본값)
- Min Stop Distance: ATR(14) * 0.5 미만이면 진입 스킵
- Max Notional Cap: 가용 USDT * 10 (기본값)

## 설정값 (env/state)
- 활성화
  - `ADV_TREND_ENABLED` (env)
  - `_adv_trend_enabled` (state)
- 유니버스
  - `ADV_TREND_MIN_QV` (env)
  - `ADV_TREND_UNIVERSE_TOP_N` (env)
- 리스크/필터
  - `ADV_TREND_RISK_PCT` (default 1.0)
  - `ADV_TREND_MAX_NOTIONAL_MULT` (default 10.0)
  - `ADV_TREND_MIN_STOP_ATR` (default 0.5)
  - `ADV_TREND_ADX_MIN` (default 25)
  - `ADV_TREND_MFI_LONG_MAX`
  - `ADV_TREND_MFI_SHORT_MIN`
- 지표 파라미터
  - `ADV_TREND_ADX_LEN`
  - `ADV_TREND_MFI_LEN`
  - `ADV_TREND_EMA_LEN`
  - `ADV_TREND_SUPER_ATR_LEN`
  - `ADV_TREND_SUPER_MULT`
  - `ADV_TREND_TP1_R_MULT`
  - `ADV_TREND_TP1_FRACTION`

## 텔레그램 명령
- `/adv_trend on|off|status`

## 웹 UI
- 설정 페이지(엔진 토글)에 `Triple-Check Engine` 토글 노출

## 주의사항
- SL/TP는 이 엔진 전용 로직(공통 TP/SL 무시)
- TP1/TP2는 봇에서 관리, SL은 거래소 STOP_MARKET 주문
