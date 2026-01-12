# RSI Scanner + Auto SHORT (Binance USDT Perpetual)

## Install
pip install ccxt pandas requests websocket-client

## Config (.env)
- 텔레그램 토큰/채널: `.env`의 `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- 바이낸스 키: `.env`의 `BINANCE_API_KEY`, `BINANCE_API_SECRET`
- 트레이딩 파라미터: `engine_runner.py` 상단에서 직접 수정 (USDT_PER_TRADE=진입비율%, LEVERAGE=10, MARGIN_MODE=cross, COOLDOWN_SEC 등)
- 거래량 필터: 24h `quoteVolume` 3천만 USDT 이상만 스캔
- 진입 조건(요약): 24h 변동률 절대값 상위 40개(앵커 BTC/ETH 포함), RSI 엔진은 상승률>0만 대상, RSI(1h≥70/15m≥73/5m≥76/3m≥80이면서 직전>현재), 5m 구조 거절(고점 하락 연속 또는 연속 윗꼬리 40%↑) + 5m 거래량≥최근20 평균×1.1, 동시 포지션 최대 7개
- 청산 옵션: `AUTO_EXIT_ENABLED`를 True로 켜면 5m EMA20 터치+ROI≥0에 자동 청산(기본 False)
- 실거래 여부: `executor.py`의 `DRY_RUN = True/False`로 제어
- 파비오 전용 모드: 환경변수 `FABIO_ONLY_MODE=1` 설정 시 RSI/롱스캘프 스캔은 건너뛰고 파비오만 실행

## Run
- 메인&관리 동시 : ./run_all.sh
- 메인: python engine_runner.py
- 웹소켓 관리모드 (단독실행) : python manage_ws.py
- 아틀라스 알림 : python -m atlas_test.main

## 백테스트
- RSI 엔진
  - RSI 백테스트 
    python3 -m engines.rsi.run_backtest \
    --symbols PTB/USDT:USDT,RIVER/USDT:USDT,AKE/USDT:USDT,CLO/USDT:USDT,AVNT/USDT:USDT,GPS/USDT:USDT \
    --start 2026-01-01 \
    --end 2026-01-10  \
    --sl-pct 0.04 \
    --tp-pct 0.05

- PumpFade 엔진
  - PumpFade 백테스트
    python3 -m engines.pumpfade.backtest_runner \
    --symbols BTC/USDT:USDT,ETH/USDT:USDT \
    --days 7 \
    --out reports/pumpfade_backtest.csv


## Live 전환
- `executor.py`에서 `DRY_RUN = False`로 변경 후 실행

## 엔진 설명

### RSI 엔진
- 엔진개요: 1h/15m/5m/3m RSI 멀티 타임프레임 스캔에 5m 구조 거절, 5m 거래량 급증, EMA/ATR 기반 임펄스 블록을 결합한 숏 스캐너.
- 엔트리 기준: 유니버스는 24h qVol≥30M + 상승률>0 상위 N(+BTC/ETH), RSI(1h≥77/15m≥78/5m≥80/3m≥80) 통과, 5m RSI 하락 전환, 5m 구조 거절(낮아지는 고점 또는 윗꼬리 30% 이상 연속), 5m 거래량≥최근20 평균×1.1, 임펄스 블록 미발동; 스파이크 모드(3m≥85 + 3m 하락전환 + 1h/15m 통과 + vol) 또는 구조 모드(전 타임프레임+구조+vol) 중 하나 충족 시 ENTRY_READY.

### Fabio 엔진
- 엔진개요: HTF(4h) EMA7/20으로 추세를 판별하고 LTF(15m)에서 리테스트/리클레임, RSI/ATR/볼륨/볼린저 위치를 조합해 진입하는 풀백 엔진이며 숏은 5m/3m 구조 거절 시그널을 사용.
- 엔트리 기준: (롱) HTF EMA7>EMA20 & 종가≥EMA20, LTF EMA20 리테스트 후 reclaim, EMA20 거리 제한(dist_to_ema20_max), 거래량 비율≥trigger_vol_ratio_min, 위치 필터(EMA20 터치/EMA7-ATR 거리/BB 위치 중 2개 이상) 통과, 트리거 캔들(close>EMA7 & 양봉) + 구조 확인(EMA 기울기 또는 2연속 EMA20 상단 마감). (숏) HTF/MTF 약세(EMA20<EMA60 또는 close<EMA60) + LTF EMA20 리테스트 거절, RSI 다운턴+리젝트 캔들, lower-high/윗꼬리 체인 등 구조 조건, EMA20 거리 제한, 급락/임펄스 업 블록 통과, (옵션) 거래량 조건 통과.

### AtlasFabio 엔진
- 엔진개요: Fabio 시그널에 Atlas 게이트(HTF Supertrend + LTF ATR/볼륨 강도)를 결합한 추가 진입 엔진.
- 엔트리 기준: 1h Supertrend 방향이 롱/숏을 허용(UP=롱, DOWN=숏)하고 15m ATR/거래량이 SMA 대비 기준 이상일 때 사이즈 보정, Fabio 시그널이 entry_ready 및 trigger 강도를 통과하며 5m/3m RSI 히트 조건을 만족해야 ENTRY_READY.

### Swaggy 엔진
- 엔진개요: 1h/15m/5m 스윙 레벨과 볼륨 프로파일(POC/VAH/VAL, HVN/LVN)로 레벨을 만들고 RECLAIM/RETEST/SWEEP/REJECTION 트리거를 평가하는 레벨 기반 엔진.
- 엔트리 기준: 레벨 터치 범위 내에서 트리거 발생, 트리거 강도≥regime별 entry_min, 거리/확장바/쿨다운/LVN 갭/레짐 필터 통과 시 ENTRY_READY.

### Atlas 게이트(Swaggy)
- 엔진개요: BTC 15m EMA20/60과 ATR 비율로 시장 레짐(bull/bear/chaos)을 판단해 Swaggy의 전역 방향/허용도를 제어하는 게이트 엔진.
- 엔트리 기준: 레짐에 따라 long/short 허용(또는 예외 필요) 결정, 로컬 게이트에서 알트의 RS/상관/베타/거래량 비율을 평가해 예외 진입 및 사이즈 보정을 적용.

### DTFX 엔진
- 엔진개요: LTF(기본 1m)에서 유동성 스윕 → MSS(구조 전환) → OB/FVG 되돌림 존 터치 흐름으로 상태 머신을 돌리는 롱/숏 엔진.
- 엔트리 기준: 스윕 감지 후 MSS 확정, 진입 존(OB/FVG) 설정, 존 터치 시 캔들 조건(윗꼬리 비율/바디 ATR/확인 종가 비율)과 ATR 기반 필터를 통과하면 ENTRY_READY.

### Div15m Long 엔진
- 엔진개요: 15m 피봇 저점 기반 RSI 상승 다이버전스를 탐지하고 EMA 리클레임으로 진입하는 롱 엔진.
- 엔트리 기준: 피봇 저점 2개에서 가격 LL + RSI HL, RSI 과매도 확인(윈도우 내 RSI≤32), 스파이크 볼륨 차단 통과, EMA7 리클레임 확인 후 EMA20 상회 또는 최근 3봉 고점 돌파 시 ENTRY_READY.

### Div15m Short 엔진
- 엔진개요: 15m 피봇 고점 기반 RSI 하락 다이버전스를 탐지하고 EMA 리젝트로 진입하는 숏 엔진.
- 엔트리 기준: 피봇 고점 2개에서 가격 HH + RSI LH, RSI 과매수 확인(윈도우 내 RSI≥70), EMA7 리젝트 확인, 레짐 필터(EMA120 하향 및 가격<EMA) 통과, MACD 히스토그램 감소 필터 통과, EMA20 하회 또는 최근 3봉 저점 이탈 시 ENTRY_READY.

### PumpFade 엔진
- 엔진개요: 급등 종목의 고점 재도전 실패 구간에서 조정 숏을 노리는 15m 실패봉 기반 엔진(EMA20~EMA60 회귀).
- 엔트리 기준: 6h 상승률/24h 상승률/3h 변동성/1h 거래량 급증 중 2개 이상 충족, 15m 고점 리테스트 존에서 실패봉(A-1/A-2) 발생 + 다음 15m 봉에서 EMA7 이탈 또는 실패봉 저점 이탈 확인, 거래량 약화 조건 충족, RSI 꺾임 및 MACD 히스토그램 증가 차단 후 ENTRY_READY(리밋 진입, 2봉 내 미체결 시 취소).
