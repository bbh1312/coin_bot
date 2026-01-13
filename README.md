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
- 웹 실행 python web_app/app.py (http://127.0.0.1:5000)


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
    --symbols PTB/USDT:USDT,HYPER/USDT:USDT,POL/USDT:USDT,RIVER/USDT:USDT,AKE/USDT:USDT \
    --days 7 \
    --out logs/pumpfade/pumpfade_backtest.csv
- 아틀라스 파비오
  - 백테스트
    python3 backtest_atlasfabio.py \
      --symbols PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT,HYPER/USDT:USDT \
      --days 10 \
      --sl-pct 0.02 \
      --tp-pct 0.03
  - 예시
    ```
    python3 backtest_atlasfabio.py \
      --symbols PROM/USDT:USDT,B/USDT:USDT,PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT \       
      --days 7 \
      --sl-pct 0.03 \
      --tp-pct 0.05
      [BACKTEST] AKE/USDT:USDT trades=1 wins=1 losses=0 winrate=100.00% tp=1 sl=0 avg_mfe=0.0671 avg_mae=0.0197 avg_hold=165.0
      [BACKTEST] POL/USDT:USDT trades=1 wins=1 losses=0 winrate=100.00% tp=0 sl=0 avg_mfe=0.0469 avg_mae=0.0263 avg_hold=495.0
      [BACKTEST] TRUTH/USDT:USDT trades=1 wins=1 losses=0 winrate=100.00% tp=1 sl=0 avg_mfe=0.0531 avg_mae=0.0217 avg_hold=270.0
      [BACKTEST] PROM/USDT:USDT trades=0 wins=0 losses=0 winrate=0.00% tp=0 sl=0 avg_mfe=0.0000 avg_mae=0.0000 avg_hold=0.0
      [BACKTEST] B/USDT:USDT trades=1 wins=0 losses=1 winrate=0.00% tp=0 sl=1 avg_mfe=0.0146 avg_mae=0.0304 avg_hold=435.0
      [BACKTEST] PTB/USDT:USDT trades=0 wins=0 losses=0 winrate=0.00% tp=0 sl=0 avg_mfe=0.0000 avg_mae=0.0000 avg_hold=0.0
      [BACKTEST] TOTAL trades=4 wins=3 losses=1 winrate=75.00% tp=2 sl=1 avg_mfe=0.0454 avg_mae=0.0245 avg_hold=341.2
  ```
- 스웨기
  - 백테스트
    python3 engines/swaggy/run_backtest.py \
    --symbols PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT,ALCH/USDT:USDT,ARC/USDT:USDT,B/USDT:USDT \
    --sl-pct 0.03 \
    --tp-pct 0.03 \
    --days 10
  - 예시
  ```
  python3 engines/swaggy/run_backtest.py \
    --symbols PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT,ALCH/USDT:USDT,ARC/USDT:USDT,B/USDT:USDT \
    --sl-pct 0.30 \
    --tp-pct 0.03 \
    --days 10
  [BACKTEST] PTB/USDT:USDT trades=97 wins=54 losses=43 winrate=55.67% tp=0 sl=0 avg_mfe=0.0123 avg_mae=0.0144 avg_hold=64.5
  [BACKTEST] AKE/USDT:USDT trades=93 wins=48 losses=45 winrate=51.61% tp=0 sl=3 avg_mfe=0.0131 avg_mae=0.0138 avg_hold=63.9
  [BACKTEST] POL/USDT:USDT trades=87 wins=38 losses=49 winrate=43.68% tp=0 sl=0 avg_mfe=0.0102 avg_mae=0.0113 avg_hold=65.1
  [BACKTEST] TRUTH/USDT:USDT trades=98 wins=50 losses=48 winrate=51.02% tp=0 sl=3 avg_mfe=0.0167 avg_mae=0.0134 avg_hold=64.6
  [BACKTEST] ALCH/USDT:USDT trades=93 wins=51 losses=42 winrate=54.84% tp=0 sl=1 avg_mfe=0.0092 avg_mae=0.0082 avg_hold=64.7
  [BACKTEST] ARC/USDT:USDT trades=110 wins=49 losses=61 winrate=44.55% tp=0 sl=1 avg_mfe=0.0109 avg_mae=0.0120 avg_hold=64.8
  [BACKTEST] B/USDT:USDT trades=105 wins=51 losses=54 winrate=48.57% tp=0 sl=2 avg_mfe=0.0169 avg_mae=0.0132 avg_hold=64.9
  [BACKTEST] TOTAL trades=683 wins=341 losses=342 winrate=49.93% tp=0 sl=10 avg_mfe=0.0128 avg_mae=0.0124 avg_hold=64.6
  ```
- 아틀라스 스웨기
  - 백테스트
    python33 -m engines.swaggy_atlas_lab.run_backtest \
    --symbols PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT,ALCH/USDT:USDT,ARC/USDT:USDT,B/USDT:USDT \
    --days 10 \
    --sl-pct 0.30 \
    --tp-pct 0.03 \
    --mode all
  - 예시
    ```
     python3 engines/swaggy_atlas_lab/run_backtest_sweep.py \
    --symbols PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT,ALCH/USDT:USDT,ARC/USDT:USDT,B/USDT:USDT \
    --days 10 \
    --mode shadow \
    --tp-pct 0.03 \
    --sl-pct 0.30 
    [BACKTEST] PTB/USDT:USDT@shadow trades=6 wins=6 losses=0 winrate=100.00% tp=6 sl=0 avg_mfe=0.0354 avg_mae=0.0365 avg_hold=562.5
    [BACKTEST] AKE/USDT:USDT@shadow trades=13 wins=13 losses=0 winrate=100.00% tp=13 sl=0 avg_mfe=0.0377 avg_mae=0.0266 avg_hold=486.2
    [BACKTEST] POL/USDT:USDT@shadow trades=10 wins=9 losses=1 winrate=90.00% tp=9 sl=1 avg_mfe=0.0301 avg_mae=0.0501 avg_hold=1152.0
    [BACKTEST] TRUTH/USDT:USDT@shadow trades=21 wins=21 losses=0 winrate=100.00% tp=21 sl=0 avg_mfe=0.0348 avg_mae=0.0284 avg_hold=561.7
    [BACKTEST] ALCH/USDT:USDT@shadow trades=7 wins=7 losses=0 winrate=100.00% tp=7 sl=0 avg_mfe=0.0356 avg_mae=0.0598 avg_hold=1857.1
    [BACKTEST] ARC/USDT:USDT@shadow trades=5 wins=4 losses=1 winrate=80.00% tp=4 sl=1 avg_mfe=0.0301 avg_mae=0.1097 avg_hold=2450.0
    [BACKTEST] B/USDT:USDT@shadow trades=15 wins=15 losses=0 winrate=100.00% tp=15 sl=0 avg_mfe=0.0513 avg_mae=0.0572 avg_hold=823.0
    [BACKTEST] TOTAL@shadow trades=77 wins=75 losses=2 winrate=97.40% tp=75 sl=2 avg_mfe=0.0377 avg_mae=0.0453 avg_hold=916.9
    [BACKTEST] TOTAL trades=77 wins=75 losses=2 winrate=97.40% tp=75 sl=2 avg_mfe=0.0377 avg_mae=0.0453 avg_hold=916.9    
    ```
- 아틀라스 일반 숏
  - 백테스트
     python3 -m engines.atlas_rs_fail_short.backtest_runner \
    --symbols PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT,ALCH/USDT:USDT,ARC/USDT:USDT,B/USDT:USDT \
    --days 30 \
    --sl-pct 0.03 \
    --tp-pct 0.03 \
    --out logs/atlas_rs_fail_short/arsf_signals.csv
  - 예시
  ```
  python3 -m engines.atlas_rs_fail_short.backtest_runner \
    --symbols PTB/USDT:USDT,AKE/USDT:USDT,POL/USDT:USDT,TRUTH/USDT:USDT,ALCH/USDT:USDT,ARC/USDT:USDT,B/USDT:USDT \
    --days 10 \
    --sl-pct 0.30 \
    --tp-pct 0.02 \
    --out logs/atlas_rs_fail_short/arsf_signals.csv
    [BACKTEST] PTB/USDT:USDT trades=9 wins=8 losses=1 winrate=88.89% tp=8 sl=1 avg_mfe=0.0268 avg_mae=0.0488 avg_hold=475.0
    [BACKTEST] AKE/USDT:USDT trades=3 wins=2 losses=1 winrate=66.67% tp=2 sl=1 avg_mfe=0.0214 avg_mae=0.1188 avg_hold=2250.0
    [BACKTEST] POL/USDT:USDT trades=3 wins=2 losses=1 winrate=66.67% tp=2 sl=1 avg_mfe=0.0186 avg_mae=0.1740 avg_hold=4230.0
    [BACKTEST] TRUTH/USDT:USDT trades=11 wins=10 losses=1 winrate=90.91% tp=10 sl=1 avg_mfe=0.0286 avg_mae=0.0430 avg_hold=469.1
    [BACKTEST] ALCH/USDT:USDT trades=2 wins=2 losses=0 winrate=100.00% tp=2 sl=0 avg_mfe=0.0282 avg_mae=0.0506 avg_hold=2310.0
    [BACKTEST] ARC/USDT:USDT trades=8 wins=7 losses=1 winrate=87.50% tp=7 sl=1 avg_mfe=0.0263 avg_mae=0.0543 avg_hold=930.0
    [BACKTEST] B/USDT:USDT trades=6 wins=6 losses=0 winrate=100.00% tp=6 sl=0 avg_mfe=0.0306 avg_mae=0.0513 avg_hold=835.0
    [BACKTEST] total trades=42 wins=37 losses=5 winrate=88.10% tp=37 sl=5 avg_mfe=0.0268 avg_mae=0.0627 avg_hold=1093.9
  ```

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
