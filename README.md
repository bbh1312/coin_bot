# RSI Scanner + Auto SHORT (Binance USDT Perpetual)

## Install
pip install ccxt pandas requests websocket-client

## Config (.env)
- 텔레그램 토큰/채널: `.env`의 `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- 바이낸스 키: `.env`의 `BINANCE_API_KEY`, `BINANCE_API_SECRET`
- 트레이딩 파라미터: `engine_runner.py` 상단에서 직접 수정 (USDT_PER_TRADE=20, LEVERAGE=10, MARGIN_MODE=cross, COOLDOWN_SEC 등)
- 거래량 필터: 24h `quoteVolume` 3천만 USDT 이상만 스캔
- 진입 조건(요약): 24h 변동률 절대값 상위 40개(앵커 BTC/ETH 포함), RSI 엔진은 상승률>0만 대상, RSI(1h≥70/15m≥73/5m≥76/3m≥80이면서 직전>현재), 5m 구조 거절(고점 하락 연속 또는 연속 윗꼬리 40%↑) + 5m 거래량≥최근20 평균×1.1, 동시 포지션 최대 7개
- 청산 옵션: `AUTO_EXIT_ENABLED`를 True로 켜면 5m EMA20 터치+ROI≥0에 자동 청산(기본 False)
- 실거래 여부: `executor.py`의 `DRY_RUN = True/False`로 제어
- 파비오 전용 모드: 환경변수 `FABIO_ONLY_MODE=1` 설정 시 RSI/롱스캘프 스캔은 건너뛰고 파비오만 실행

## Run
- 메인&관리 동시 : ./run_all.sh
- 메인: python engine_runner.py --no-manage-loop
- 웹소켓관리 : python manage_ws.py
- 메인+웹소켓관리 동시 : ./run_all.sh (기본 --no-manage-loop 포함)
- 아틀라스 알림 : python -m atlas_test.main

## Live 전환
- `executor.py`에서 `DRY_RUN = False`로 변경 후 실행
