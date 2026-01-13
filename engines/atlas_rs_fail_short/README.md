# atlas_rs_fail_short

Atlas 기반 상대적 약세(Short) 후보를 선별한 뒤, LTF 반등 실패 확정 시 `ENTRY_READY` 신호를 발행하는 엔진입니다.

핵심 조건
- Atlas dir=BEAR, rs_z<=-1.5, score>=50, regime in {bear, range, chaos_range, chaos_vol}
- HTF EMA20 <= EMA60
- LTF: pullback 터치 이후 close<EMA20 + 상단 윗꼬리 비중 조건

실행 예시
```
python -m engines.atlas_rs_fail_short.run --once
```

백테스트 예시
```
python -m engines.atlas_rs_fail_short.backtest_runner --symbols BTC/USDT:USDT,ETH/USDT:USDT --days 30 --out logs/atlas_rs_fail_short/arsf_signals.csv
```
