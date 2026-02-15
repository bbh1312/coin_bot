# SR_PRO_SHORT_V1 A/B 재검증 플랜

기준일: 2026-02-15 (KST)  
데이터 조건: 공통 유니버스, 캐시 사용, 확정봉

## 공통 옵션

```bash
--days <N> \
--universe common \
--cache-only \
--common-only \
--common-warmup-dir logs/common_warmup/ohlcv \
--use-confirmed
```

## 이미 확인된 결과

1) mid(단일 변경: dvf_norm_max)

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 7 \
  --universe common \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --use-confirmed \
  --dvf-norm-max -0.08
```

- 결과: `TOTAL entries=82 trades=80 winrate=72.50% net_sum=0.241 net_sum_usdt=2.413`

2) 튜닝셋(복합 변경)

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 7 \
  --total-window-days 14 \
  --rolling-zones \
  --universe common \
  --use-confirmed \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --exclude-symbols BTC/USDT \
  --tp-mult 0.988 \
  --sl-atr-mult 0.4 \
  --sl-cap-pct 0.018 \
  --sl-cap-atr-mult 0.6 \
  --dvf-norm-max -0.08 \
  --dvf-norm-immediate -0.25 \
  --dvf-norm-diff-th -0.03 \
  --dvf-confirm-bars 1 \
  --big-bear-body-mult 1.2 \
  --touch-mode bot \
  --ema200-filter
```

- 결과: `TOTAL entries=110 trades=108 winrate=70.37% net_sum=0.425 net_sum_usdt=4.249`

## 권장 A/B 순서 (원인 분리)

### A1. 기준(base) vs mid

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 14 \
  --universe common \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --use-confirmed
```

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 14 \
  --universe common \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --use-confirmed \
  --dvf-norm-max -0.08
```

### A2. mid에서 BTC 제외 효과만 분리

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 14 \
  --universe common \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --use-confirmed \
  --dvf-norm-max -0.08 \
  --exclude-symbols BTC/USDT
```

### A3. mid에서 TP/SL 조합 효과만 분리

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 14 \
  --universe common \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --use-confirmed \
  --dvf-norm-max -0.08 \
  --tp-mult 0.988 \
  --sl-atr-mult 0.4 \
  --sl-cap-pct 0.018 \
  --sl-cap-atr-mult 0.6
```

### A4. 2) 튜닝셋 재검증 (14d, 30d)

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 14 \
  --total-window-days 14 \
  --rolling-zones \
  --universe common \
  --use-confirmed \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --exclude-symbols BTC/USDT \
  --tp-mult 0.988 \
  --sl-atr-mult 0.4 \
  --sl-cap-pct 0.018 \
  --sl-cap-atr-mult 0.6 \
  --dvf-norm-max -0.08 \
  --dvf-norm-immediate -0.25 \
  --dvf-norm-diff-th -0.03 \
  --dvf-confirm-bars 1 \
  --big-bear-body-mult 1.2 \
  --touch-mode bot \
  --ema200-filter
```

```bash
python3 engines/sr_pro_short_v1/run_backtest.py \
  --days 30 \
  --total-window-days 14 \
  --rolling-zones \
  --universe common \
  --use-confirmed \
  --cache-only \
  --common-only \
  --common-warmup-dir logs/common_warmup/ohlcv \
  --exclude-symbols BTC/USDT \
  --tp-mult 0.988 \
  --sl-atr-mult 0.4 \
  --sl-cap-pct 0.018 \
  --sl-cap-atr-mult 0.6 \
  --dvf-norm-max -0.08 \
  --dvf-norm-immediate -0.25 \
  --dvf-norm-diff-th -0.03 \
  --dvf-confirm-bars 1 \
  --big-bear-body-mult 1.2 \
  --touch-mode bot \
  --ema200-filter
```

## 판정 기준(권장)

- 1순위: `net_sum_usdt`
- 2순위: `sl_rate` (낮을수록 안정)
- 3순위: `trades` (너무 과다/과소 회피)

운영 반영은 최소 `14d + 30d`에서 동시에 우세할 때 권장.

## 실행 결과 (2026-02-15)

### A1/A2/A3/A4 결과표

| Case | entries | exits | trades | wins | losses | winrate | tp_sum | sl_sum | net_sum | net_sum_usdt | entry_syms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| A1_BASE_14D | 132 | 131 | 131 | 114 | 17 | 87.02% | 1.140 | -0.315 | 0.825 | 8.254 | 35 |
| A1_MID_14D | 132 | 131 | 131 | 114 | 17 | 87.02% | 1.140 | -0.315 | 0.825 | 8.254 | 35 |
| A2_MID_BTC_OFF_14D | 132 | 131 | 131 | 114 | 17 | 87.02% | 1.140 | -0.315 | 0.825 | 8.254 | 35 |
| A3_MID_TPSL_14D | 119 | 118 | 118 | 96 | 22 | 81.36% | 1.152 | -0.353 | 0.799 | 7.993 | 35 |
| A4_TUNED_14D | 167 | 165 | 165 | 120 | 45 | 72.73% | 1.440 | -0.810 | 0.630 | 6.300 | 26 |
| A4_TUNED_30D | 167 | 165 | 165 | 120 | 45 | 72.73% | 1.440 | -0.810 | 0.630 | 6.300 | 26 |

### 해석 메모

- 현재 캐시 히스토리 범위가 약 14~15일 수준이라 `--days 30`이 사실상 14일과 동일 결과로 수렴했다.
- 이번 실행 기준에서는 A1(기본/중간/BTC 제외)이 동일했고, TP/SL만 바꾼 A3와 복합튜닝 A4는 기대 대비 성능이 낮았다.
