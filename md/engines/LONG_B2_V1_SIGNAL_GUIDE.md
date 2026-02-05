# LONG_B2_V1 신호 기준 (백테스트 기준)

작성 기준: 2026-02-05

---

## 1) 타임프레임 구성
- 상위 프레임(Trend): **1h**
- 기준 프레임(Main): **15m**
- 실행 프레임(Trigger): **3m**
- BTC 세이프티 가드: **1h / 15m / 1m**

---

## 2) BTC 세이프티 가드 (AND)
다음 조건이 모두 만족될 때만 엔진 활성:
1. **BTC 1h**: `Close > EMA20`
2. **BTC 15m**: `RSI > 48`
3. **BTC 1m**: 최근 5개 캔들 내 **-0.3% 이상 급락 없음**
   - 계산: `close[n] - close[n-1] <= -0.003` 발생 시 차단

---

## 3) 추세 필터 (1h + 15m)
엔트리 후보가 되려면:
- 1h: `Close > EMA120` **AND** `EMA20 > EMA60`
- 15m: `EMA20 > EMA60`

---

## 4) 보조 지표 계산
- 15m: **Bollinger Middle(20MA)**, **Lower Band(20,2)**, **RSI(14)**, **ATR(14)**
- 3m: **EMA10**, **RSI(14)**, **ATR(14)**, **OBV**, **상승봉 평균 거래량(lookback_up)**

---

## 5) 거래량/OBV/변동성 필터
- **상승봉 평균 거래량**: 최근 `lookback_up` 구간에서 **양봉만 평균**
- **Volume Ratio**: `current_volume / avg_up_volume`
  - 음봉일 때만 비교 적용
  - 조건: `vol_ratio <= vol_ratio_max`
- **OBV 보조 조건**: `OBV_now - OBV_prev >= obv_flat_min`
- **ATR 스파이크 제외**: 
  - `range(high-low) >= ATR * atr_spike_mult` 이면 진입 차단

---

## 6) 핵심 진입 트리거 (OR)
아래 4개 중 하나만 만족하면 진입 후보:

### A) Gap Filler
- 이전봉 종가가 **EMA10 아래**, 다음 봉 종가가 **EMA10 위로 복귀**
  - 조건: `prev_close < EMA10(prev)` **AND** `cur_close > EMA10(cur)`

### B) Spring Trap
- 15m에서 **밴드 중단 하향 이탈 후 복귀**
  - `main_low < BB_mid` **AND** `main_close > BB_mid`
- 동시에 `RSI(15m) >= 48` **AND** `RSI 상승중`

### C) Golden Pocket
- 가격이 **Fib 0.382/0.5/0.618 근처** 또는 **BB 하단 접촉**
- 동시에 `OBV 유지/상승` **AND** `vol_ratio <= vol_ratio_max`

### D) Recovery Trigger
- 최근 15m에서 **BB 중단 이탈**이 발생했고,
- **3봉 이내**에 `main_close > BB_mid`로 복귀
- 동시에 `OBV 유지/상승` **AND** `vol_ratio <= vol_ratio_max`

---

## 7) 진입 가격
- **3m 다음 봉 종가** 기준으로 진입
- 확정봉(`--use-confirmed`) 기준

---

## 8) 손절/익절/관리 로직

### 손절 (SL)
- `max(ATR(15m) * 1.5, entry - prev_low)` 하단

### 익절 1 (TP1)
- **전고점(최근 스윙 하이)** 도달 시 **30% 부분 익절**

### 익절 2 (Trailing)
- TP1 이후 최고점 기준 **-1%** 밀리면 잔여 전량 청산

### 타임컷
- 진입 후 **2시간 경과** 시 잔여 전량 청산

---

## 9) 기본 파라미터
- `lookback_up=10`
- `vol_ratio_max=0.4`
- `obv_flat_min=0.0`
- `atr_spike_mult=1.8`
- `tp1_ratio=0.4`
- `trail_pct=0.008`
- `time_stop_min=150`

---

## 10) 백테스트 공통 지침
- 공통 캐시 사용
- 확정봉 기준
- SL 우선 판정
- 콘솔 로그: 심볼 요약 + TOTAL
- 워밍업 자동 계산 적용

