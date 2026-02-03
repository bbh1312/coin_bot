# 공통 백테스트 지침서 (실매매 동기화 기준)

작성 기준: 2026-02-03

---

## 1) 실매매와 동일한 데이터/봉 기준

### 핵심 원칙
- 실매매와 같은 **공통 캐시(`common_warmup`)**를 사용한다.
- **확정봉 기준(이전봉)**으로 계산한다.
- **유니버스도 실매매 공통 유니버스**를 그대로 사용한다.

### 권장 옵션
- `--use-live-cache` : 공통 캐시 사용 강제
- `--use-confirmed` : 이전봉 기준 계산
- `--universe common` : 실매매 공통 유니버스(`logs/common_universe/latest.txt`)

---

## 2) 캐시 사용 (실매매 공통 캐시)

### 캐시 파일 경로
- 기본: `logs/common_warmup/ohlcv/`
- 각 심볼/타임프레임 파일 예시: `BTC_USDT_USDT_3m.csv`

### 백테스트에서 캐시 사용 방법
```
--use-live-cache \
--common-warmup-dir logs/common_warmup/ohlcv
```

> `--use-live-cache`가 켜져 있으면 `COMMON_WARMUP_CACHE_DIR` 환경변수도 자동 참조한다.

---

## 3) SL 우선 판정

### 룰
- 한 봉에서 TP와 SL이 동시에 충족되면 **SL을 우선 처리**한다.

---

## 4) 콘솔 로그 출력 규칙

### 콘솔 출력은 심볼별 요약 + TOTAL만 출력
- 개별 ENTRY/EXIT 로그는 출력하지 않는다.

### 출력 예시
```
[BACKTEST] CYBER/USDT:USDT entries=1 exits=1 trades=1 wins=0 losses=1 winrate=0.00% avg_mfe=0.0293 avg_mae=0.0588 avg_hold=1.0 net_sum=-0.250
[BACKTEST] TOTAL entries=29 exits=25 trades=25 wins=13 losses=12 winrate=52.00% avg_mfe=0.0263 avg_mae=0.0285 avg_hold=12.8 net_sum=0.899
```

### 계산 항목 정의
- `avg_mfe`: 평균 최대 유리 변동 (MFE)
- `avg_mae`: 평균 최대 불리 변동 (MAE)
- `avg_hold`: 평균 보유 바 수
- `net_sum`: pnl% 합산 값

---

## 5) 표준 실행 명령어 (실매매 동기화)

```
python engines/<ENGINE>/run_backtest.py \
  --days 3 \
  --universe common \
  --use-confirmed \
  --use-live-cache \
  --tp-pct 0.02 \
  --sl-pct 0.02 \
  --cooldown-bars 5
```

---

## 6) 정합성 체크 포인트

1) `logs/common_universe/latest.txt`가 최신인지 확인
2) `logs/common_warmup/ohlcv/*_<tf>.csv` 타임스탬프가 최신인지 확인
3) `--use-confirmed` 옵션이 켜져 있는지 확인
4) SL/TP 동봉 충돌 시 SL 우선 확인
5) 콘솔 로그가 요약만 출력되는지 확인
