# 백테스트 콘솔 출력 지침서

이 문서는 엔진 백테스트 콘솔 출력 포맷을 표준화하기 위한 지침서입니다.
다른 엔진 백테스트 러너도 동일한 형식으로 맞춰주세요.

## 커맨드라인 규칙

- 기간 옵션은 `--start`/`--end` 대신 `--days`를 사용한다.
- 디버그 출력은 `--verbose`에서만 켠다.
- 기본 콘솔 출력은 요약 라인만 남긴다.
- SL/TP는 `--sl-pct`, `--tp-pct` 옵션으로 지정한다.

## 심볼별 출력 포맷

심볼마다 1줄 요약을 출력한다:

```
[BACKTEST] <SYMBOL> entries=<N> exits=<N> trades=<N> wins=<N> losses=<N> winrate=<PCT>% tp=<N> sl=<N> avg_mfe=<F> avg_mae=<F> avg_hold=<F> last_day_exits=<N> base_usdt=<F> tp_sum=<F> sl_sum=<F> net_sum=<F> entry_syms=<N>
```

참고:
- `winrate`는 `wins / trades * 100` (소수점 2자리).
- `avg_mfe`/`avg_mae`는 트레이드 평균값.
- `avg_hold`는 **분 단위**(bars 아님).
- 고정 포맷: `winrate=%.2f%%`, `avg_mfe=%.4f`, `avg_mae=%.4f`, `avg_hold=%.1f`.

### 심볼 요약 아래 상세 로그

심볼 요약 바로 아래에 **OPEN/EXIT 로그만** 출력한다.  
ENTRY는 중복 방지를 위해 콘솔에 출력하지 않는다.

정렬 기준:
- `entry_dt` 기준 **내림차순**

출력 포맷:

```
[BACKTEST][OPEN] sym=<SYMBOL> mode=<MODE> side=<SIDE> entry_dt=<YYYY-MM-DD HH:MM> exit_dt= entry_px=<PX> last_px=<PX|N/A> last_dt=<YYYY-MM-DD HH:MM|N/A> unrealized_pct=<PCT|N/A>
[BACKTEST][EXIT] sym=<SYMBOL> mode=<MODE> side=<SIDE> entry_dt=<YYYY-MM-DD HH:MM> exit_dt=<YYYY-MM-DD HH:MM> entry_px=<PX> exit_px=<PX> reason=<TP|SL|TIMEOUT|MANUAL>
```

참고:
- `unrealized_pct`는 open 포지션의 미실현 손익(%).
- `last_px/last_dt`는 해당 심볼의 **마지막 LTF 종가/시간**.
- open 포지션이 없으면 `[OPEN]` 라인은 출력하지 않는다.

### 콘솔 출력 예시

```
[BACKTEST] ARPA/USDT:USDT entries=5 exits=4 trades=4 wins=4 losses=0 winrate=100.00% tp=4 sl=0 avg_mfe=0.0389 avg_mae=0.0584 avg_hold=115.0 last_day_exits=2 base_usdt=10.00 tp_sum=0.800 sl_sum=0.000 net_sum=0.800 entry_syms=3
[BACKTEST][OPEN] sym=ARPA/USDT:USDT mode=swaggy_no_atlas side=LONG entry_dt=2026-01-20 02:15 exit_dt= entry_px=0.01966 last_px=0.01767 last_dt=2026-01-20 06:45 unrealized_pct=-10.12%
[BACKTEST][EXIT] sym=ARPA/USDT:USDT mode=swaggy_no_atlas side=LONG entry_dt=2026-01-20 00:10 exit_dt=2026-01-20 01:45 entry_px=0.01918 exit_px=0.0195636 reason=TP
[BACKTEST][EXIT] sym=ARPA/USDT:USDT mode=swaggy_no_atlas side=LONG entry_dt=2026-01-19 22:15 exit_dt=2026-01-19 23:45 entry_px=0.01967 exit_px=0.0200634 reason=TP
```

## TOTAL 출력 포맷

멀티 모드일 경우:

```
[BACKTEST] TOTAL@<MODE> entries=<N> exits=<N> trades=<N> wins=<N> losses=<N> winrate=<PCT>% tp=<N> sl=<N> avg_mfe=<F> avg_mae=<F> avg_hold=<F> last_day_exits=<N> base_usdt=<F> tp_sum=<F> sl_sum=<F> net_sum=<F> entry_syms=<N>
[BACKTEST] TOTAL entries=<N> exits=<N> trades=<N> wins=<N> losses=<N> winrate=<PCT>% tp=<N> sl=<N> avg_mfe=<F> avg_mae=<F> avg_hold=<F> last_day_exits=<N> base_usdt=<F> tp_sum=<F> sl_sum=<F> net_sum=<F> entry_syms=<N>
```

싱글 모드일 경우:

```
[BACKTEST] TOTAL entries=<N> exits=<N> trades=<N> wins=<N> losses=<N> winrate=<PCT>% tp=<N> sl=<N> avg_mfe=<F> avg_mae=<F> avg_hold=<F> last_day_exits=<N> base_usdt=<F> tp_sum=<F> sl_sum=<F> net_sum=<F> entry_syms=<N>
```

합계는 트레이드 수 가중 평균으로 계산한다:
- `avg_mfe = sum(mfe_per_trade) / total_trades`
- `avg_mae = sum(mae_per_trade) / total_trades`
- `avg_hold = sum(hold_minutes_per_trade) / total_trades`

추가 필드 의미:
- `last_day_exits`: 최근 1일(24h) 기준 청산 횟수
- `base_usdt`: 백테스트 기본 진입 금액
- `tp_sum`: TP 합산 금액 (= tp * base_usdt * tp_pct)
- `sl_sum`: SL 합산 금액 (= sl * base_usdt * sl_pct)
- `net_sum`: tp_sum - sl_sum
- `entry_syms`: 진입 발생 심볼 수

주의:
- 멀티 모드에서만 `TOTAL@<MODE>`와 최종 `TOTAL`을 함께 출력한다.
- 싱글 모드는 `TOTAL@<MODE>`를 출력하지 않는다.

## 시간대/요일 요약 (KST)

TOTAL 아래에 **KST 기준 시간대/요일 요약 표**를 추가한다.

시간대 요약:

```
[BACKTEST] BY_HOUR(KST) hour entries tp sl sl_rate
[BACKTEST] HOUR 00 entries=<N> tp=<N> sl=<N> sl_rate=<PCT>%
...
```

요일 요약:

```
[BACKTEST] BY_DOW(KST) dow entries tp sl sl_rate
[BACKTEST] DOW Mon entries=<N> tp=<N> sl=<N> sl_rate=<PCT>%
...
```

정의:
- `entries`: 해당 시간대/요일 **진입 수**
- `tp`: TP 종료 수
- `sl`: SL 종료 수
- `sl_rate`: `sl / entries * 100` (소수점 2자리)

## 홀드 시간 규칙

LTF 기준 분 단위로 변환:

```
hold_minutes = holding_bars * ltf_minutes
```

## 노이즈 제어

- 백테스트 모드에서는 사이클 단위 funnel 로그를 출력하지 않는다.
- CSV/로그 파일은 저장하되, 기본 콘솔에는 출력하지 않는다.

## 로그/CSV 저장 위치

- 백테스트 로그와 CSV는 `logs/<엔진>/backtest/` 하위에 저장한다.

## 라이브와 동일 조건 (엔진별 백테스트)

- 엔진별 백테스트는 **라이브와 동일한 조건**으로 맞춘다.
  - 미완성봉 제외(라이브에서 `iloc[:-1]`와 동일)
  - 동일한 캔들 window limit 적용
  - 라이브와 동일한 유니버스 산출 방식 사용

### 공통 게이트(라이브 전용) 참고

라이브에서만 engine_runner가 추가로 적용하는 공통 게이트가 있다:

- in_position(다른 엔진 포지션 보유 시 차단)
- 엔트리 락/중복 방지
- MAX_OPEN_POSITIONS 제한
- LIVE_TRADING/LONG_LIVE_TRADING off면 차단

백테스트에는 공통 게이트가 기본으로 존재하지 않으며,
**라이브와 동일 비교가 필요한 엔진은 해당 게이트를 백테스트에 직접 반영**한다.
*** End Patch}/** END of assistant to=functions.apply_patch code **/
