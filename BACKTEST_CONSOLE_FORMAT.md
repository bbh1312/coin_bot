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
[BACKTEST] <SYMBOL> trades=<N> wins=<N> losses=<N> winrate=<PCT>% tp=<N> sl=<N> avg_mfe=<F> avg_mae=<F> avg_hold=<F>
```

참고:
- `winrate`는 `wins / trades * 100` (소수점 2자리).
- `avg_mfe`/`avg_mae`는 트레이드 평균값.
- `avg_hold`는 **분 단위**(bars 아님).
- 고정 포맷: `winrate=%.2f%%`, `avg_mfe=%.4f`, `avg_mae=%.4f`, `avg_hold=%.1f`.

## TOTAL 출력 포맷

마지막에 1줄 합계를 출력한다:

```
[BACKTEST] TOTAL trades=<N> wins=<N> losses=<N> winrate=<PCT>% tp=<N> sl=<N> avg_mfe=<F> avg_mae=<F> avg_hold=<F>
```

합계는 트레이드 수 가중 평균으로 계산한다:
- `avg_mfe = sum(mfe_per_trade) / total_trades`
- `avg_mae = sum(mae_per_trade) / total_trades`
- `avg_hold = sum(hold_minutes_per_trade) / total_trades`

## 홀드 시간 규칙

LTF 기준 분 단위로 변환:

```
hold_minutes = holding_bars * ltf_minutes
```

## 노이즈 제어

- 백테스트 모드에서는 사이클 단위 funnel 로그를 출력하지 않는다.
- CSV/로그 파일은 저장하되, 기본 콘솔에는 출력하지 않는다.
*** End Patch}/** END of assistant to=functions.apply_patch code **/
