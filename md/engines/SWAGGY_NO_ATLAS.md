# SWAGGY_NO_ATLAS 엔진

Atlas 게이트 없이 Swaggy 시그널만으로 진입하는 경량 버전이다.

## 1) 엔진 구조 요약

- SwaggySignalEngine가 진입 시그널 생성
- Atlas 평가/정책 적용 없음 (Atlas pass_hard/soft 무시)
- Over-extension(추격 방지) 로직은 Swaggy 시그널 내부 계산 결과를 그대로 사용
- 최종 통과 시 시장가 진입, TP/SL은 공통 모듈(auto_exit)이 처리 (엔진별 override 가능)

## 2) 시간프레임

- LTF: 5m
- MTF: 15m
- HTF: 1h
- HTF2: 4h

## 3) 진입 로직 (Atlas 미사용)

- SwaggySignalEngine의 조건만 사용
- 레벨/터치/트리거/confirm/overext 판단은 기존 Swaggy와 동일
- Atlas 관련 필터/정책은 모두 비활성

## 4) 로그

- 엔진 로그: `logs/swaggy_no_atlas/swaggy_no_atlas-YYYY-MM-DD.log`
- 트레이드 JSONL: `logs/swaggy_trades.jsonl` (engine=SWAGGY_NO_ATLAS)
- Atlas 지표 필드는 null로 기록됨

## 5) 토글/상태

- 텔레그램: `/swaggy_no_atlas on|off|status`
- 웹 UI: Engines 섹션의 "Swaggy No Atlas"

## 6) 백테스트 실행

백테스트는 Swaggy 단독(Atlas 미사용) 러너를 사용한다.

```bash
python3 -m engines.swaggy_atlas_lab.run_backtest_no_atlas \
  --days 7 \
  --tp-pct 0.03 \
  --sl-pct 0.30 \
  --cooldown-min 30 \
  --max-symbols 40
```

메모
- `run_backtest_no_atlas`는 Atlas 관련 `--mode` 옵션을 지원하지 않는다.
- 로그/리포트 경로는 `logs/swaggy_atlas_lab/backtest/`, `reports/swaggy_atlas_lab/`에 생성된다.
- 콘솔 출력 형식은 `md/engines/BACKTEST_CONSOLE_FORMAT.md` 지침을 따른다.
