# SWAGGY_NO_ATLAS 엔진

Atlas 게이트 없이 Swaggy 시그널만으로 진입하는 경량 버전이다. SwaggySignalEngine의 로직을 그대로 사용하며, Atlas 평가/정책은 모두 비활성화된다.

## 1) 개요

- 목적: Atlas 의존 없이 Swaggy 시그널만으로 빠르게 진입 후보를 생성
- 성격: 신호 빈도는 높아질 수 있으나, Atlas 품질 필터는 없음
- 사용처: Atlas 상태가 불안정하거나, Swaggy 자체의 성능만 분리 검증할 때

## 2) 엔진 구조 요약

- SwaggySignalEngine가 레벨/터치/트리거/confirm/overext 판단을 수행
- Atlas 관련 필터/정책 적용 없음 (pass_hard/soft/atlas_mult 무시)
- Over-extension(추격 방지) 로직은 Swaggy 내부 계산 결과 그대로 사용
- 최종 통과 시 시장가 진입, TP/SL은 공통 모듈(auto_exit)이 처리

## 3) 시간프레임

- LTF: 5m
- MTF: 15m
- HTF: 1h
- HTF2: 4h
- D1: 1d

## 4) 진입 파이프라인 (Swaggy 단독)

1) 레벨 생성/갱신 (Swaggy 레벨)
2) 터치 판정 (touch_pass / fail_reason 기록)
3) 트리거 조합 평가 (trigger_combo)
4) confirm 통과 여부 평가 (confirm_pass / confirm_fail)
5) overext 체크 (추격 방지)
6) ENTRY_READY 발생 시 진입 (시장가)

## 4-1) 초보자용 설명 (차트를 잘 몰라도 이해 가능한 버전)

스웨기 단독 엔진은 "중요한 가격대에 닿았다가, 그 자리에서 반전 신호가 나올 때" 진입합니다.

진입 흐름을 쉬운 말로 정리하면 다음과 같습니다:

1) 중요한 가격대 찾기
   - 최근 많은 거래가 몰린 가격이나, 크게 흔들렸던 가격대를 "중요한 자리"로 봅니다.

2) 가격이 그 자리에 실제로 닿았는지 확인
   - 그냥 근처에 있는 게 아니라, 실제로 닿았는지 확인합니다.

3) 닿은 뒤에 되돌림/반전 신호가 있는지 확인
   - 한번 뚫었다가 다시 돌아오는 움직임 같은 "반전 신호"가 있어야 들어갑니다.

4) 너무 멀리 튀어버린 상태(과열)이면 스킵
   - 가격이 너무 멀리 튀면 "쫓아가는 진입"이 되기 때문에 들어가지 않습니다.

5) 아주 짧은 시간대 흐름이 진입 방향과 맞는지 확인
   - 마지막으로 방향이 맞는지 확인한 뒤 진입합니다.

정리하면:
"중요한 자리 도착 → 반전 신호 확인 → 과열 여부 확인 → 방향 확인 → 진입" 입니다.

## 5) 진입 정보 (핵심 필드)

- `entry_px`: 진입 가격 (LTF 확정봉 기준)
- `confirm_type`: confirm 패턴 요약
- `trigger_bits`: 트리거 비트 플래그
- `overext_dist_at_entry`: 과열 거리 (ATR 기준)
- `level_score`, `touch_count`, `level_age` 등 레벨 품질 지표

## 6) 로그

- 엔진 로그: `logs/swaggy_no_atlas/swaggy_no_atlas-YYYY-MM-DD.log`
- 트레이드 JSONL: `logs/swaggy_trades.jsonl` (engine=SWAGGY_NO_ATLAS)
- Atlas 지표 필드는 null로 기록됨

## 7) 토글/상태

- 텔레그램: `/swaggy_no_atlas on|off|status`
- 웹 UI: Engines 섹션의 "Swaggy No Atlas"

## 8) 백테스트 실행

```bash
python3 -m engines.swaggy_no_atlas.run_backtest \
  --days 7 \
  --tp-pct 0.03 \
  --sl-pct 0.30 \
  --cooldown-min 30 \
  --max-symbols 40
```

메모
- `run_backtest`는 Atlas 관련 `--mode` 옵션을 지원하지 않는다.
- 로그/리포트 경로는 `logs/swaggy_no_atlas/backtest/`에 생성된다.
- 콘솔 출력 형식은 `md/engines/BACKTEST_CONSOLE_FORMAT.md` 지침을 따른다.
