# 실매매 엔진 연결 지침서

이 문서는 최근 엔진들의 실매매 연결 과정에서 적용했던 기준을 정리한 지침서입니다. 신규 엔진 추가 시 동일한 흐름으로 적용하세요.

## 1) 엔진 기본 구조
- `engines/<engine_name>/engine.py`에 config dataclass 생성
- `engines/<engine_name>/run_backtest.py` 생성
- 기본값은 실매매 기준과 동일하게 설정

## 2) 확정봉 기준 통일
- 실매매와 백테스트 모두 동일한 확정봉 기준 사용
- `df.iloc[:-1]`로 마지막 미확정봉 제외
- 확정봉 사용 시 **fetch limit는 +1** (부족 길이 방지)

## 3) 공통 캐시/워밍업
- 실매매는 `cycle_cache.get_df()` + `logs/common_warmup/ohlcv` 캐시 사용
- 필요한 TF가 `COMMON_WARMUP_TFS`에 포함되어 있어야 함
- 워밍업 로직:
  - 기존 캐시 파일 먼저 로드
  - 부족 데이터만 REST로 보충
  - 최근 N봉 기준 갭 감지 후 재조회

## 4) realtime-only 실행 조건
- realtime-only 모드에서 돌릴 엔진은 `_realtime_only_required()`에 추가
- 1분봉 엔진은 `new_1m_bar` 조건으로 실행

## 5) 메인 루프 연결
- 유니버스 연결 (보통 shared_universe)
- 사이클별 실행 플래그 추가 (예: `<engine>_ran`)
- `_run_<engine>_cycle`을 스레드로 실행
- 콘솔 로그에 cycle start/end 출력

## 6) 웹/텔레그램 토글
- env enable 플래그 추가 (`*_ENABLED`)
- 텔레그램 명령 `/engine_name on|off|status` 추가
- 웹 UI 토글 추가 (`web_app/app.py`, `index.html`)
- `/status`에 엔진 상태 표시

## 6-1) 관리모드(관리 큐) 연결
- `engine_runner.py`의 `_process_manage_queue()` 내 `allowed_engines`에 엔진 라벨 추가
- 관리모드에서 `engine_removed`로 스킵되면 이 목록 누락이 원인

## 7) 엔진 라벨 매핑
- `_engine_label_from_reason`
- `_reason_from_engine_label`
- `_display_engine_label`
- `_is_engine_enabled`

## 8) 팔로워 동기화
- 엔트리/청산 브로드캐스트 경로 확인
- 리콘실 종료 시에도 팔로워 청산 전파되도록 처리

## 9) 백테스트 로그 포맷
- `BACKTEST_GUIDE_COMMON.md` 형식 준수
- 심볼별 요약 + TOTAL 요약
- 필요 시 시간대/요일 요약 추가

## 9-1) 실매매 로그 경로
- 엔진별 로그는 `logs/<engine_name>/<engine_name>-YYYY-MM-DD.log`
- 실매매 사이클 로그는 `*_CYCLE_START/END`, 신호 로그는 `*_SIGNAL` 형식으로 기록

## 10) 실매매 연결 체크리스트
- 엔진 기본 설정값이 실매매 기준으로 고정되어 있는지 확인
- 확정봉 기준이 백테스트와 동일한지 확인 (이전봉 사용)
- 공통 워밍업 TF에 필요한 타임프레임이 포함되어 있는지 확인
- 캐시 파일에 최근 N봉 연속성이 보장되는지 확인 (갭 없음)
- 워밍업 로그에 `REFETCH_GAP`, `MISSING_PASS` 동작 여부 확인
- `_realtime_only_required()`에 엔진이 포함되어 있는지 확인
- 메인 루프에서 엔진 스레드가 실제 호출되는지 확인
- 콘솔 로그에 `*_CYCLE_START/END`가 찍히는지 확인
- 텔레그램/웹에 엔진 ON/OFF 토글이 있는지 확인
- `/status`에 엔진 상태/공통 워밍업 상태가 표시되는지 확인
- 엔진 라벨 매핑이 등록되어 있는지 확인
- 팔로워 계정 동기화(엔트리/청산) 동작 확인
- 관리모드에서 엔진 요청이 `engine_removed`로 스킵되지 않는지 확인
- **런타임 설정 동기화 키 목록에 엔진 토글 키가 포함되는지 확인**
  - `_reload_runtime_settings_from_disk()`의 `keys`
  - `save_state()` / `save_state_to()`의 `runtime_keys`
- **`_reload_runtime_settings_from_disk()`의 `global` 선언에 엔진 토글 플래그가 포함되어 있는지 확인**
  - 예: `SR_PRO_LONG_V1_ENABLED` 같은 글로벌 누락 시 `/status`가 OFF로 고정됨

## 참고 문서
- `md/engines/BACKTEST_GUIDE_COMMON.md`
- `md/engines/COMMON_LIVE_CHECKLIST.md`
