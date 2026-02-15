# 실매매 공통 체크리스트

## 1) 런타임 설정
- /live, /long_live 상태 확인
- /max_pos 값 확인
- /exit_cd_h 값 확인 (청산 쿨다운)
- /entry_usdt 값 확인
- 엔진 토글 ON/OFF 확인 (웹/텔레그램)
  - /sr_pro_short_v1 on|off|status
  - /sr_pro_short_v2 on|off|status
  - /short_bend_15m3m on|off|status
  - /sr_pro_long_v1 on|off|status
  - /scout_only_exhaustion_short on|off|status

## 2) TP/SL 적용
- 공통 TP/SL: /l_exit_tp, /l_exit_sl, /s_exit_tp, /s_exit_sl
- 엔진별 TP/SL override: /engine_exit ENGINE SIDE tp sl
- 엔진명 표기: SR_PRO_SHORT_V1, SR_PRO_SHORT_V2, SR_PRO_LONG_V1, SCOUT_ONLY_EXHAUSTION_SHORT, DTFX, DIV15M_LONG, DIV15M_SHORT, RSI
  - short_bend 엔진: SHORT_BEND_15M3M

## 3) 엔트리 게이트
- max_pos 제한 적용 여부 확인
- 동일 심볼 중복 방지 (entry_lock/entry_guard)
- exit_cooldown 적용 여부 확인

## 4) 알림/로그
- 텔레그램 엔트리 알림 수신 확인
- 엔트리 이벤트 로그 기록 확인
- entry_gate 로그에서 실제 차단 사유 확인

## 5) 상태 동기화
- 재시작 시 API 기준 동기화 완료 여부
- state.json의 엔진 토글/override 값 정상 반영
- in_pos/entry_order_id 동기화 확인
  - _scout_only_exhaustion_short_enabled 값 반영 확인

## 6) 관리모드 화이트리스트
- `_process_manage_queue()`의 `allowed_engines`에 신규 엔진 라벨 포함 확인
- `engine_removed` 스킵 로그가 없는지 확인
