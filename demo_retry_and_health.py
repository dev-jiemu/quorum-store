"""hinted handoff 재시도 + 헬스 모니터 데모.

시나리오
--------
1) secondary-2를 처음부터 failure_rate=1.0(항상 실패)으로 만들어 장애 상태를 흉내낸다.
2) 코디네이터의 백그라운드 워커(hint_worker, health_monitor)를 띄운다.
3) 몇 번 쓰기를 수행하면 secondary-2로의 복제는 계속 실패해 힌트 큐에 쌓이고,
   헬스 모니터도 곧 secondary-2를 DOWN으로 표시해 다음 쓰기부터는 요청 자체를 보내지 않는다.
4) secondary-2가 복구됐다고 가정(failure_rate를 0으로 낮춤)하면 헬스 모니터가
   다시 UP으로 표시하고, hint_worker가 밀려있던 쓰기를 재전송해 결국 최신 데이터를 갖게 된다.
"""

import asyncio

from quorum_store import DataNode, RoutingCoordinator


async def main():
    primary = DataNode("primary", base_latency=0.02, jitter=0.01)
    secondary1 = DataNode("secondary-1", base_latency=0.03, jitter=0.02)
    secondary2 = DataNode("secondary-2", base_latency=0.03, jitter=0.02, failure_rate=1.0)

    coordinator = RoutingCoordinator(
        primary,
        [secondary1, secondary2],
        write_quorum=2,
        read_quorum=2,
        heartbeat_interval=0.1,
    )
    coordinator.start_background_workers()

    key = "file/status.txt"

    print("=== 1) secondary-2 장애 상태에서 쓰기 3번 ===")
    for i in range(1, 4):
        result = await coordinator.write(key, f"업데이트 {i}")
        print(f"  write v{result.version} 완료 (acked_by={result.acked_nodes})")
        await asyncio.sleep(0.15)

    await asyncio.sleep(0.3)
    print(f"\nsecondary-2 alive 상태: {secondary2.alive}")
    print(f"secondary-2 저장값: {secondary2.store.get(key)}\n")

    print("=== 2) secondary-2 복구 (failure_rate=0으로 낮춤) ===")
    secondary2.failure_rate = 0.0

    # 헬스 모니터가 UP으로 되돌리고, hint_worker가 밀린 재시도를 처리할 시간을 준다.
    await asyncio.sleep(1.2)

    print(f"secondary-2 alive 상태: {secondary2.alive}")
    print(f"secondary-2 최종 저장값: {secondary2.store.get(key)}")


if __name__ == "__main__":
    asyncio.run(main())
