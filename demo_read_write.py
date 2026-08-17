"""읽기 쿼럼 + read repair 데모.

시나리오
--------
1) key를 정상적으로 write 한다 (N=3, W=2).
2) secondary-2가 어떤 이유로 이 데이터를 갖고 있지 않다고 가정하고 (네트워크 파티션 도중 놓친 상황을 흉내) 강제로 store에서 지운다.
3) read(N=3, R=2)를 호출하면 응답한 노드 중 가장 높은 버전을 정답으로 채택하면서, 뒤처진 secondary-2는 백그라운드에서 read repair로 복구한다.
"""

import asyncio

from quorum_store import DataNode, RoutingCoordinator


async def main():
    primary = DataNode("primary", base_latency=0.02, jitter=0.01)
    secondary1 = DataNode("secondary-1", base_latency=0.03, jitter=0.02)
    secondary2 = DataNode("secondary-2", base_latency=0.03, jitter=0.02)

    coordinator = RoutingCoordinator(
        primary, [secondary1, secondary2], write_quorum=2, read_quorum=2
    )

    key = "file/note.txt"

    print("=== 1) 정상 쓰기 ===")
    write_result = await coordinator.write(key, "첫 번째 버전")
    print(f"write 완료: v{write_result.version}, acked_by={write_result.acked_nodes}\n")
    await asyncio.sleep(0.15)  # 백그라운드 복제까지 대기

    print("=== 2) secondary-2가 이 데이터를 놓쳤다고 가정 (강제로 지움) ===")
    secondary2.store.pop(key, None)
    print(f"secondary-2 현재 상태: {secondary2.store.get(key)}\n")

    print("=== 3) 읽기 쿼럼 실행 (read_quorum=2) ===")
    read_result = await coordinator.read(key)
    print(
        f"read 결과: value={read_result.value!r} version={read_result.version} "
        f"replied={read_result.replied_count}/{read_result.total_nodes} "
        f"stale_nodes={read_result.stale_nodes}\n"
    )

    await asyncio.sleep(0.2)  # read repair 백그라운드 작업 대기
    print("=== 4) read repair 이후 secondary-2 상태 ===")
    print(f"secondary-2: {secondary2.store.get(key)}")


if __name__ == "__main__":
    asyncio.run(main())
