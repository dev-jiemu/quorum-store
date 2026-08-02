"""
데이터 라우팅 서비스 - 쓰기(write) 쿼럼 로직 시뮬레이션

구조
----
- DataNode: 주/부 데이터 노드를 흉내내는 클래스. 네트워크 지연과 장애를
  asyncio.sleep + 확률적 실패로 시뮬레이션한다.
- RoutingCoordinator: 클라이언트 요청을 받아 주 노드에 먼저 저장하고,
  부 노드로 병렬 복제 요청을 보낸 뒤 N개 중 W개의 ack가 모이면 즉시
  클라이언트에 응답한다. 나머지 부 노드로의 복제는 백그라운드에서 계속
  진행된다(fire-and-forget).

이 파일 하나로 아래 세 가지 정책을 모두 표현할 수 있다.
  - W = 1            : 주 노드만 ack (기존에 얘기했던 "빠르지만 정합성 낮음")
  - W = N             : 모든 노드 ack (정합성 높지만 느림)
  - 1 < W < N         : 쿼럼(과반수) ack, 실무에서 흔히 쓰는 절충안
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field


class NodeFailure(Exception):
    """노드가 일시적으로 응답 못하면 예외처리하기 ㅇㅂㅇ"""


@dataclass
class WriteAck:
    node_id: str
    latency: float


@dataclass
class WriteResult:
    key: str
    acked_count: int
    quorum: int
    total_nodes: int
    elapsed: float
    success: bool
    acked_nodes: list[str] = field(default_factory=list)


class DataNode:
    """저장 노드 시뮬레이션. 실제로는 여기서 디스크 I/O나 네트워크 호출이 일어난다."""

    def __init__(
        self,
        node_id: str,
        base_latency: float = 0.02,
        jitter: float = 0.03,
        failure_rate: float = 0.0,
    ):
        self.node_id = node_id
        self.base_latency = base_latency
        self.jitter = jitter
        self.failure_rate = failure_rate
        self.store: dict[str, str] = {}

    async def write(self, key: str, value: str) -> WriteAck:
        delay = self.base_latency + random.random() * self.jitter
        await asyncio.sleep(delay)

        # 일정 확률로 장애내기
        if random.random() < self.failure_rate:
            raise NodeFailure(f"{self.node_id} 응답 없음 (timeout/장애 시뮬레이션)")

        self.store[key] = value
        return WriteAck(node_id=self.node_id, latency=delay)


class RoutingCoordinator:
    """클라이언트 요청을 주 노드/부 노드로 라우팅하는 서비스."""

    def __init__(self, primary: DataNode, secondaries: list[DataNode], write_quorum: int):
        self.primary = primary
        self.secondaries = secondaries
        self.total_nodes = 1 + len(secondaries)

        if not (1 <= write_quorum <= self.total_nodes):
            raise ValueError("write_quorum은 1 이상 total_nodes 이하여야 합니다.")
        self.write_quorum = write_quorum

    async def write(self, key: str, value: str) -> WriteResult:
        start = time.monotonic()

        # 주 노드에는 먼저 동기적으로 저장한다. 여기서 실패하면 전체 쓰기 실패로 본다.
        primary_ack = await self.primary.write(key, value)
        acked_nodes = [primary_ack.node_id]

        # 부 노드들에는 병렬로 복제 요청을 보냄
        pending = {asyncio.create_task(s.write(key, value)): s for s in self.secondaries}

        # W개(주 노드 포함)를 채울 때까지만 기다리고 응답함 -> 나머지는 알아서 백그라운드 job
        while len(acked_nodes) < self.write_quorum and pending:
            done, still_pending = await asyncio.wait(
                pending.keys(), return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                node = pending.pop(task)
                try:
                    ack = task.result()
                    acked_nodes.append(ack.node_id)
                except NodeFailure as e:
                    print(f"  [경고] {e}")
            pending = {t: n for t, n in pending.items() if t in still_pending}

        elapsed = time.monotonic() - start
        success = len(acked_nodes) >= self.write_quorum

        # 4) 쿼럼을 채우고도 남은 부 노드 복제는 백그라운드로 넘기고 응답부터 반환
        if pending:
            asyncio.create_task(self._finish_in_background(key, pending))

        return WriteResult(
            key=key,
            acked_count=len(acked_nodes),
            quorum=self.write_quorum,
            total_nodes=self.total_nodes,
            elapsed=elapsed,
            success=success,
            acked_nodes=acked_nodes,
        )

    async def _finish_in_background(self, key: str, pending: dict[asyncio.Task, DataNode]):
        """쿼럼 응답 이후에도 계속 진행되는 나머지 복제. 실패하면 재시도 큐로 넘기는
        자리(hinted handoff 등)로, 여기서는 로그만 남긴다."""
        results = await asyncio.gather(*pending.keys(), return_exceptions=True)
        for node, result in zip(pending.values(), results):
            if isinstance(result, Exception):
                print(f"  [백그라운드] {node.node_id} 복제 실패 -> 재시도 큐 등록 필요: {result}")
            else:
                print(f"  [백그라운드] {node.node_id} 복제 완료 (쿼럼 응답 이후 도착)")


async def demo(write_quorum: int, failure_rate: float = 0.0, label: str = ""):
    primary = DataNode("primary", base_latency=0.02, jitter=0.01)
    secondaries = [
        DataNode(f"secondary-{i}", base_latency=0.05, jitter=0.08, failure_rate=failure_rate)
        for i in range(1, 3)
    ]  # N = 3 (primary + secondary-1, secondary-2)

    coordinator = RoutingCoordinator(primary, secondaries, write_quorum=write_quorum)

    result = await coordinator.write("file/hello.txt", "hello world")
    print(
        f"[{label}] quorum={result.quorum}/{result.total_nodes} "
        f"ack={result.acked_count} elapsed={result.elapsed*1000:.1f}ms "
        f"success={result.success} acked_by={result.acked_nodes}"
    )
    # 백그라운드 복제가 있다면 마저 끝날 시간을 잠깐 준다 (데모용).
    await asyncio.sleep(0.15)
    print()


async def main():
    random.seed(7)
    print("=== 정책별 응답 시간 비교 (N=3: primary + secondary-1, secondary-2) ===\n")

    # 주 노드만 ack (W=1) : 가장 빠르지만 정합성 낮음
    await demo(write_quorum=1, label="W=1 (primary only)")

    # 쿼럼 ack (W=2) : 절충안, 실무에서 흔히 쓰는 설정
    await demo(write_quorum=2, label="W=2 (quorum)")

    # 전체 ack (W=3) : 가장 느리지만 정합성 최고
    await demo(write_quorum=3, label="W=3 (all nodes)")

    # 부 노드 하나가 자주 실패하는 상황에서 쿼럼이 주는 이점
    print("=== 부 노드 장애율 40%일 때 ===\n")
    await demo(write_quorum=2, failure_rate=0.4, label="W=2, failure_rate=0.4")


if __name__ == "__main__":
    asyncio.run(main())
