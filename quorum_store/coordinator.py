"""라우팅 코디네이터.

담당하는 일:
  1) 쓰기 쿼럼: 주 노드 저장 -> 부 노드 병렬 복제 -> W개 ack 시 응답, 나머지는 백그라운드
  2) 읽기 쿼럼: N개 중 R개 응답을 모아 가장 높은 버전을 정답으로 채택 (Last-Write-Wins)
  3) read repair: 읽기 중 뒤처진(낮은 버전) 노드를 발견하면 최신 값으로 백그라운드 갱신
  4) hinted handoff: 복제 실패한 쓰기를 큐에 넣고 별도 워커가 backoff와 함께 재시도
  5) 헬스 모니터: 주기적으로 ping을 보내 죽은 노드를 감지하고 쿼럼 계산에서 제외
"""

from __future__ import annotations

import asyncio
import itertools
import time

from .models import ReadResult, VersionedValue, WriteResult
from .node import DataNode, NodeFailure


class RoutingCoordinator:
    def __init__(
        self,
        primary: DataNode,
        secondaries: list[DataNode],
        write_quorum: int,
        read_quorum: int,
        heartbeat_interval: float = 0.2,
    ):
        self.primary = primary
        self.secondaries = secondaries
        self.all_nodes = [primary, *secondaries]
        self.total_nodes = len(self.all_nodes)

        if not (1 <= write_quorum <= self.total_nodes):
            raise ValueError("write_quorum은 1~total_nodes 범위여야 합니다.")
        if not (1 <= read_quorum <= self.total_nodes):
            raise ValueError("read_quorum은 1~total_nodes 범위여야 합니다.")

        self.write_quorum = write_quorum
        self.read_quorum = read_quorum

        self._version_counter = itertools.count(1)
        self._hint_queue: asyncio.Queue = asyncio.Queue()
        self._heartbeat_interval = heartbeat_interval
        self._background_tasks: set[asyncio.Task] = set()

    # ---------------------------------------------------------------- 쓰기
    async def write(self, key: str, value: str) -> WriteResult:
        version = next(self._version_counter)
        start = time.monotonic()

        # 1) 주 노드는 동기적으로 먼저 저장한다. 여기서 실패하면 쓰기 자체를 실패로 본다.
        primary_ack = await self.primary.write(key, value, version)
        acked_nodes = [primary_ack.node_id]

        # 2) 살아있다고 판단되는 부 노드들에만 병렬 복제 요청.
        #    이미 죽은 것으로 알려진 노드는 시도조차 하지 않고 바로 힌트 큐에 등록한다
        #    (실제 hinted handoff에서도 도달 불가능한 노드는 바로 핸드오프 대상이 된다).
        pending = {}
        for s in self.secondaries:
            if s.alive:
                pending[asyncio.create_task(s.write(key, value, version))] = s
            else:
                self._hint_queue.put_nowait((s, key, value, version))

        # 3) W개를 채울 때까지만 기다린다.
        while len(acked_nodes) < self.write_quorum and pending:
            done, _ = await asyncio.wait(pending.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                node = pending.pop(task)
                try:
                    ack = task.result()
                    acked_nodes.append(ack.node_id)
                except NodeFailure as e:
                    print(f"  [경고] {e} -> 힌트 큐 등록")
                    self._hint_queue.put_nowait((node, key, value, version))

        elapsed = time.monotonic() - start
        success = len(acked_nodes) >= self.write_quorum

        # 4) 쿼럼을 채우고 남은 복제는 백그라운드로 넘긴다.
        if pending:
            self._spawn_background(self._finish_write_background(key, value, version, pending))

        return WriteResult(
            key=key,
            version=version,
            acked_count=len(acked_nodes),
            quorum=self.write_quorum,
            total_nodes=self.total_nodes,
            elapsed=elapsed,
            success=success,
            acked_nodes=acked_nodes,
        )

    async def _finish_write_background(self, key, value, version, pending):
        results = await asyncio.gather(*pending.keys(), return_exceptions=True)
        for node, result in zip(pending.values(), results):
            if isinstance(result, Exception):
                print(f"  [백그라운드] {node.node_id} 복제 실패 -> 힌트 큐 등록")
                self._hint_queue.put_nowait((node, key, value, version))
            else:
                print(f"  [백그라운드] {node.node_id} 복제 완료 (v{version})")

    # ---------------------------------------------------------------- 읽기
    async def read(self, key: str) -> ReadResult:
        start = time.monotonic()
        tasks = {asyncio.create_task(n.read(key)): n for n in self.all_nodes if n.alive}
        replies = []

        while len(replies) < self.read_quorum and tasks:
            done, _ = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                node = tasks.pop(task)
                try:
                    replies.append(task.result())
                except NodeFailure as e:
                    print(f"  [경고] {e}")

        elapsed = time.monotonic() - start
        success = len(replies) >= self.read_quorum

        # 응답 중 가장 높은 버전을 정답으로 채택
        latest: VersionedValue | None = None
        for r in replies:
            if r.value is not None and (latest is None or r.value.version > latest.version):
                latest = r.value

        stale = [
            r.node_id
            for r in replies
            if latest is not None and (r.value is None or r.value.version < latest.version)
        ]

        if latest is not None and stale:
            stale_nodes = [n for n in self.all_nodes if n.node_id in stale]
            self._spawn_background(self._read_repair(key, latest, stale_nodes))

        # 쿼럼 채우고 남은 노드 응답도 뒤늦게 도착하면 read repair 후보로 확인
        if tasks:
            self._spawn_background(self._drain_remaining_reads(key, latest, tasks))

        return ReadResult(
            key=key,
            value=latest.value if latest else None,
            version=latest.version if latest else None,
            replied_count=len(replies),
            quorum=self.read_quorum,
            total_nodes=self.total_nodes,
            elapsed=elapsed,
            success=success,
            stale_nodes=stale,
        )

    async def _read_repair(self, key: str, latest: VersionedValue, stale_nodes: list[DataNode]):
        for node in stale_nodes:
            try:
                await node.write(key, latest.value, latest.version)
                print(f"  [read repair] {node.node_id} -> v{latest.version}로 갱신")
            except NodeFailure:
                self._hint_queue.put_nowait((node, key, latest.value, latest.version))

    async def _drain_remaining_reads(self, key, latest, tasks):
        results = await asyncio.gather(*tasks.keys(), return_exceptions=True)
        stale_nodes = []
        for node, result in zip(tasks.values(), results):
            if isinstance(result, Exception):
                continue
            if latest and (result.value is None or result.value.version < latest.version):
                stale_nodes.append(node)
        if latest and stale_nodes:
            await self._read_repair(key, latest, stale_nodes)

    # ------------------------------------------------- hinted handoff 재시도
    async def hint_worker(self, retry_interval: float = 0.3, max_retries: int = 5):
        """복제 실패 큐를 계속 소비하며 backoff 재시도하는 백그라운드 워커."""
        while True:
            node, key, value, version = await self._hint_queue.get()
            for attempt in range(1, max_retries + 1):
                try:
                    await node.write(key, value, version)
                    print(f"  [hinted handoff] {node.node_id} 재시도 성공 (v{version}, {attempt}번째)")
                    break
                except NodeFailure:
                    await asyncio.sleep(retry_interval * attempt)
            else:
                print(f"  [hinted handoff] {node.node_id} 재시도 {max_retries}회 모두 실패, 포기")
            self._hint_queue.task_done()

    # -------------------------------------------------------- 헬스 모니터
    async def health_monitor(self):
        """주기적으로 모든 노드를 ping하여 alive 상태를 갱신한다."""
        while True:
            for node in self.all_nodes:
                ok = await node.ping()
                if node.alive and not ok:
                    print(f"  [헬스체크] {node.node_id} DOWN 처리")
                elif not node.alive and ok:
                    print(f"  [헬스체크] {node.node_id} 복구 감지, UP 처리")
                node.alive = ok
            await asyncio.sleep(self._heartbeat_interval)

    # ------------------------------------------------------------- 유틸
    def _spawn_background(self, coro):
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def start_background_workers(self):
        """hint_worker와 health_monitor를 백그라운드로 띄운다. 서비스 시작 시 한 번 호출."""
        self._spawn_background(self.hint_worker())
        self._spawn_background(self.health_monitor())
