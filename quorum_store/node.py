"""데이터 노드 시뮬레이션.

실제로는 여기서 디스크 I/O나 네트워크 호출이 일어나지만, 학습용 시뮬레이션에서는
asyncio.sleep으로 지연을, random.random()으로 장애를 대강 흉내내봄
"""

from __future__ import annotations

import asyncio
import random

from .models import ReadReply, VersionedValue, WriteAck


class NodeFailure(Exception):
    """노드가 일시적으로 응답하지 못했음을 나타내는 예외."""


class DataNode:
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
        self.store: dict[str, VersionedValue] = {}
        self.alive = True  # 헬스 모니터가 갱신하는 상태

    def _simulate_delay(self) -> float:
        return self.base_latency + random.random() * self.jitter

    async def write(self, key: str, value: str, version: int) -> WriteAck:
        delay = self._simulate_delay()
        await asyncio.sleep(delay)

        if not self.alive or random.random() < self.failure_rate:
            raise NodeFailure(f"{self.node_id} 응답 없음 (timeout/장애 시뮬레이션)")

        existing = self.store.get(key)
        # 더 낮은 버전으로 덮어쓰지 않는다 (뒤늦게 도착한 재시도 대비).
        if existing is None or version >= existing.version:
            self.store[key] = VersionedValue(value=value, version=version)

        return WriteAck(node_id=self.node_id, version=version, latency=delay)

    async def read(self, key: str) -> ReadReply:
        delay = self._simulate_delay()
        await asyncio.sleep(delay)

        if not self.alive or random.random() < self.failure_rate:
            raise NodeFailure(f"{self.node_id} 응답 없음 (timeout/장애 시뮬레이션)")

        return ReadReply(node_id=self.node_id, value=self.store.get(key), latency=delay)

    async def ping(self) -> bool:
        await asyncio.sleep(0.005)
        return random.random() >= self.failure_rate
