"""쓰기/읽기에서 주고받는 공용 데이터 모델.

버전(version)은 실제 시스템의 vector clock 대신, 코디네이터가 쓰기마다 1씩 증가시켜 부여하는 단순한 정수를 사용
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class VersionedValue:
    value: str
    version: int
    written_at: float = field(default_factory=time.time)


@dataclass
class WriteAck:
    node_id: str
    version: int
    latency: float


@dataclass
class WriteResult:
    key: str
    version: int
    acked_count: int
    quorum: int
    total_nodes: int
    elapsed: float
    success: bool
    acked_nodes: list[str] = field(default_factory=list)


@dataclass
class ReadReply:
    node_id: str
    value: VersionedValue | None
    latency: float


@dataclass
class ReadResult:
    key: str
    value: str | None
    version: int | None
    replied_count: int
    quorum: int
    total_nodes: int
    elapsed: float
    success: bool
    stale_nodes: list[str] = field(default_factory=list)
