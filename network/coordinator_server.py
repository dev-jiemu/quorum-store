"""쿼럼 라우팅을 실제 HTTP 호출로 수행하는 코디네이터 서버 (FastAPI).

quorum_store.coordinator.RoutingCoordinator와 로직은 동일하지만(주 노드 우선
저장 -> 부 노드 병렬 복제 -> W개 ack 시 응답 -> 나머지는 백그라운드), 이번엔
로컬 함수 호출 대신 httpx로 진짜 네트워크 요청을 보낸다.

읽기 시 read repair, 힌트 큐(hinted handoff), 헬스 모니터는 시뮬레이션 버전
(quorum_store 패키지)에 이미 구현되어 있으므로 여기서는 핵심 쿼럼 라우팅만
네트워크 버전으로 옮겨 동작을 보여준다.

환경 변수:
  NODE_URLS      콤마로 구분된 노드 URL 목록. 첫 번째가 주 노드.
                 예) http://localhost:8001,http://localhost:8002,http://localhost:8003
  WRITE_QUORUM   기본 2
  READ_QUORUM    기본 2

실행 예:
  NODE_URLS=http://localhost:8001,http://localhost:8002,http://localhost:8003 \
      uvicorn network.coordinator_server:app --port 9000
"""

from __future__ import annotations

import asyncio
import itertools
import os
import time

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

NODE_URLS = os.environ.get(
    "NODE_URLS", "http://localhost:8001,http://localhost:8002,http://localhost:8003"
).split(",")
WRITE_QUORUM = int(os.environ.get("WRITE_QUORUM", "2"))
READ_QUORUM = int(os.environ.get("READ_QUORUM", "2"))

PRIMARY_URL = NODE_URLS[0]
SECONDARY_URLS = NODE_URLS[1:]

app = FastAPI(title="quorum-store coordinator")
_version_counter = itertools.count(1)


class WriteRequest(BaseModel):
    key: str
    value: str


async def _node_write(client: httpx.AsyncClient, url: str, key: str, value: str, version: int):
    resp = await client.post(
        f"{url}/write", json={"key": key, "value": value, "version": version}, timeout=5.0
    )
    resp.raise_for_status()
    return resp.json()


async def _node_read(client: httpx.AsyncClient, url: str, key: str):
    resp = await client.get(f"{url}/read/{key}", timeout=5.0)
    resp.raise_for_status()
    return resp.json()


@app.post("/write")
async def write(req: WriteRequest):
    version = next(_version_counter)
    start = time.monotonic()

    # trust_env=False: 노드들은 항상 로컬/사설 네트워크 주소이므로 시스템 프록시 설정은 무시한다.
    async with httpx.AsyncClient(trust_env=False) as client:
        # 1) 주 노드는 동기적으로 먼저 저장.
        primary_result = await _node_write(client, PRIMARY_URL, req.key, req.value, version)
        acked = [primary_result["node_id"]]

        # 2) 부 노드로 병렬 복제 요청.
        tasks = {
            asyncio.create_task(_node_write(client, url, req.key, req.value, version)): url
            for url in SECONDARY_URLS
        }

        # 3) W개를 채울 때까지만 기다린다.
        while len(acked) < WRITE_QUORUM and tasks:
            done, _ = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                url = tasks.pop(t)
                try:
                    result = t.result()
                    acked.append(result["node_id"])
                except Exception as e:
                    print(f"  [경고] {url} 복제 실패: {e}")

        elapsed = time.monotonic() - start

        # 4) 나머지는 백그라운드로 넘긴다.
        if tasks:
            asyncio.create_task(_finish_background(tasks, req.key, version))

    return {
        "key": req.key,
        "version": version,
        "acked_count": len(acked),
        "quorum": WRITE_QUORUM,
        "total_nodes": len(NODE_URLS),
        "elapsed_ms": elapsed * 1000,
        "success": len(acked) >= WRITE_QUORUM,
        "acked_nodes": acked,
    }


async def _finish_background(tasks: dict, key: str, version: int):
    results = await asyncio.gather(*tasks.keys(), return_exceptions=True)
    for url, result in zip(tasks.values(), results):
        if isinstance(result, Exception):
            print(f"  [백그라운드] {url} 복제 실패: {result}")
        else:
            print(f"  [백그라운드] {url} 복제 완료 (v{version})")


@app.get("/read/{key:path}")
async def read(key: str):
    start = time.monotonic()

    # trust_env=False: 노드들은 항상 로컬/사설 네트워크 주소이므로 시스템 프록시 설정은 무시한다.
    async with httpx.AsyncClient(trust_env=False) as client:
        tasks = {asyncio.create_task(_node_read(client, url, key)): url for url in NODE_URLS}
        replies = []

        while len(replies) < READ_QUORUM and tasks:
            done, _ = await asyncio.wait(tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                url = tasks.pop(t)
                try:
                    replies.append(t.result())
                except Exception as e:
                    print(f"  [경고] {url} 읽기 실패: {e}")

        elapsed = time.monotonic() - start

        latest = None
        for r in replies:
            if r["value"] is not None and (latest is None or r["version"] > latest["version"]):
                latest = r

        if tasks:
            asyncio.create_task(_drain(tasks))

    return {
        "key": key,
        "value": latest["value"] if latest else None,
        "version": latest["version"] if latest else None,
        "replied_count": len(replies),
        "quorum": READ_QUORUM,
        "total_nodes": len(NODE_URLS),
        "elapsed_ms": elapsed * 1000,
        "success": len(replies) >= READ_QUORUM,
    }


async def _drain(tasks: dict):
    await asyncio.gather(*tasks.keys(), return_exceptions=True)
