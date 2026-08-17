"""실제 네트워크로 동작하는 데이터 노드 서버 (FastAPI).

지금까지의 quorum_store 패키지는 한 프로세스 안에서 클래스 호출로 노드를
흉내냈다면, 이 파일은 노드 하나를 진짜 독립된 HTTP 서버로 띄운 것이다.
내부 저장 로직은 quorum_store.node.DataNode를 그대로 재사용한다.

환경 변수:
  NODE_ID        (필수) 노드 식별자
  FAILURE_RATE   (선택, 기본 0.0) 인위적 장애 확률 - 테스트용
  BASE_LATENCY   (선택, 기본 0.0) 인위적 지연(초) - 테스트용
  JITTER         (선택, 기본 0.0)

실행 예:
  NODE_ID=primary uvicorn network.node_server:app --port 8001
"""

from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from quorum_store.node import DataNode, NodeFailure

NODE_ID = os.environ.get("NODE_ID", "node")
FAILURE_RATE = float(os.environ.get("FAILURE_RATE", "0.0"))
BASE_LATENCY = float(os.environ.get("BASE_LATENCY", "0.0"))
JITTER = float(os.environ.get("JITTER", "0.0"))

node = DataNode(NODE_ID, base_latency=BASE_LATENCY, jitter=JITTER, failure_rate=FAILURE_RATE)
app = FastAPI(title=f"quorum-store node ({NODE_ID})")


class WriteRequest(BaseModel):
    key: str
    value: str
    version: int


@app.post("/write")
async def write(req: WriteRequest):
    try:
        ack = await node.write(req.key, req.value, req.version)
    except NodeFailure as e:
        raise HTTPException(status_code=503, detail=str(e))
    return {"node_id": ack.node_id, "version": ack.version, "latency_ms": ack.latency * 1000}


@app.get("/read/{key:path}")
async def read(key: str):
    try:
        reply = await node.read(key)
    except NodeFailure as e:
        raise HTTPException(status_code=503, detail=str(e))
    if reply.value is None:
        return {"node_id": reply.node_id, "value": None, "version": None}
    return {"node_id": reply.node_id, "value": reply.value.value, "version": reply.value.version}


@app.get("/ping")
async def ping():
    ok = await node.ping()
    if not ok:
        raise HTTPException(status_code=503, detail=f"{NODE_ID} unreachable")
    return {"node_id": NODE_ID, "alive": True}
