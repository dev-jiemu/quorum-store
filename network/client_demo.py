"""coordinator_server가 떠 있다고 가정하고 실제 HTTP로 쓰기/읽기를 호출하는 예시.

먼저 아래처럼 노드 서버 3개 + 코디네이터 서버 1개를 각각 다른 터미널에서 띄운다.

  NODE_ID=primary     uvicorn network.node_server:app --port 8001
  NODE_ID=secondary-1 uvicorn network.node_server:app --port 8002
  NODE_ID=secondary-2 uvicorn network.node_server:app --port 8003

  NODE_URLS=http://localhost:8001,http://localhost:8002,http://localhost:8003 \
      uvicorn network.coordinator_server:app --port 9000

그 다음 이 스크립트를 실행한다.

  python3 network/client_demo.py
"""

import httpx

COORDINATOR_URL = "http://localhost:9000"


def main():
    # trust_env=False: 로컬호스트로만 통신하므로 시스템 프록시 설정(HTTP_PROXY 등)은 무시한다.
    with httpx.Client(trust_env=False, timeout=5.0) as client:
        write_resp = client.post(
            f"{COORDINATOR_URL}/write",
            json={"key": "file/hello.txt", "value": "hello over http"},
        )
        print("write:", write_resp.json())

        read_resp = client.get(f"{COORDINATOR_URL}/read/file/hello.txt")
        print("read:", read_resp.json())


if __name__ == "__main__":
    main()
