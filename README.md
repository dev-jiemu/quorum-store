# quorum-store
오브젝트 스토리지 따라해보기 : 쿼럼 기반 분산 스토리지 라우팅 로직 토이 구현체 <br/>
🤔 책 읽다가 주/부 데이터 노드 구조에 따라 어떻게 파일을 저장하고 사용자에게 응답하는지 코드로 구현해보고 싶어짐

---

### 1. 코드 흐름 : 쿼럼방식
DataNode
- 주, 부 데이터 노드의 역할을 담당. 확률적으로 네트워크 지연/장애 일으킬거임

RoutingCoordinator
- 클라이언트 요청을 받았을때 주 노드에 먼저 파일을 저장하고 부 노드로 병렬복제 보낸뒤에 N개 중 W개의 ack 가 모이면 응답을 보냄


책을 잃다보니까 정확도와 응답속도 둘의 중점을 어디에 두냐에 따라 장/단점을 설명하던데, Claude 의 주장에 의하면 실무에선 그 중간인 쿼럼 방식을 사용한다고 함 <br/>
`Ref. Amazon Dynamo`
```
전체 복제본 수를 N, 쓰기 시 응답을 기다리는 복제본 수를 W라고 하면
N=3이면 W=2 정도로 설정해서 과반수 응답만 받으면 클라이언트에 응답하는 방식

* R+W > N 공식을 만족시키면 읽기 쿼럼과 쓰기 쿼럼이 항상 겹치기 때문에, 전체 노드를 기다리지 않아도 최신 데이터를 읽을 수 있다는 정합성 보장이 됨
```

---

### 2. 프로젝트 구조

```
quorum_store/              # 핵심 로직 (단일 프로세스 asyncio 시뮬레이션)
  models.py                 # WriteAck, WriteResult, ReadReply, ReadResult 등 공용 모델
  node.py                    # DataNode - 노드 하나(저장 + 지연/장애 시뮬레이션)
  coordinator.py              # RoutingCoordinator - 쓰기/읽기 쿼럼, read repair,
                              #  hinted handoff, 헬스 모니터

quorum_write_routing.py     # 1단계: 쓰기 쿼럼만 다루는 최초 버전 (W=1 / N / 쿼럼 비교 데모)
demo_read_write.py          # 2단계: 읽기 쿼럼 + read repair 데모
demo_retry_and_health.py    # 2단계: 노드 장애 -> hinted handoff 재시도 + 헬스 모니터 데모

network/                    # 3단계: 진짜 HTTP 서버로 옮긴 버전
  node_server.py             # 노드 하나 = FastAPI 서버 하나
  coordinator_server.py       # 코디네이터도 FastAPI 서버, httpx로 노드에 실제 요청
  client_demo.py              # 코디네이터 서버에 실제로 쓰기/읽기 요청 날리는 예시
```

### 3. 구현한 것

- **쓰기 쿼럼**: 주 노드 저장 → 부 노드 병렬 복제 → W개 ack 오면 응답, 나머지는 백그라운드 복제
- **읽기 쿼럼**: N개 중 R개 응답을 모아 가장 높은 버전(version)을 정답으로 채택 (Last-Write-Wins)
- **read repair**: 읽는 도중 뒤처진(낮은 버전/누락) 노드를 발견하면 백그라운드로 최신 값 갱신
- **hinted handoff**: 복제 실패한 쓰기를 큐에 쌓아뒀다가 별도 워커가 backoff 하며 재시도
- **헬스 모니터**: 주기적으로 ping 보내서 죽은 노드를 감지하고, 살아있는 노드로만 쿼럼 계산

### 4. 실행 방법

**시뮬레이션 버전** (네트워크 없이 한 프로세스 안에서 동작, 로직 이해용)
```bash
python3 quorum_write_routing.py       # 쓰기만: W=1 vs 쿼럼 vs 전체 ack 응답시간 비교
python3 demo_read_write.py            # 읽기 쿼럼 + read repair
python3 demo_retry_and_health.py      # 노드 장애 + hinted handoff + 헬스 모니터
```

**실제 네트워크 버전** (노드/코디네이터가 진짜 프로세스로 분리됨)
```bash
pip install -r requirements.txt

# 터미널 4개 (또는 백그라운드)에 각각
NODE_ID=primary     uvicorn network.node_server:app --port 8001
NODE_ID=secondary-1 uvicorn network.node_server:app --port 8002
NODE_ID=secondary-2 uvicorn network.node_server:app --port 8003
NODE_URLS=http://localhost:8001,http://localhost:8002,http://localhost:8003 \
    uvicorn network.coordinator_server:app --port 9000

# 그리고 나서
python3 network/client_demo.py
```

> network 버전은 핵심 쿼럼 라우팅(쓰기/읽기)만 옮긴 버전이라, read repair/hinted
> handoff/헬스 모니터는 아직 시뮬레이션(`quorum_store` 패키지) 쪽에만 있음. 다음에
> 이어서 옮길 예정.