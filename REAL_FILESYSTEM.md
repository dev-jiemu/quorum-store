# 토이 구현 vs 실제 파일시스템 — 읽기 쿼럼 버전 관리 비교

---

## 1. 이 구현의 구조 (토이)

각 노드가 버전과 값을 한 덩어리로 인메모리 dict에 보관한다.  
별도 DB나 매니페스트는 없고, 버전은 코디네이터가 단순 카운터로 직접 발급한다.

```
coordinator._version_counter = itertools.count(1)  # 1, 2, 3 …
```

### 노드 저장소

```python
# node.py
self.store: dict[str, VersionedValue] = {}   # 인메모리

# models.py
@dataclass
class VersionedValue:
    value: str    # 실제 값 (파일 내용 전체)
    version: int  # 단순 정수 버전
```

### 읽기 경로 (coordinator.py:107-154)

```
클라이언트
    │
    ▼
코디네이터 ──────────────────────────────────────────────────────┐
    │  read(key) 동시 요청                                        │
    ├──▶ Node-A.read(key) → ReadReply(value="v2", version=2)     │
    ├──▶ Node-B.read(key) → ReadReply(value="v2", version=2)     │
    └──▶ Node-C.read(key) → ReadReply(value="v1", version=1) ←──┘
                                  │
                    R개 모이면 버전 비교
                    → version=2 채택 (최신)
                    → "v2" 그대로 반환 ✅
                    → Node-C는 백그라운드 read repair
```

**핵심**: 노드가 응답할 때 **버전 + 값이 함께** 오기 때문에 별도 조회 없이 즉시 반환 가능.

---

## 2. 실제 파일시스템이라면 — 매니페스트 패턴

실제 오브젝트 스토리지(S3, GFS, Ceph 등)에서는 **메타데이터(버전 정보)** 와 **실제 파일 데이터** 를 분리해서 저장한다.

### 저장 구조

```
┌─────────────────────────────────┐    ┌──────────────────────────────┐
│        메타데이터 DB             │    │       데이터 스토어           │
│  (etcd / DynamoDB / Cassandra)  │    │    (디스크 / 오브젝트 스토어) │
│                                 │    │                              │
│  key      │ version │ content_  │    │  sha256 hash → 실제 바이트   │
│           │         │ hash      │    │                              │
│  "foo.txt"│    3    │ "a1b2c3…" │───▶│  a1b2c3… → [파일 바이트]    │
│  "bar.png"│    1    │ "d4e5f6…" │───▶│  d4e5f6… → [파일 바이트]    │
└─────────────────────────────────┘    └──────────────────────────────┘
```

- 메타 DB: 파일명(key), 버전, content hash, 크기, 타임스탬프 등 경량 정보만
- 데이터 스토어: content hash를 파일명으로 쓰는 **content-addressable storage**
  - 같은 내용이면 같은 hash → 중복 저장 없음
  - hash가 곧 무결성 검증 수단

### 실제 읽기 경로 (2단계)

```
클라이언트
    │
    ▼
코디네이터
    │
    │ ① 메타데이터 쿼럼 (경량)
    ├──▶ MetaNode-A → { version: 3, hash: "a1b2c3" }
    ├──▶ MetaNode-B → { version: 3, hash: "a1b2c3" }
    └──▶ MetaNode-C → { version: 2, hash: "old111" }
                │
                │ R개 모이면 → version=3 채택 → hash="a1b2c3"
                │
    │ ② 데이터 fetch (단일 조회, hash로 특정)
    └──▶ DataStore.get("a1b2c3") → [파일 바이트]
                │
                ▼
           클라이언트에 파일 반환 ✅
```

### 실제 쓰기 경로

```
클라이언트 (파일 업로드)
    │
    ▼
코디네이터
    │
    │ ① 파일 데이터 저장 (content-addressable)
    └──▶ DataStore.put(bytes) → hash = sha256(bytes) = "a1b2c3"
    │
    │ ② 메타데이터 쿼럼 쓰기
    ├──▶ MetaNode-A.write({ key, version: 3, hash: "a1b2c3" }) → ack
    ├──▶ MetaNode-B.write({ key, version: 3, hash: "a1b2c3" }) → ack
    └──▶ MetaNode-C.write(…) → (백그라운드)
                │
                │ W개 ack → 클라이언트에 성공 응답
```

### 실제 버전 발급 방식

토이 구현처럼 코디네이터 단일 카운터를 쓰면 코디네이터가 SPOF(단일 장애점)가 된다.  
실제 시스템은 아래 중 하나를 쓴다:

| 방식 | 설명 | 사용처 |
|------|------|--------|
| **Lamport Clock** | 이벤트 순서를 논리 시간으로 표현, 단조증가 | 분산 시스템 일반 |
| **Vector Clock** | 노드별 카운터 벡터, 동시 쓰기 감지 가능 | Amazon Dynamo |
| **Fencing Token (ZooKeeper)** | ZK 시퀀스 노드로 전역 순번 발급 | 강한 일관성이 필요한 경우 |
| **조건부 쓰기 (CAS)** | DB의 `version = old_version + 1` 원자적 갱신 | DynamoDB, Postgres |

---

## 3. 핵심 차이 요약

| 항목 | 이 구현 (토이) | 실제 파일시스템 |
|------|--------------|----------------|
| **버전 저장 위치** | 값과 함께 노드 인메모리 dict | 별도 메타데이터 DB |
| **파일 데이터** | `value: str` 필드 (값 = 데이터 전체) | content-addressable 데이터 스토어 |
| **읽기 단계** | 1단계 (버전+값 한번에) | 2단계 (메타쿼럼 → 데이터 fetch) |
| **버전 발급** | 코디네이터 단일 카운터 | Lamport Clock / ZK 토큰 / CAS |
| **중복 처리** | 없음 | 동일 hash → 동일 파일 (중복 저장 X) |
| **무결성 검증** | 없음 | content hash로 자동 검증 |
| **파일 크기** | 제한 없지만 메모리에 다 올라감 | 대용량 파일도 스트리밍 처리 가능 |

---

## 4. 이 토이 구현이 2단계로 분리하지 않은 이유

- **학습 목적**: 쿼럼 로직(W, R, N 관계 / read repair / hinted handoff) 자체에 집중
- `value: str` 을 파일 경로나 hash로 바꾸기만 해도 실제 파일시스템으로 확장 가능
- 실제로 `network/node_server.py` 단계에서 HTTP로 분리한 것처럼, 다음 단계는 메타 DB와 데이터 스토어를 분리하는 것이 자연스러운 확장 방향

```
# 확장한다면 node.py의 store를 이렇게 바꾸면 됨
self.meta_db: dict[str, ManifestEntry] = {}   # 버전 + hash
self.data_store: dict[str, bytes] = {}         # hash → 실제 파일 바이트
```
