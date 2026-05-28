# Mini-Kafka (Distributed Message Queue)

![Build Status](https://github.com/san4b0t/mini-kafka/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go](https://img.shields.io/badge/Go-1.22+-00ADD8.svg?logo=go&logoColor=white)

A production-standard, multi-threaded, publish-subscribe message broker written entirely in Go, utilizing the standard library.

## Architecture & Design

- **Storage**: Append-only log. Messages are written to `.dat` files sequentially. An index file (`.idx`) maps message offsets to byte positions for lock-free sequential reads.
- **Concurrency**: `sync.RWMutex` serializes writes to disk, while `os.File.ReadAt` allows fully concurrent lock-free message polling. Consumer group offsets are atomically managed in a thread-safe map. Validated through 50-goroutine stress tests with 5,000+ concurrent messages in 28.39s.
- **Distributed Topology**: Brokers run as a cluster. A deterministic hashing algorithm routes requests to the authoritative broker for a topic, using a transparent internal HTTP reverse proxy.

## How to run locally

```bash
make build
make run
```

## How to run with Docker Compose (3 Node Cluster)

```bash
make docker-up
```

## Running Tests

#### Tests include aggressive multi-threaded stress tests testing the lock safety of the file backend.

```bash
make test
```

## API Examples

#### Create topic

```bash
curl -X POST http://localhost:8081/topics -H "Content-Type: application/json" -d '{"name": "logs"}'
```

#### Publish message

```bash
curl -X POST http://localhost:8081/topics/logs/messages -H "Content-Type: application/octet-stream" -d 'Hello Distributed World!'
```

#### Consume message

```bash
curl -X GET "http://localhost:8081/topics/logs/messages?offset=0"
```

#### Commit offset for consumer group

```bash
curl -X POST http://localhost:8081/topics/logs/groups/analytics-service/commit -H "Content-Type: application/json" -d '{"offset": 1}'
```

#### Metrics

```bash
curl http://localhost:8081/metrics
```

---

### 6. API SPECIFICATION

| Endpoint                        | Method | Body                 | Response                            | Status Codes                            |
| :------------------------------ | :----- | :------------------- | :---------------------------------- | :-------------------------------------- |
| `/topics`                       | `POST` | `{"name":"string"}`  | `{"message":"...", "topic":"..."}`  | 201 Created, 400 Bad Request, 500 Error |
| `/topics/{t}/messages`          | `POST` | Raw Bytes            | `{"offset": uint64}`                | 201 Created, 400 Bad Request, 500 Error |
| `/topics/{t}/messages?offset=X` | `GET`  | None                 | Raw Bytes                           | 200 OK, 404 Not Found, 500 Error        |
| `/topics/{t}/groups/{g}/commit` | `POST` | `{"offset": uint64}` | `{"message":"..."}`                 | 200 OK, 400 Bad Request, 500 Error      |
| `/topics/{t}/groups/{g}`        | `GET`  | None                 | `{"offset": uint64}`                | 200 OK                                  |
| `/health`                       | `GET`  | None                 | `{"status":"ok", "broker_id": "X"}` | 200 OK                                  |
| `/metrics`                      | `GET`  | None                 | `{"messages_published": X, ...}`    | 200 OK                                  |

---

### 7. CONCURRENCY AND THREAD SAFETY

In a naive implementation, if thousands of producers try to write to a log simultaneously, OS-level writes interleave, corrupting the message layout. If consumers read simultaneously while an append resizes the internal byte slice, a panic (index out of bounds) occurs (a classic Data Race).

**Synchronization Primitives Utilized:**

1.  **`sync.RWMutex` (Storage Layer):** Inside `CommitLog`, `mu.Lock()` ensures only one producer can append to the disk and update the `.idx` offset map at a given microsecond. This resolves the write-write race.
2.  **Lock-Free Disk Reads:** The consumer `Read()` calls `mu.RLock()` _only_ for the microseconds needed to fetch the integer position from the index slice. Crucially, the actual disk read (`dataFile.ReadAt`) happens **outside the lock**. Unix file descriptors support concurrent `pread`, meaning hundreds of consumers can read different parts of the `.dat` file in parallel without blocking producers or each other.
3.  **`sync.RWMutex` (Offset Layer):** Committing offsets writes to an in-memory `map`. Maps in Go are explicitly not thread-safe. `OffsetManager` uses an explicit lock around map mutations.
4.  **`sync/atomic` (Metrics Layer):** Counters are updated using `atomic.AddUint64` to avoid locking overhead entirely for observability counters.

**Deadlock Avoidance:**
Locks are highly localized. A lock is never held while making a network call, preventing distributed deadlocks. The `CommitLog` lock and `OffsetManager` lock are never acquired simultaneously.

---

### 8. DISTRIBUTED / MULTI-BROKER SETUP

The Docker Compose configuration spins up three independent containers (`broker1`, `broker2`, `broker3`) interconnected via an internal Docker bridge network (`kafka-net`).

**Routing Strategy (Topic Ownership):**
Because replicating distributed logs requires Raft/Paxos (which is out of scope for a single sprint project), we utilize **Partition Ownership via Consistent Hashing**.

1. When any broker receives a request for a topic (e.g., `logs`), it hashes the topic string: `hash("logs") % 3`.
2. Let's say the hash output means Node 2 is the owner.
3. If Node 1 receives the POST request from a client, Node 1 checks the hash, sees Node 2 is the owner, and uses Go's native `httputil.ReverseProxy` to transparently stream the HTTP request to Node 2 over the Docker network.
4. The client receives a seamless response, completely unaware of internal proxying.

---

### 9. TESTING
The `stress_test.go` spins up `50` concurrent goroutines, each rapidly publishing `100` messages, mimicking a thundering herd. It validates thread-safety by verifying zero dropped messages, zero corrupted payloads, and proper index incrementation.

---

### 10. CI/CD AUTOMATION

The GitHub Actions (`ci.yml`) runs `go test -race ./...`. Go's Race Detector dynamically instruments memory accesses and will fail the build immediately if two goroutines access the same memory location without synchronization.

---

### FINAL COMMANDS

**Run locally:**

```bash
go mod tidy
make run
```

**Run tests:**

```bash
make test
```

**Run Docker Compose (3-Node Cluster):**

```bash
make docker-up
```

**Example API Usage Sequence (Terminal 1 running make run, Terminal 2 for commands):**

```bash
# 1. Create a topic
curl -X POST http://localhost:8081/topics -H "Content-Type: application/json" -d '{"name":"events"}'

# 2. Publish 3 messages
curl -X POST http://localhost:8081/topics/events/messages -d 'Event 1'
curl -X POST http://localhost:8081/topics/events/messages -d 'Event 2'
curl -X POST http://localhost:8081/topics/events/messages -d 'Event 3'

# 3. Consumer pulls message 0
curl "http://localhost:8081/topics/events/messages?offset=0"
# (Output: Event 1)

# 4. Consumer commits offset 1 (acknowledging message 0 is processed)
curl -X POST http://localhost:8081/topics/events/groups/my-consumer/commit -H "Content-Type: application/json" -d '{"offset": 1}'

# 5. Check cluster metrics
curl http://localhost:8081/metrics
```
