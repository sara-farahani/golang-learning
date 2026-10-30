# GoPulse: High-Throughput Concurrent Stream Engine

> **In Plain English:** GoPulse is a **high-speed background task processing service** written in Go.
> Think of it like a smart factory assembly line: when thousands of tasks (like user signups, payment webhooks, or log events) arrive every second, GoPulse buffers them, processes them across multiple worker threads in parallel without crashing the server, safely quarantines errors, and tracks live throughput.

> **🎓 Project Scope:** GoPulse is an **educational / practice project** built to demonstrate real-world Go concurrency patterns — token-bucket rate limiting, bounded worker pools, dead-letter queues with backoff retry, lock-free telemetry, and graceful shutdown — end-to-end with Docker deployment. It intentionally uses only the Go standard library and keeps state in memory, so it is **not production-ready as-is** — treated as a solid foundation to extend toward one.

---

## ✨ Features

### Core Engine
- ⚡ **Token-Bucket Rate Limiter** — burstable, non-blocking (`Allow()`) or blocking (`Wait()`) admission control with configurable refill rate & burst capacity.
- 🧵 **Bounded Worker Pool** — fixed pool of concurrent goroutines pulling jobs from a buffered channel (no unbounded thread spawning).
- 🔀 **Fan-Out / Fan-In Pipeline** — multi-stage multiplexing pattern built in.
- ☠️ **Dead-Letter Queue (DLQ)** — thread-safe quarantine for poison-pill jobs with automatic exponential-backoff retry.
- 📊 **Lock-Free Telemetry** — `sync/atomic` counters for processed / failed / DLQ / throughput / avg-latency with zero mutex contention.
- 🛑 **Graceful Shutdown** — traps `SIGINT` / `SIGTERM`, drains in-flight jobs, exits with **zero data loss** and no goroutine leaks.

### Built-in Web Dashboard (no frontend framework needed)
- 🌐 **Live control UI** at `http://localhost:8080` — served by a tiny embedded `net/http` server.
- 📈 **Real-time stats** — processed, failed, DLQ count, active workers, avg latency (auto-refreshed).
- 🚀 **Interactive controls** — enqueue single jobs, trigger 10-job bursts, inject poison pills into the DLQ from the browser.
- 📜 **Live activity log** — streaming per-job execution log inside the page.

### REST API
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Web dashboard UI |
| `/api/metrics` | GET | JSON snapshot of engine telemetry |
| `/api/enqueue?type=normal` | GET | Enqueue a normal job (runs through the rate limiter) |
| `/api/enqueue?type=poison` | GET | Inject a poison-pill job → lands in the DLQ |

### Containerization
- 🐳 **Multi-stage Dockerfile** — compiles a static, stripped binary (`-trimpath -ldflags="-s -w"`), runs on a minimal **Alpine 3.20** image (**~20 MB total**).
- 👤 **Non-root container** — runs as a dedicated unprivileged user.
- 🧩 **Docker Compose** — one command (`docker compose up`) to build, run, and expose the dashboard & API; `SIGTERM` → graceful shutdown wired end-to-end.
- 🪶 **Zero external dependencies** — pure Go standard library (`context`, `sync`, `sync/atomic`, `time`, `os/signal`, `net/http`).

---

## What Does This Project Do?

Imagine you run an online service (like Stripe, Uber, or an IoT system):
1. **The Influx:** 1,000 tasks arrive per second (payments to verify, emails to send, or telemetry packets to parse).
2. **The Risk:** If you create an unconstrained thread for each task, your computer runs out of RAM and crashes (an Out-Of-Memory panic). If you process them one-by-one, your system slows to a crawl.
3. **The Solution (GoPulse):**
   - **Step 1 (The Gatekeeper):** A **Token-Bucket Rate Limiter** regulates how fast tasks enter.
   - **Step 2 (The Conveyor Belt):** Tasks enter a bounded Go channel (an in-memory queue).
   - **Step 3 (The Workers):** A fixed team of **4 background worker goroutines** pull tasks off the conveyor belt and execute them simultaneously.
   - **Step 4 (The Quarantine Bin):** If a task has bad data (a "poison pill"), it doesn't crash the server. It is moved to a **Dead-Letter Queue (DLQ)** with automatic retry backoff.
   - **Step 5 (The Live Speedometer):** A **Lock-Free Atomic Counter** tracks how many tasks finish per second directly in CPU hardware without locking.
   - **Step 6 (Polite Teardown):** When you press `Ctrl+C`, it stops taking new work and finishes whatever is in flight before shutting down with zero lost data.

---

## What You See When You Run It

When you execute `go run cmd/gopulse/main.go`, you see the concurrent engine in action:

```text
🚀 Initializing GoPulse Stream Engine...
🌐 Interactive Web UI Dashboard available at: http://localhost:8080 (Open in your browser!)
✅ [Worker 1] Finished Job job-001 in 80ms: Successfully processed [job-001] priority=MEDIUM
✅ [Worker 2] Finished Job job-002 in 80ms: Successfully processed [job-002] priority=MEDIUM
✅ [Worker 3] Finished Job job-003 in 80ms: Successfully processed [job-003] priority=MEDIUM
✅ [Worker 4] Finished Job job-004 in 80ms: Successfully processed [job-004] priority=MEDIUM
✅ [Worker 1] Finished Job job-005 in 80ms: Successfully processed [job-005] priority=MEDIUM
...
🛑 Shutdown signal received! Commencing graceful teardown...
✨ All workers exited cleanly. Zero jobs dropped.
📊 Final Telemetry: Processed=20 Failed=0 DLQ=0 AvgLatency=80.12ms
```

---

## Technical Highlights & Go Concurrency Patterns
- **Go Foundations**: Custom structs, enums via `iota`, pointer receivers, and modern sentinel error wrapping (`errors.Is`).
- **CSP Channels & Goroutines**: Bounded worker pools, unbuffered vs buffered channels, directional channels (`chan<-`, `<-`).
- **Pipelines**: Fan-Out and Fan-In multiplexing with worker pools.
- **Resilience**: Token-bucket rate limiting, Context timeouts (`context.WithTimeout`), and exponential backoff retry with Dead-Letter Queues (DLQ).
- **Lock-Free Telemetry**: High-frequency metrics counters with hardware atomic operations (`sync/atomic`).
- **Graceful Shutdown**: Trapping OS signals (`SIGINT`, `SIGTERM`) via `signal.NotifyContext`, closing work queues, and draining in-flight goroutines with `sync.WaitGroup`.
- **Testing & Verification**: 100% thread safety verified with `go test -race ./...`.

---

## Project Structure

```text
gopulse/
├── cmd/
│   └── gopulse/
│       └── main.go                      # Application entry point, pipeline bootstrapper & signal trap
├── internal/
│   ├── domain/
│   │   ├── job.go                       # Core domain models, state enums & validation
│   │   └── job_test.go                  # Table-driven unit tests & error validation
│   ├── pipeline/
│   │   ├── worker_pool.go               # Bounded worker pool & task dispatcher
│   │   ├── fanout_fanin.go              # Multi-stage fan-out/fan-in pipeline
│   │   └── worker_test.go               # Concurrent pool tests & parallel benchmarks
│   ├── ratelimit/
│   │   ├── token_bucket.go              # Burstable token-bucket rate limiter
│   │   └── token_bucket_test.go         # Rate limiter burst & concurrency tests
│   ├── dlq/
│   │   ├── dead_letter_queue.go         # Thread-safe DLQ with backoff retry
│   │   └── dead_letter_queue_test.go    # DLQ stress testing with 50 concurrent goroutines
│   └── metrics/
│       ├── atomic_counter.go            # Lock-free atomic throughput & latency telemetry
│       └── atomic_counter_test.go       # Parallel atomic stress tests (100 goroutines)
├── gopulse_test.go                      # Root package end-to-end integration test
├── go.mod                               # Go 1.23 standard library module
├── Dockerfile                           # Multi-stage build → ~20 MB Alpine runtime, non-root user
├── docker-compose.yml                   # One-command deploy: maps dashboard & API on :8080
├── .dockerignore                        # Keeps tests/docs out of the image build context
├── Makefile                             # Convenient dev, test, and race targets
└── README.md
```

---

## Prerequisites

- Option A (recommended): [Docker](https://docs.docker.com/get-docker/) with Compose v2.
- Option B: [Go 1.22+](https://go.dev/dl/) installed.

---

## Quick Start & Running Tests

### 🐳 Option A: Run with Docker Compose (recommended)

```bash
# Build & start (detached) — dashboard and API become available on :8080
docker compose up -d --build

# Watch the live telemetry in the container logs
docker compose logs -f

# Open the interactive Web UI dashboard in your browser
open http://localhost:8080        # macOS  (Linux: xdg-open http://localhost:8080)

# Or drive the API directly
curl http://localhost:8080/api/metrics              # live engine stats (JSON)
curl "http://localhost:8080/api/enqueue?type=normal" # enqueue a normal job
curl "http://localhost:8080/api/enqueue?type=poison" # inject a poison pill → DLQ

# Stop gracefully (SIGTERM → drains in-flight jobs, zero data loss)
docker compose down
```

### 🛠️ Option B: Run natively with Go

### 1. Run All Tests Across All Packages
```bash
go test -v ./...
```
*(Every single package contains full test suites: root, domain, ratelimit, dlq, metrics, and pipeline!)*

### 2. Run Race Detector (Thread Sanitizer)
```bash
go test -v -race ./...
```
*(Guarantees 100% thread safety with zero data races across all concurrent components!)*

### 3. Run Benchmark Tests
```bash
go test -v -bench=. -benchmem ./...
```

### 4. Run the Engine with Live Terminal Telemetry
```bash
go run cmd/gopulse/main.go
```
*(The embedded Web UI dashboard starts automatically at http://localhost:8080 — open it in your browser!)*

---

## Architectural Highlights

1. **Zero External Dependencies**:
   Constructed entirely using Go's standard library (`context`, `sync`, `sync/atomic`, `time`, `os/signal`).
2. **Deterministic Termination**:
   Every goroutine lifecycle is tied to a `context.Context` and tracked in a `sync.WaitGroup`. No goroutine leaks.
3. **Thread Safety**:
   100% clean under `go test -race ./...`.
