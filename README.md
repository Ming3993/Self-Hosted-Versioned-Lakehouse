# Self-Hosted-Versioned-Lakehouse
Self-hosted Data Lakehouse with Trino (High-performance SQL) and Nessie (Git-like Data Versioning) on Iceberg. Built on a hybrid LXC (Proxmox/Incus) environment using Podman &amp; Systemd.

## 🏛️ Architectural Decisions

### 1. Why Daemonless Container Orchestration? (Podman + Systemd)

Instead of relying on a heavy central daemon like Docker, I utilized **Podman** with **Systemd** to manage service lifecycles.

* **Reliability:** Systemd automatically restarts critical services (Nessie, MinIO) upon failure.
* **Security:** Rootless containers reduce the attack surface.
* **Standardization:** Treats containers as native Linux services.

### 2. Why Apache Iceberg & Nessie?

Traditional Data Lakes suffer from the "Dirty Read" problem.

* **Iceberg** brings **ACID transactions** to the lake, ensuring data consistency.
* **Nessie** provides **Git-like semantics** (Branch, Commit, Tag, Rollback) for data, allowing safe experimentation and instant recovery from bad ETL jobs.

### 3. Why MinIO?

* Provides a high-performance, **S3-compatible** object storage layer, simulating a real-world cloud environment (AWS S3) strictly on-premise.

### 4. Why Separation of Compute & Storage?

* Allows independent scaling of the Query Layer (**Trino**) and Storage Layer (**MinIO**), optimizing resource usage within the LXC infrastructure.

### 5.Why Trino instead of Spark SQL for Querying?

While Spark is excellent for heavy ETL processes, Trino was chosen as the serving layer for the following reasons:

* **Low Latency for BI/Ad-hoc** Trino is a pure MPP (Massively Parallel Processing) engine designed for sub-second query responses, whereas Spark has overheads from task scheduling and container startup (better suited for long-running batch jobs).

* **Concurrency:** Trino handles multiple concurrent users (analysts, dashboards) much better than Spark Thrift Server.

* **Data Federation**: Trino acts as a unified access point, capable of joining data across Iceberg tables (MinIO) and operational databases (Postgres) in a single query if needed.