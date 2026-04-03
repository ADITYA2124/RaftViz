# RaftViz

RaftViz is a Spring Boot Raft consensus demo that shows leader election, log replication, dynamic node discovery, and a live dashboard. Nodes discover each other automatically, so a new system can join the network and appear on the dashboard without being manually added to a URL or properties file.

## Features

- Dynamic cluster discovery using multicast announcements
- Raft leader election and log replication
- Dashboard with live node connectivity and role changes
- Per-node log inspection with a dedicated endpoint
- Swagger UI for exploring and testing the API
- Bash demo script that starts a cluster and simulates a late-joining node

## Requirements

- Java 21
- Bash
- `curl`
- Network support for multicast on the machines that should auto-discover each other

## Build And Start

1. Open a terminal in the project root.
2. Build the project:

```bash
./mvnw clean package
```

3. Start a node:

```bash
PORT=8080 \
NODE_ID=node-1 \
ADVERTISE_ADDR=http://127.0.0.1:8080 \
./mvnw spring-boot:run
```

4. Open the dashboard:

```text
http://127.0.0.1:8080/dashboard.html
```

5. Open Swagger UI:

```text
http://127.0.0.1:8080/swagger-ui/index.html
```

## Start Multiple Nodes Manually

Start each node in a separate terminal with a unique `PORT`, `NODE_ID`, and `ADVERTISE_ADDR`.

Node 1:

```bash
PORT=8080 NODE_ID=node-1 ADVERTISE_ADDR=http://127.0.0.1:8080 ./mvnw spring-boot:run
```

Node 2:

```bash
PORT=8081 NODE_ID=node-2 ADVERTISE_ADDR=http://127.0.0.1:8081 ./mvnw spring-boot:run
```

Node 3:

```bash
PORT=8082 NODE_ID=node-3 ADVERTISE_ADDR=http://127.0.0.1:8082 ./mvnw spring-boot:run
```

Open the dashboard on any running node. The other nodes should appear automatically after discovery.

## Demo Script

The project includes a bash script that simulates a Raft environment for efficient log storage and dynamic UI updates.

Run it with:

```bash
bash scripts/demo-raft.sh
```

What it does:

1. Builds the jar if needed.
2. Starts `node-1`, `node-2`, and `node-3`.
3. Waits for election and discovery.
4. Appends sample log entries through the cluster.
5. Starts `node-4` later to demonstrate dynamic join behavior.
6. Appends more entries so you can compare logs across nodes.

Stop the demo with `Ctrl+C`.

Runtime logs from the demo are written to:

```text
demo-runtime/
```

## API Usage

### Client Write

Append a command to the replicated log:

```bash
curl -L -X POST http://127.0.0.1:8080/log \
  -H "Content-Type: application/json" \
  -d '{"message":"create order ledger"}'
```

You can send the write to any node. If that node is not the leader, it redirects to the leader when known.

### Inspect Cluster Members

```bash
curl http://127.0.0.1:8080/raft/cluster
```

### Inspect One Node State

```bash
curl http://127.0.0.1:8080/raft/state
```

### Inspect Local Logs On A Node

```bash
curl http://127.0.0.1:8080/raft/logs
```

### Inspect Logs For A Specific Node

```bash
curl http://127.0.0.1:8080/raft/nodes/node-2/logs
```

## Swagger

Swagger UI is available after startup at:

```text
/swagger-ui/index.html
```

OpenAPI JSON is available at:

```text
/v3/api-docs
```

## Discovery Configuration

These optional environment variables control dynamic discovery:

- `RAFT_DISCOVERY_GROUP`
- `RAFT_DISCOVERY_PORT`
- `RAFT_DISCOVERY_ANNOUNCE_INTERVAL_MS`
- `RAFT_DISCOVERY_STALE_AFTER_MS`
- `RAFT_DISCOVERY_EVICT_AFTER_MS`

Defaults are defined in [application.yaml](C:\Users\Jai\Desktop\Raftviz\src\main\resources\application.yaml).

## Notes

- Use a reachable `ADVERTISE_ADDR` for each machine so other nodes can call it.
- For multi-machine demos, ensure multicast traffic is allowed on the network.
- The dashboard is discovery-driven now, so no `?nodes=` query parameter is required.
