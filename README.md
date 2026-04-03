# RaftViz

RaftViz is a Spring Boot project for visualizing how the Raft consensus algorithm elects leaders, replicates logs, and keeps distributed state consistent. The project is designed to be useful both as a learning tool and as a practical demonstration of why efficient replicated log storage matters in distributed systems.

## Why This Project Is Important

Raft is one of the most approachable and widely taught consensus algorithms, but many demos stop at theory. RaftViz focuses on the parts people usually want to see in action:

- how a leader is elected
- how commands are appended to a replicated log
- how followers catch up after joining late
- how cluster membership and connectivity affect behavior
- how logs are stored consistently across nodes

This matters because the same ideas appear in real systems like configuration stores, orchestration platforms, replicated metadata services, and fault-tolerant coordination layers.

## What The Project Does

- runs a Raft-style cluster with leader election and log replication
- discovers nodes dynamically instead of requiring a hardcoded node list
- shows cluster state on a live dashboard with a ring topology
- exposes per-node log inspection endpoints
- supports both same-machine multi-port demos and multi-laptop LAN demos
- includes Swagger UI for exploring the API
- includes a bash demo script that starts a cluster and simulates a late-joining node

## How Dynamic Discovery Works

RaftViz now uses a hybrid discovery model so it works in the two most common demo environments.

### Local Machine, Multiple Ports

If you start multiple nodes on one laptop, the application automatically probes localhost ports in a configurable range and adds any Raft node it finds to the cluster view.

That means:

- you do not need to add `?nodes=...` to the dashboard
- you do not need to hardcode peers in properties
- starting a new node on another local port should make it appear automatically

### Multiple Laptops On The Same Network

If multiple laptops are on the same simple LAN, each node sends UDP broadcast discovery announcements and listens for announcements from others.

That means:

- each machine should use a reachable LAN IP in `ADVERTISE_ADDR`
- when one node sees another node’s broadcast, it adds it dynamically
- the dashboard should show nodes joining and leaving as network visibility changes

Important note:

- `127.0.0.1` or `localhost` only works for same-machine demos
- for multi-laptop usage, use something like `http://192.168.x.y:8080`

## Requirements

- Java 21
- Bash
- `curl`
- a local network that allows UDP broadcast for multi-laptop discovery

## Build

From the project root:

```bash
./mvnw clean package
```

On Windows PowerShell:

```powershell
.\mvnw.cmd clean package
```

## Start A Single Node

```bash
PORT=8080 \
NODE_ID=node-1 \
ADVERTISE_ADDR=http://127.0.0.1:8080 \
./mvnw spring-boot:run
```

Dashboard:

```text
http://127.0.0.1:8080/dashboard.html
```

Swagger UI:

```text
http://127.0.0.1:8080/swagger-ui/index.html
```

OpenAPI JSON:

```text
http://127.0.0.1:8080/v3/api-docs
```

## Run Multiple Nodes On One Laptop

Open separate terminals and start nodes with different ports.

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

How it behaves locally:

- the dashboard on any node will query `/raft/cluster`
- the discovery service probes localhost ports automatically
- each discovered node is added to the ring topology
- the leader is highlighted and follower logs can be inspected individually

If you want to extend the local scan range, use:

```bash
RAFT_DISCOVERY_LOCAL_SCAN_START_PORT=8080
RAFT_DISCOVERY_LOCAL_SCAN_END_PORT=8100
```

## Run Across Multiple Laptops On The Same Network

Each laptop should run one or more nodes, but the important part is that `ADVERTISE_ADDR` must be the machine’s LAN IP and chosen port.

Example on laptop A:

```bash
PORT=8080 \
NODE_ID=laptop-a-node-1 \
ADVERTISE_ADDR=http://192.168.1.10:8080 \
./mvnw spring-boot:run
```

Example on laptop B:

```bash
PORT=8080 \
NODE_ID=laptop-b-node-1 \
ADVERTISE_ADDR=http://192.168.1.11:8080 \
./mvnw spring-boot:run
```

How it behaves on a simple LAN:

- each node broadcasts its presence using UDP on the discovery port
- nodes listening on the same network can detect each other automatically
- once discovered, they exchange Raft traffic over HTTP using the advertised URI
- the dashboard on any node should reflect the shared network view

Checklist for LAN demos:

- all machines are on the same subnet
- firewalls allow the chosen HTTP port and UDP discovery port
- `ADVERTISE_ADDR` uses the real LAN IP, not localhost
- the discovery port matches on all machines

## Demo Script

The bash demo script starts a ready-made local cluster and demonstrates dynamic UI updates.

Run:

```bash
bash scripts/demo-raft.sh
```

What it does:

1. builds the jar if it does not exist
2. starts `node-1`, `node-2`, and `node-3`
3. waits for election and discovery
4. appends sample log entries
5. starts `node-4` later
6. appends more entries so follower catch-up can be observed

During the demo:

- open `http://127.0.0.1:8080/dashboard.html`
- watch the ring topology update when `node-4` joins
- click different nodes to compare logs

## API Usage

### Append A Client Command

```bash
curl -L -X POST http://127.0.0.1:8080/log \
  -H "Content-Type: application/json" \
  -d '{"message":"create order ledger"}'
```

Followers redirect writes to the leader when the leader is known.

### View Cluster Members

```bash
curl http://127.0.0.1:8080/raft/cluster
```

### View Current Node State

```bash
curl http://127.0.0.1:8080/raft/state
```

### View Local Logs On A Node

```bash
curl http://127.0.0.1:8080/raft/logs
```

### View Logs For One Specific Node

```bash
curl http://127.0.0.1:8080/raft/nodes/node-2/logs
```

## Simulation Endpoints

These endpoints are useful for demos when you want to force elections or manipulate a node's term without stopping the process manually.

### Trigger An Election On One Node

```bash
curl -X POST http://127.0.0.1:8081/raft/simulate/trigger-election
```

What it does:

- forces that node to start an election immediately
- the node increments its term as part of the election process
- it becomes leader only if it wins a majority and its log is up to date enough

### Bump A Node Term Only

```bash
curl -X POST http://127.0.0.1:8081/raft/simulate/bump-term
```

What it does:

- increments the selected node's term
- leaves the node as a follower
- useful when you want to inspect how other nodes react to a higher observed term

### Bump Term And Immediately Start Election

```bash
curl -X POST http://127.0.0.1:8081/raft/simulate/bump-term-and-elect
```

What it does:

- increments the term on that node
- immediately starts a new election
- useful for demonstrating that higher term helps only when the node can still win votes

Important note:

- higher term does not automatically guarantee leadership
- the candidate also needs a majority of votes
- other nodes can reject the vote if the candidate's log is stale

### Force The Current Leader To Step Down

```bash
curl -X POST http://127.0.0.1:8080/raft/simulate/step-down
```

What it does:

- if the targeted node is the leader, it becomes a follower
- this stops leader heartbeats from that node
- another election should happen shortly after

### Typical Demo Flow

1. Check the current roles:

```bash
curl http://127.0.0.1:8080/raft/state
curl http://127.0.0.1:8081/raft/state
curl http://127.0.0.1:8082/raft/state
```

2. Force the leader to step down:

```bash
curl -X POST http://127.0.0.1:8080/raft/simulate/step-down
```

3. Trigger an election on a follower:

```bash
curl -X POST http://127.0.0.1:8081/raft/simulate/trigger-election
```

4. Or make one node more aggressive:

```bash
curl -X POST http://127.0.0.1:8082/raft/simulate/bump-term-and-elect
```

5. Re-check the cluster:

```bash
curl http://127.0.0.1:8080/raft/cluster/state
```

## Swagger

Swagger UI:

```text
/swagger-ui/index.html
```

OpenAPI JSON:

```text
/v3/api-docs
```

Swagger is useful here because it lets you:

- test log appends without writing a separate client
- inspect cluster endpoints quickly
- verify responses while nodes are joining or electing

## Discovery Configuration

Optional environment variables:

- `RAFT_DISCOVERY_PORT`
- `RAFT_DISCOVERY_ANNOUNCE_INTERVAL_MS`
- `RAFT_DISCOVERY_STALE_AFTER_MS`
- `RAFT_DISCOVERY_EVICT_AFTER_MS`
- `RAFT_DISCOVERY_LOCAL_SCAN_ENABLED`
- `RAFT_DISCOVERY_LOCAL_SCAN_START_PORT`
- `RAFT_DISCOVERY_LOCAL_SCAN_END_PORT`

Defaults are defined in [application.yaml](C:\Users\Jai\Desktop\Raftviz\src\main\resources\application.yaml).

## UI Overview

The dashboard is now intentionally built around usability:

- ring topology for quick cluster scanning
- visible leader emphasis
- node cards positioned around the ring
- direct per-node log inspection
- clearer online, degraded, and offline connectivity states

## Troubleshooting

If nodes do not appear locally:

- ensure each node uses a unique `PORT`
- ensure the local scan range includes those ports
- wait a few seconds for discovery polling

If nodes do not appear across laptops:

- verify `ADVERTISE_ADDR` uses the LAN IP
- verify both HTTP and UDP discovery ports are allowed by firewall
- make sure all laptops are on the same network

If the dashboard opens but looks empty:

- check `GET /raft/cluster`
- check `GET /raft/state`
- verify the node is actually running on the expected port
