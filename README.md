# ContextNet - Processing Node (Stress Test / Teste de Escalonamento)

## Description

This repository simulates the **EnQyMo ecosystem** on top of the [ContextNet](https://github.com/ContextNet) middleware, in order to obtain performance metrics (e.g., round-trip time and processing capacity) that are reported in a research paper. The results are not used for training any model; they are only collected and presented in the final paper.

The ecosystem is composed of a **Processing Node (PN)**, a **Mobile Node (MN)** and a **Group Definer (GD)**, all connected through a ContextNet Gateway (with Kafka and Zookeeper). A **stress-test suite** simulates many mobile nodes to measure how the Processing Node behaves when it sends a *groupcast* alert.

## Dataset Information

Not applicable. This project does not use any dataset. The only data it produces are the simulation metrics (`stress_test_results.csv`) described in the [Methodology](#methodology) section.

## Code Information

| Directory / File                     | Purpose                                                                |
|--------------------------------------|------------------------------------------------------------------------|
| `processing-node/`                   | Processing Node: sends groupcast alerts, computes RTT, answers records |
| `mobile-node/`                       | Real Mobile Node: triggers alerts (`A`) and requests records (`T`)     |
| `group-definer/`                     | Group Definer service of ContextNet                                    |
| `stress-test/`                       | Simulated mobile nodes (`stress-test.jar`) and `data/beacons.json`     |
| `utils/`                             | Shared utilities library used by the other modules                     |
| `scripts-linux/`, `scripts-windows/` | Compile scripts (`compile-*.sh` / `compile-*.bat`)                     |
| `start-gw.yml`                       | Docker Compose: Gateway, Kafka and Zookeeper                           |
| `contextnet-stationary.yml`          | Docker Compose: Processing Node and Group Definer                      |
| `properties.xml`                     | Configuration properties                                               |
| `start.sh` / `stop.sh`               | Helper scripts to start/stop the stack                                 |

All modules are Java projects built with Maven (shaded/fat JARs). Simulated nodes are attached to the beacons defined in `stress-test/data/beacons.json`.

## Requirements

- Docker and Docker Compose
- Java 17 or newer
- Maven
- ContextNet libraries (`contextnet`, `ExchangeData`, `clientLib`), resolved by Maven
- Other Maven dependencies (declared in each `pom.xml`): `jackson-databind`, `jackson-datatype-jsr310`, `kafka-clients`, `slf4j-api`, `slf4j-simple`

Used ports: Gateway `6200` (UDP), external Kafka `6010`, Zookeeper `6000`.

## Usage Instructions

### 1. Start the infrastructure (Gateway, Kafka and Zookeeper)

```bash
docker compose -f start-gw.yml up -d
```

### 2. Compile the Processing Node, Mobile Node and Group Definer

Linux/macOS (see the scripts in `scripts-linux/`):

```bash
./compile-all.sh
```

Windows:

```bat
cd scripts-windows
compile-all.bat
```

### 3. Start the Processing Node and Group Definer containers

```bash
docker compose -f contextnet-stationary.yml up --build
```

### 4. Run the Mobile Node

```bash
cd mobile-node/ && java -jar target/mobile-node.jar
```

### 5. Run the stress test

Compile the module (Windows):

```bat
cd scripts-windows
.\compile-stress.bat
```

Start the simulated mobile nodes, setting the number of nodes, the duration (seconds) and a fixed beacon so they receive the groupcast:

```bash
cd stress-test
java -jar target/stress-test.jar --nodes 50 --duration 120 --static 0
```

> **Note 1:** The default gateway host is `127.0.0.1`. Use `--gateway-host` to change it, e.g. `--gateway-host 172.20.137.125` (WSL address).
>
> **Note 2:** Omit `--static 0` so nodes simulate movement by jumping randomly between beacons.

### 6. Trigger alerts and collect results

In the regular **Mobile Node** window (step 4):

1. Press **`A`** to fire an artificial alert. The PN sends a groupcast to all simulated nodes, which answer with `[ACK]`; the PN computes the RTT.
2. Repeat (`A`) as many times as desired.
3. Press **`T`** (Request Record) to collect the metrics.
4. The PN packs the results and sends them via *Unicast* to the Mobile Node.
5. The Mobile Node saves them to **`stress_test_results.csv`** in the folder where it was run.

The output CSV has the columns `alert_id`, `acks_received`, `avg_rtt_ms`, `total_rtt_ms` and `snapshot_timestamp`.

## Methodology

1. The infrastructure (Gateway, Kafka, Zookeeper) is started with Docker Compose.
2. The Processing Node and Group Definer run as containers; the Mobile Node runs locally.
3. `stress-test.jar` spawns N simulated mobile nodes (`--nodes`) for a given time (`--duration`), each attached to a beacon from `beacons.json` (fixed with `--static`, or randomly moving).
4. Each alert (`A`) makes the PN send a groupcast to the nodes of the group; every node replies with an `[ACK]`.
5. The PN measures the RTT per ACK and aggregates, per alert: ACK count, total RTT and average RTT.
6. On request (`T`), the PN sends the aggregated records via Unicast to the Mobile Node, which writes them to `stress_test_results.csv`.
7. The experiment is repeated with different numbers of simulated nodes (e.g., 5, 10, 25, 50, 75, 100) to study how RTT scales. The resulting metrics are reported in the paper.

## Reproduction Script

The following script reproduces one simulation run (run it from the repository root; on Windows use WSL/Git Bash or the equivalent `.bat` steps above). Change `NODES` to reproduce each configuration.

```bash
#!/usr/bin/env bash
set -e

NODES=50          # number of simulated mobile nodes
DURATION=120      # test duration in seconds

# 1. Infrastructure
docker compose -f start-gw.yml up -d

# 2. Build
./compile-all.sh                       # or scripts-windows\compile-all.bat
(cd scripts-windows && ./compile-stress.bat)   # stress-test module (Windows)

# 3. Processing Node + Group Definer
docker compose -f contextnet-stationary.yml up --build -d

# 4. Simulated nodes (background)
(cd stress-test && java -jar target/stress-test.jar \
    --nodes "$NODES" --duration "$DURATION" --static 0) &

# 5. Mobile Node (interactive): press 'A' to fire alerts, then 'T' to collect results.
#    Metrics are written to mobile-node/stress_test_results.csv
(cd mobile-node && java -jar target/mobile-node.jar)

wait
```

> Firing alerts (`A`) and requesting the record (`T`) is done interactively in the Mobile Node console.

## Citations

If you use this code or the reported results, please cite the paper that presents the EnQyMo ecosystem simulation (reference to be added upon publication) and the ContextNet middleware.

## License & Contribution Guidelines

**License:** No license file is currently included in this repository; the code is not licensed for reuse or redistribution until a license is added. Contact the repository owner (EnQyMo) before reusing this code.

**Contributions:** this repository does not currently define a formal contribution process. If you'd like to contribute, please open an issue first to discuss the proposed change before submitting a pull request.
