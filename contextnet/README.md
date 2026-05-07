# ContextNet - Processing Node

Projeto que usa ContextNet para um sistema de monitoramento do ar.

## Como executar

### Pré-requisitos
- Docker instalado
- Java 17 ou superior
- Maven (para compilar)

### 1. Subir a infraestrutura (Gateway, Kafka e Zookeeper)

```bash
docker compose -f start-gw.yml up -d
```

### 2. Compilar o Processing Node, Mobile Node e Group Definer

```bash
source compile-all.sh
```

### 3. Subir os containers do Processing Node e Group Definer

```bash
docker compose -f contextnet-stationary.yml up --build
```

### 4. Executar o Mobile Node

```bash
cd mobile-node/ && java -jar target/mobile-node.jar
```

## 🔧 Portas utilizadas

- **Gateway**: `6200` (UDP)
- **Kafka externo**: `6010`
- **Zookeeper**: `6000`
