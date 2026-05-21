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
./compile-all.sh
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

## 🚀 Teste de Estresse (Stress Test)

O projeto inclui uma suíte de testes de carga desenhada para avaliar o tempo de resposta e capacidade de processamento do Processing Node.

### 1. Compilar o módulo
Para Windows:
```bash
cd scripts-windows
.\compile-stress.bat
```

### 2. Executar os Nós Móveis Simulados
Inicie a simulação definindo a quantidade de nós, a duração do teste e fixando um *beacon* para garantir que recebam o *groupcast*:
```bash
cd stress-test
java -jar target/stress-test.jar --nodes 50 --duration 120 --static 0 
```
> **Nota1:** O exemplo acima usa o ip default para o host onde os containers estão rodando (localhost - 127.0.0.1), para especificar outro ip basta usar a flag `--gateway-host` assim, por exemplo: `--gateway-host 172.20.137.125` para o endereço do wsl.

> **Nota2:** É possível omitir `--static 0` para que os nós simulem movimento saltando aleatoriamente entre os *beacons*.

### 3. Disparar e Coletar os Resultados
Com os nós simulados rodando, vá para a janela do seu **Mobile Node** normal (iniciado no passo 4 da execução padrão):

1. Pressione **`A`** para disparar um alerta artificial para o Processing Node. O PN enviará um *groupcast* para todos os nós simulados, que responderão com um `[ACK]`. O PN fará o cálculo do RTT (Round Trip Time).
2. Repita o disparo (A) quantas vezes desejar.
3. Para coletar as métricas calculadas, pressione **`T`** (Request Record).
4. O Processing Node empacotará os resultados calculados e os enviará via *Unicast* de volta para o seu Mobile Node.
5. O Mobile Node automaticamente salvará o resultado no arquivo **`stress_test_results.csv`** na pasta em que foi executado.
