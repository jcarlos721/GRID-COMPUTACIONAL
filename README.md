# GRID-COMPUTACIONAL

Grid Computacional em Python 3.10+ (apenas biblioteca padrão) para calcular a soma de números primos até `N` utilizando o **Crivo de Eratóstenes segmentado** sobre TCP com modelo *Pull*, controle de *lease* e tolerância a falhas.

## Estrutura do Projeto

- `src/protocol.py`: Serialização/desserialização JSON delimitada por `\n`, validação de contrato (`REGISTER`, `REGISTERED`, `GET_TASK`, `TASK_ASSIGN`, `HEARTBEAT`, `SUBMIT_RESULT`, `NO_TASK`, `ACK`, `REJECT`), rejeição de valores negativos em `SUBMIT_RESULT` e `LineBuffer` para tratar pacotes TCP fragmentados ou agrupados.
- `src/sieve.py`: Geração de primos base até `isqrt(N)` (`generate_base_primes`) e crivo segmentado em `[start, end]` (`sum_primes_in_range`).
- `src/task_manager.py`: Máquina de estados thread-safe (`PENDING`, `IN_PROGRESS`, `COMPLETED`) protegida por `threading.Lock`, com controle de *lease* em relógio monotônico (`time.monotonic()`), isolamento por `session_id`, `attempt_id`, métricas (`reassignments`, `stale_rejects`, `completed_by_worker`) e proteção contra resultados obsoletos.
- `src/sequential_baseline.py`: Execução sequencial de referência com medição de tempo (`time.perf_counter()`).
- `src/master.py`: Nó coordenador TCP multithread que exige `REGISTER` antes de `GET_TASK`, mantém monitor periódico de *lease* (`_reaper_loop`) e exibe o resumo final de métricas.
- `src/worker.py`: Nó trabalhador TCP em modelo *pull* com *heartbeat* periódico, timeout periódico no `recv` (`1.0s`) para parada limpa e flags de simulação de falhas.
- `tests/test_core.py`: Testes unitários isolados de protocolo, crivo segmentado, baseline e máquina de estados.
- `tests/test_grid.py`: Testes de integração TCP entre Master e Workers (incluindo `GET_TASK` sem `REGISTER`, `worker_id` duplicado e timeout no `recv`).
- `tests/fault_simulation.py`: Simulação reproduzível de queda abrupta de worker e atraso com expiração de *lease* (`STALE_ATTEMPT`).

## Parâmetros CLI

### Master (`python3 -m src.master`)
- `--host`: Endereço de bind (padrão: `127.0.0.1`)
- `--port`: Porta TCP (padrão: `5000`)
- `--n`: Limite superior `N` (padrão: `1000000`)
- `--chunk-size`: Tamanho de cada subintervalo `[start, end]` (padrão: `50000`)
- `--lease-timeout`: Tempo máximo de *lease* em segundos (padrão: `10.0`)

### Worker (`python3 -m src.worker`)
- `--master-host`: Endereço IP/host do Master (padrão: `127.0.0.1`)
- `--master-port`: Porta TCP do Master (padrão: `5000`)
- `--worker-id`: Identificador do Worker (padrão: `worker-<pid>-<6hex>`)
- `--heartbeat-interval`: Intervalo de *heartbeat* em segundos (padrão: `max(0.2, lease_timeout / 3.0)`)
- `--artificial-delay`: Atraso artificial em segundos antes de enviar `SUBMIT_RESULT`, após encerrar os *heartbeats* da tarefa (simula nó lento)
- `--fail-after-tasks`: Encerra o worker abruptamente após concluir `N` tarefas (simula queda)

### Comportamento do `HEARTBEAT`
Durante o processamento de cada bloco `[start, end]`, o worker mantém uma thread auxiliar que envia `HEARTBEAT` (`worker_id`, `task_id`, `attempt_id`) no intervalo configurado. No Master, `TaskManager.renew_lease` verifica sob `Lock` se a tarefa ainda está `IN_PROGRESS` para o mesmo `worker_id` e `attempt_id` antes do vencimento e **renova efetivamente o prazo** (`deadline_ts = time.monotonic() + lease_timeout_sec`).

## Execução Local (Mesma Máquina)

### 1. Referência Sequencial (Baseline)
```bash
python3 -m src.sequential_baseline 100000
```

### 2. Grid Local (1 Master + 2 Workers)
Terminal 1 (Master):
```bash
python3 -m src.master --host 127.0.0.1 --port 5056 --n 100000 --chunk-size 25000 --lease-timeout 5
```

Terminais 2 e 3 (Workers):
```bash
python3 -m src.worker --master-host 127.0.0.1 --master-port 5056 --worker-id worker-1
python3 -m src.worker --master-host 127.0.0.1 --master-port 5056 --worker-id worker-2
```

Exemplo de saída real observada:
```text
[MASTER] Escutando em 127.0.0.1:5056 | N=100000 | chunk_size=25000 | lease=5.0s | tarefas=4
[MASTER] Worker registrado: worker-1 (session=...)
[MASTER] Atribuída tarefa 0 (attempt=1) [2, 25001] -> worker-1
[MASTER] Tarefa 0 (attempt=1) concluída por worker-1 | soma_parcial=32405717 | acumulado=32405717 (1/4)
[MASTER] Worker registrado: worker-2 (session=...)
[MASTER] Atribuída tarefa 1 (attempt=1) [25002, 50001] -> worker-1
[MASTER] Atribuída tarefa 2 (attempt=1) [50002, 75001] -> worker-2
[MASTER] Tarefa 1 (attempt=1) concluída por worker-1 | soma_parcial=88607591 | acumulado=121013308 (2/4)
[MASTER] Atribuída tarefa 3 (attempt=1) [75002, 100000] -> worker-1
[MASTER] Tarefa 2 (attempt=1) concluída por worker-2 | soma_parcial=141060794 | acumulado=262074102 (3/4)
[MASTER] Tarefa 3 (attempt=1) concluída por worker-1 | soma_parcial=192322435 | acumulado=454396537 (4/4)
[MASTER] CONCLUÍDO: N=100000 | primes=9592 | sum=454396537 | elapsed_ms=310.68
[MASTER] RESUMO MÉTRICAS | elapsed_sec=0.311 | total_tasks=4 | reassignments=0 | stale_rejects=0 | completed_by_worker={'worker-1': 3, 'worker-2': 1} | final_sum=454396537
```

## Execução em Rede Local (Multi-Máquina)

Para distribuir o cálculo entre máquinas na mesma LAN (ex.: Master no IP `192.168.0.134`, porta `5000`), inicie o Master fazendo bind em `0.0.0.0` (todas as interfaces de rede) e aponte os Workers para o IP do Master.

Exemplo com carga alta (`N=100000000`, referência baseline esperada: `primes=5761455`, `sum=279209790387276`):

1. **Na máquina do Master (`192.168.0.134`):**
```bash
python3 -m src.master --host 0.0.0.0 --port 5000 --n 100000000 --chunk-size 2000000 --lease-timeout 15
```

2. **Em cada máquina Worker na mesma rede:**
```bash
python3 -m src.worker --master-host 192.168.0.134 --master-port 5000 --worker-id pc-sala-1
python3 -m src.worker --master-host 192.168.0.134 --master-port 5000 --worker-id pc-sala-2
```

3. **Comparação local com o baseline sequencial:**
```bash
python3 -m src.sequential_baseline 100000000
```
*(Certifique-se de que a porta TCP `5000` esteja liberada no firewall da máquina do Master.)*

## Cenários de Teste

Executar toda a suíte unitária e de integração:
```bash
python3 -m unittest discover -s tests -v
```

Executar especificamente os cenários de simulação de falhas (queda abrupta e atraso com expiração de *lease*):
```bash
python3 -m tests.fault_simulation -v
```

## Guia Rápido de Defesa (Arquitetura)

### 1. Ciclo de vida de uma tarefa
- **`PENDING`**: Estado inicial de todo subintervalo `[start, end]` criado ao particionar `[2, N]`.
- **`IN_PROGRESS`**: Quando um worker registrado solicita trabalho (`GET_TASK`), a tarefa muda para `IN_PROGRESS`, recebe um `attempt_id` incremental, o `session_id` da conexão TCP e um `deadline_ts = monotonic() + lease_timeout_sec`. Enquanto processa, o worker envia `HEARTBEAT` para renovar esse prazo.
- **`IN_PROGRESS` -> `PENDING` (Reatribuição)**: Se o *lease* expirar (`monotonic() >= deadline_ts`) ou se a conexão específica (`session_id`) cair abruptamente, o Master devolve a tarefa para `PENDING` sob `threading.Lock`, liberando-a para outro worker com um novo `attempt_id`.
- **`COMPLETED`**: Ao receber `SUBMIT_RESULT` válido, o Master soma `partial_sum` no acumulador global e marca a tarefa como `COMPLETED`.

### 2. Por que o `attempt_id` evita dupla contagem
Imagine que o `worker-1` pega a tarefa `0` (`attempt_id=1`) e trava ou sofre um atraso de rede maior que o `lease_timeout`. O Master expira o *lease*, devolve a tarefa `0` para `PENDING` e a entrega ao `worker-2` com `attempt_id=2`. O `worker-2` conclui e o Master consolida o resultado. Quando o `worker-1` finalmente acorda e envia `SUBMIT_RESULT` com `attempt_id=1`, o `TaskManager` verifica sob `Lock` que `task.attempt_id != 1` (e/ou `task.state != IN_PROGRESS`) e responde `REJECT (STALE_ATTEMPT)`, descartando o resultado obsoleto sem alterar `total_sum`.

### 3. Como o *Pull Model* equilibra máquinas heterogêneas
Em vez de o Master empurrar (*push*) uma cota fixa de blocos por máquina no início, cada worker puxa (`GET_TASK`) apenas um bloco por vez quando termina o anterior. Assim, máquinas mais rápidas ou com menos carga completam mais ciclos de `GET_TASK -> cálculo -> SUBMIT_RESULT` naturalmente, enquanto máquinas mais lentas processam menos blocos — evitando que um nó lento retenha metade do trabalho no final (*straggler problem*) e maximizando o aproveitamento do cluster.

### 4. *Overhead* de rede vs. Ganho de paralelismo (`N` pequeno vs. `N` grande)
Para entradas pequenas como `N=100000`, o crivo sequencial termina em poucos milissegundos (~5 ms). Nesse cenário, o tempo gasto abrindo conexões TCP, serializando/desserializando JSON, trocando mensagens (`REGISTER`, `GET_TASK`, `SUBMIT_RESULT`, `ACK`) e coordenando threads supera amplamente o tempo de cálculo, tornando o Grid mais lento que a execução sequencial. Já em cargas maiores como `N=100000000` com `--chunk-size` adequado (ex.: `2000000` a `5000000`, resultando em ~20 a 50 tarefas), cada bloco demanda frações de segundo a segundos de CPU — o custo de comunicação TCP/JSON torna-se desprezível frente ao processamento e o paralelismo entre múltiplas máquinas reduz o tempo total de parede (*wall-clock*).
