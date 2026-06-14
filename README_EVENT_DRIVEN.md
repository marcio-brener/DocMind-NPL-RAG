# DocMind — Arquitetura Event-Driven

> **Documentação Técnica de Mensageria Assíncrona, Filas RabbitMQ, Pipeline RAG e Escalabilidade**

---

## Arquitetura Event-Driven

O DocMind é construído sobre uma **arquitetura orientada a eventos** (Event-Driven Architecture — EDA), onde operações de longa duração são desacopladas da requisição HTTP e executadas de forma assíncrona por workers especializados.

### Motivação

O processamento de documentos (extração de texto, chunking semântico e vetorização) pode levar dezenas de segundos. Responder ao usuário apenas após o processamento completo seria inaceitável em termos de UX e escalabilidade. A EDA resolve isso:

- A **API aceita a requisição imediatamente** (HTTP 202 Accepted)
- Publica uma **mensagem no broker** (RabbitMQ)
- **Retorna o `document_id`** para rastreamento
- Um **worker assíncrono** consome a mensagem e executa o pipeline
- O **status é consultável** a qualquer momento via `/api/v1/tasks/{task_id}`

### Princípios Implementados

| Princípio | Implementação |
|---|---|
| **Desacoplamento** | API e Worker comunicam-se exclusivamente via RabbitMQ |
| **Assincronicidade** | Workers asyncio/aio-pika, sem bloqueio do event loop |
| **Resiliência** | Retry automático (3×) + Dead Letter Queue |
| **Rastreabilidade** | TaskService persiste estados em `data/tasks.json` |
| **Idempotência** | `upsert` no ChromaDB garante reprocessamento seguro |
| **Escalabilidade** | Múltiplos workers podem consumir a mesma fila |

---

## RabbitMQ

### Topologia de Filas e Exchanges

A topologia é declarada de forma **idempotente** no startup da aplicação (método `_declare_topology()` do `RabbitMQService`).

```
┌─────────────────────────────────────────────────────────────────────┐
│                        RabbitMQ Broker                              │
│                                                                     │
│  ┌─────────────────────┐    routing_key:document.process            │
│  │   document_exchange  │ ──────────────────────────────────────►  │
│  │   (type: DIRECT)     │                                           │
│  │   (durable: true)    │         ┌──────────────────────────────┐  │
│  └─────────────────────┘         │  document_processing_queue    │  │
│                                   │  (durable: true)              │  │
│                                   │  x-dead-letter-exchange:      │  │
│                                   │    document_exchange.dlx      │  │
│                                   │  x-dead-letter-routing-key:   │  │
│                                   │    document_dlq               │  │
│                                   └──────────────┬───────────────┘  │
│                                                  │                   │
│  ┌─────────────────────┐    routing_key:         │                   │
│  │ document_exchange    │    document_dlq         │ on reject(requeue=False)
│  │       .dlx           │ ◄───────────────────── │                   │
│  │ (type: DIRECT)       │                        │                   │
│  │ (durable: true)      │    ┌───────────────────▼───────────────┐  │
│  └──────────┬───────────┘    │          document_dlq             │  │
│             │                │      (Dead Letter Queue)           │  │
│             └───────────────►│      (durable: true)              │  │
│                               └───────────────────────────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                    rag_requests                               │  │
│  │                    (durable: true)                            │  │
│  │                    x-dead-letter-exchange: document_exchange  │  │
│  │                    .dlx                                       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

### Detalhamento das Filas

| Fila | Tipo | Durabilidade | DLX Configurado | Uso |
|---|---|---|---|---|
| `document_processing_queue` | Main | `durable=true` | `document_exchange.dlx` | Ingestão de documentos PDF/MD |
| `rag_requests` | Main | `durable=true` | `document_exchange.dlx` | Pipeline RAG assíncrono |
| `document_dlq` | Dead Letter | `durable=true` | — | Mensagens rejeitadas após 3 tentativas |

### Detalhamento das Exchanges

| Exchange | Tipo | Durabilidade | Uso |
|---|---|---|---|
| `document_exchange` | DIRECT | `durable=true` | Roteamento da fila principal de documentos |
| `document_exchange.dlx` | DIRECT | `durable=true` | Dead Letter Exchange (recebe mensagens rejeitadas) |

### Routing Keys

| Routing Key | Exchange | Destino |
|---|---|---|
| `document.process` | `document_exchange` | `document_processing_queue` |
| `document_dlq` | `document_exchange.dlx` | `document_dlq` |
| *(fila direta)* | `default_exchange` | `rag_requests` (publicação direta via routing_key=queue_name) |

### Configurações de Qualidade

- **QoS / Prefetch**: `prefetch_count=1` — garante **Fair Dispatch** (um worker processa uma mensagem por vez)
- **Delivery Mode**: `PERSISTENT` — mensagens sobrevivem a restart do broker
- **Content Type**: `application/json` — payload serializado em UTF-8
- **Reconexão Automática**: `connect_robust()` do aio-pika com callback de log `_on_reconnect()`

---

## Fluxo de Upload de Documento

### Diagrama de Sequência

```mermaid
sequenceDiagram
    participant C as Cliente
    participant API as FastAPI API
    participant TS as TaskService
    participant RMQ as RabbitMQ
    participant W as DocumentWorker
    participant PROC as DocumentProcessor
    participant SP as SemanticProcessor
    participant EMB as EmbeddingService
    participant DB as ChromaDB

    C->>+API: POST /api/v1/document/upload (file)
    API->>API: Valida extensão (.pdf/.md) e tamanho (≤10MB)
    API->>API: Gera document_id (UUID v4)
    API->>API: Salva arquivo: data/uploads/{doc_id}_{filename}
    API->>TS: create_task(task_id=doc_id, status=QUEUED)
    API->>RMQ: publish_document_processing(doc_id, filename, filepath)
    API-->>-C: 202 { document_id, status: "queued" }

    Note over RMQ,W: Mensagem persiste no broker (PERSISTENT)

    W->>+RMQ: consume(queue=document_processing_queue)
    RMQ-->>W: Mensagem: {document_id, filename, filepath}
    W->>TS: update_task(status=PROCESSING, progress=10)

    W->>+PROC: extract_text_from_pdf/markdown(filepath)
    PROC-->>-W: raw_text, page_count

    W->>PROC: clean_text(raw_text)
    W->>TS: update_task(status=PROCESSING, progress=30)

    W->>+SP: process_text_into_chunks(doc_id, cleaned_text, metadata)
    SP->>+EMB: embed_documents(text_chunks) [em lote]
    EMB-->>-SP: embeddings[384 dims × N chunks]
    SP-->>-W: SemanticProcessResponse(chunks=[...])
    W->>TS: update_task(status=PROCESSING, progress=60)

    W->>DB: delete_document_chunks(doc_id) [idempotente]
    W->>+DB: upsert_chunks(chunks)
    DB-->>-W: True (sucesso)
    W->>TS: update_task(status=PROCESSING, progress=80)

    W->>+RMQ: message.ack()
    RMQ-->>-W: ACK confirmado

    W->>TS: update_task(status=COMPLETED, progress=100)

    C->>API: GET /api/v1/tasks/{doc_id}
    API->>TS: get_task(doc_id)
    API-->>C: { status: "COMPLETED", progress: 100, chunks_indexed: N }
```

### Fluxo de Falha e DLQ

```mermaid
sequenceDiagram
    participant RMQ as RabbitMQ
    participant W as DocumentWorker
    participant DLQ as document_dlq

    RMQ-->>W: Mensagem (retry_count=0)
    W->>W: _execute_pipeline() → Exception
    W->>W: retry_count < 3 ?

    alt retry_count < 3
        W->>RMQ: Publica nova mensagem (x-retry-count: 1)
        W->>RMQ: message.ack() [remove original]
        Note over W: Aguarda próxima entrega
    else retry_count >= 3
        W->>RMQ: message.reject(requeue=False)
        RMQ-->>DLQ: Mensagem encaminhada via DLX
        Note over DLQ: Mensagem aguarda análise manual
    end
```

---

## Fluxo de Consulta RAG

### Visão Geral do Pipeline Assíncrono

```mermaid
sequenceDiagram
    participant C as Cliente
    participant API as FastAPI API
    participant TS as TaskService
    participant RMQ as RabbitMQ
    participant CONS as process_rag_request (consumer)
    participant CACHE as Redis Cache
    participant EMB as EmbeddingService
    participant DB as ChromaDB
    participant LLM as Google Gemini

    C->>+API: POST /api/v1/rag/ask { question, limit, filter_document_id }
    API->>API: Gera task_id + request_id (UUID v4)
    API->>TS: create_task(task_id, status=QUEUED)
    API->>RMQ: publish_rag_request(task_id, request_id, question, limit, filter_document_id)
    API-->>-C: 202 { task_id, request_id, status: "PROCESSING" }

    Note over RMQ,CONS: Consumer inicializado no startup (lifespan) OU via DocumentWorker

    RMQ-->>+CONS: Mensagem: {task_id, request_id, question, limit, filter_document_id}
    CONS->>TS: update_task(status=PROCESSING, progress=50)

    CONS->>CONS: is_cv_query(question)? → boost + limit=12

    CONS->>+CACHE: get(SHA-256(question|limit|doc_id))
    alt Cache HIT
        CACHE-->>CONS: Resposta cached
        CONS->>TS: update_task(COMPLETED, result=cached)
        CONS->>RMQ: message.ack()
    else Cache MISS
        CACHE-->>-CONS: None

        CONS->>+EMB: embed_query(question [+ expansão CV])
        EMB-->>-CONS: query_vector [384 dims]

        CONS->>+DB: similarity_search(query_vector, limit=5×limit, where_filter)
        DB-->>-CONS: raw_results (cosine similarity)

        CONS->>CONS: Deduplicação (texto normalizado único)

        CONS->>CONS: Re-ranking híbrido:
        Note over CONS: hybrid = max(sim, 0.7×sim + 0.3×overlap)
        Note over CONS: + keyword_boost para CV (até +0.3)
        Note over CONS: Ordenação decrescente por hybrid_score

        CONS->>CONS: Threshold dinâmico adaptativo
        Note over CONS: Fallback Top-K (3 chunks) se nenhum passar

        CONS->>CONS: Controle de contexto (≤25.000 chars)

        CONS->>+LLM: ainvoke({ context, question })
        LLM-->>-CONS: answer_text

        CONS->>+CACHE: set("rag_response:{request_id}", result, TTL=3600)
        CACHE-->>-CONS: True

        CONS->>TS: update_task(COMPLETED, progress=100, result=payload)
        CONS->>RMQ: message.ack()
    end

    C->>API: GET /api/v1/tasks/{task_id}
    API->>TS: get_task(task_id)
    API-->>C: { status: "COMPLETED", result: { answer, sources } }

    Note over C,API: Alternativa: GET /api/v1/rag/result/{request_id}
    C->>API: GET /api/v1/rag/result/{request_id}
    API->>CACHE: get("rag_response:{request_id}")
    API-->>C: { status: "COMPLETED", answer, sources }
```

### Dois Caminhos para o Resultado

O DocMind oferece **dois endpoints** para consultar o resultado de uma pergunta RAG:

| Endpoint | Fonte dos Dados | Uso Recomendado |
|---|---|---|
| `GET /api/v1/tasks/{task_id}` | `data/tasks.json` (TaskService) | Acompanhamento de progresso em tempo real |
| `GET /api/v1/rag/result/{request_id}` | Redis Cache | Polling simples pelo resultado final |

---

## NLP Event Worker

### DocumentWorker (`workers/document_worker.py`)

O `DocumentWorker` é um processo **standalone** executado separadamente da API:

```bash
python -m workers.document_worker
```

#### Responsabilidades

| Responsabilidade | Detalhe |
|---|---|
| Conectar ao RabbitMQ | Tenta conexão, retry a cada 5s se indisponível |
| Consumir `document_processing_queue` | Callback: `_on_message()` |
| Consumir `rag_requests` | Callback: `process_rag_request()` (importado de `rabbitmq_service`) |
| Executar pipeline de ingestão | Extração → Limpeza → Chunking → Embedding → ChromaDB |
| Retry automático | Até 3 tentativas via header `x-retry-count` |
| DLQ | `reject(requeue=False)` após 3 falhas |
| Shutdown gracioso | Handlers `SIGINT`/`SIGTERM` com `close()` das conexões |

#### Pipeline de Ingestão por Etapas

| Etapa | Progress | Ação |
|---|---|---|
| Localizar arquivo | 10% | Verifica existência em disco + lê tamanho |
| Extrair texto | 30% | `extract_text_from_pdf()` ou `extract_text_from_markdown()` + `clean_text()` |
| Montar metadados | — | `DocumentMetadata` com filename, content_type, page_count, char_count |
| Chunking semântico | 60% | `process_text_into_chunks()` → chunks com embeddings |
| Persistir no ChromaDB | 80% | `delete_document_chunks()` + `upsert_chunks()` |
| Concluído | 100% | `COMPLETED` com total de chunks indexados |

#### Processamento Assíncrono

O worker utiliza `asyncio.to_thread()` para operações CPU-bound (extração de texto, embeddings), evitando bloqueio do event loop:

```python
raw_text, page_count = await asyncio.to_thread(extract_text)
cleaned_text = await asyncio.to_thread(document_processor.clean_text, raw_text)
semantic_response = await asyncio.to_thread(semantic_processor.process_text_into_chunks, ...)
```

#### Timeout de Segurança

```python
await asyncio.wait_for(self._execute_pipeline(...), timeout=300.0)
```

Documentos que excedem **300 segundos** de processamento são enviados para retry/DLQ automaticamente.

### Consumer RAG no Startup da API

Além do worker standalone, o **FastAPI inicializa um consumer da fila `rag_requests`** no `lifespan`:

```python
# main.py — lifespan
await rabbitmq_service.consume_messages(
    queue_name=settings.RABBITMQ_RAG_QUEUE,
    callback=process_rag_request
)
```

Isso permite que a API **também processe perguntas RAG** sem necessitar do worker standalone rodando, desde que o RabbitMQ esteja disponível.

### Benefícios do Processamento Assíncrono

| Benefício | Impacto |
|---|---|
| **Não-bloqueante** | A API responde em <50ms mesmo para PDFs de 10MB |
| **Fault tolerance** | Falhas no worker não afetam a disponibilidade da API |
| **Observabilidade** | Status rastreável em tempo real via `TaskService` |
| **Escalabilidade** | Múltiplos workers podem consumir a mesma fila (Fair Dispatch) |
| **Durabilidade** | Mensagens persistentes sobrevivem a reinicialização do broker |

---

## Escalabilidade

### Escala com Docker Compose

O `docker-compose.yml` define 4 serviços com healthchecks e dependências:

```yaml
services:
  rabbitmq:    # rabbitmq:3-management — porta 5672 + 15672
  redis:       # redis:7-alpine — porta 6379
  api:         # FastAPI — porta 8000
  worker:      # DocumentWorker (standalone, sem porta)
```

#### Múltiplos Workers

Para escalar horizontalmente o processamento, basta aumentar as réplicas do worker:

```bash
# Docker Compose (escala para 3 workers)
docker-compose up -d --scale worker=3

# Resultado: 3 workers consumindo a mesma fila com Fair Dispatch (prefetch=1)
```

O `prefetch_count=1` garante que cada worker processa exatamente **uma mensagem por vez**, evitando sobrecarga e garantindo distribuição uniforme.

### Escala com Docker Swarm

A infraestrutura Docker Compose está pronta para ser migrada para Docker Swarm:

```bash
# Inicializar Swarm
docker swarm init

# Deploy do stack
docker stack deploy -c docker-compose.yml docmind

# Escalar o worker para N réplicas
docker service scale docmind_worker=5

# Listar serviços
docker service ls
```

#### Considerações para Swarm

| Item | Status |
|---|---|
| RabbitMQ single-node | ✅ Funciona (fila durável compartilhada) |
| Redis single-node | ✅ Funciona (cache compartilhado) |
| ChromaDB `PersistentClient` | ⚠️ Volume compartilhado necessário entre replicas da API |
| `tasks.json` | ⚠️ Volume compartilhado necessário entre API e workers |
| Worker stateless | ✅ Pronto para N réplicas |

---

## Observabilidade

### Logs do RabbitMQ

```
[RABBITMQ] Conectando ao broker em: amqp://guest:******@localhost:5672/
[RABBITMQ] Conexão estabelecida com sucesso.
[QUEUE] Fila DLQ declarada: 'document_dlq' vinculada à Exchange 'document_exchange.dlx'
[QUEUE] Fila principal declarada: 'document_processing_queue' vinculada à Exchange 'document_exchange' com routing_key='document.process' e DLX='document_exchange.dlx'
[QUEUE] Fila RAG declarada: 'rag_requests' com DLX='document_exchange.dlx'
[RABBITMQ] Tentativa de reconexão restabelecida com sucesso com o broker.
```

### Logs do Consumer (Documento)

```
[CONSUMER] Documento recebido da fila: {"document_id": "...", "filename": "..."}
[WORKER] Arquivo localizado com sucesso: .../data/uploads/... | Tamanho: 245760 bytes
[WORKER] Extração de texto concluída. Total de caracteres: 18432
[WORKER] Segmentação semântica concluída. Total de chunks gerados: 42
[WORKER] Documento processado com sucesso: task_id=... | doc_id=... | chunks=42
```

### Logs do Consumer (RAG)

```
[RABBITMQ] Mensagem recebida
[RABBITMQ_CONSUMER] payload.limit=10
[CONSUMER] Executando pipeline RAG | task_id=... | filter_document_id=None
[RABBITMQ] Resposta gerada com sucesso
```

### Logs do Pipeline RAG

```
Pipeline RAG iniciado para a pergunta: '...' | doc_id=None | limit=10 | is_cv=False
[REDIS] Verificando cache para a pergunta normalizada. Chave: 3f4a1b2c...
[REDIS] Cache MISS → Pergunta não encontrada no cache. Executando pipeline completo.
[RETRIEVAL] Iniciando busca vetorial por embeddings. Limite de candidatos (Recall): 50
===== CHROMA DEBUG =====
SPACE: cosine
DISTANCES: [0.18, 0.22, ...]
IDS: ['doc_chunk_3', ...]
==========
[RETRIEVAL] Busca vetorial concluída em 48.23ms. Obtidos 45 candidatos.
===== CHUNKS RECUPERADOS DO CHROMADB =====
[1] Chunk ID: ... | Cosine Similarity: 0.82 | Arquivo: curriculo.pdf | Texto: ...
===== CHUNKS ANTES DO RE-RANKING =====
chunk_id=... | document_id=... | similarity=0.82 | overlap=0.45 | hybrid_score=0.71
[RETRIEVAL] Re-ranking concluído por score híbrido (70% vetorial + 30% overlap).
===== CHUNKS APÓS O RE-RANKING =====
Threshold calculado: 0.5698 (is_cv=False)
[RETRIEVAL] Score híbrido máximo encontrado: 0.8143 | Threshold dinâmico de corte: 0.5698 (baseline: 0.35) | Passaram pelo filtro: 12/45 chunks.
===== CHUNKS DESCARTADOS =====
[1] Chunk ID: ... | Score Hibrido: 0.3241 | Arquivo: ... | Texto: ...
[CONTEXT] Controle de tamanho: selecionados 12/12 chunks com total de 8234 caracteres.
===== CHUNKS ENVIADOS AO GEMINI =====
[LLM_CALL] Preparando chamada ao Gemini:
- Chunks enviados: ['doc_chunk_3', ...]
- Quantidade total de caracteres: 9122
- Quantidade total de tokens estimados: 2280
Chamando LLM (Google Gemini) para geração de resposta...
Resposta gerada com sucesso pelo LLM.
Pipeline RAG concluído em 2134.56ms. LLM usado: True
[REDIS] Salvando resposta no cache → TTL: 3600s
[OBSERVABILIDADE] Resumo RAG Pipeline:
- Question: O que é o DocMind?
- Limit: 10
- Document ID: None
- Chunks Recuperados (ChromaDB): 45
- Chunks Filtrados (Threshold): 12
- Threshold Final: 0.5698
- Scores: [0.8143, 0.7892, ...]
- Tempo Execucao (Total): 2134.56ms
- Cache: MISS
```

### Métricas Expostas nos Logs

| Métrica | Unidade | Log Tag |
|---|---|---|
| Tempo busca vetorial | ms | `[RETRIEVAL]` |
| Total candidatos ChromaDB | count | `[RETRIEVAL]` |
| Chunks após deduplicação | count | `[RETRIEVAL]` |
| Score híbrido máximo | float 0-1 | `[RETRIEVAL]` |
| Threshold dinâmico | float 0-1 | `[RETRIEVAL]` |
| Chunks aprovados/total | count/count | `[RETRIEVAL]` |
| Caracteres no contexto | chars | `[CONTEXT]` |
| Tokens estimados | count | `[LLM_CALL]` |
| Tempo total do pipeline | ms | `Pipeline RAG concluído` |
| Cache HIT/MISS | boolean | `[REDIS]` |

---

## Cache Redis

### Estratégia

O Redis é utilizado como **cache de respostas RAG**, evitando chamadas redundantes ao Gemini para perguntas semanticamente equivalentes.

```
┌─────────────┐   Pergunta   ┌──────────────┐
│  RAG Client │ ──────────►  │  RAGService  │
└─────────────┘              └──────┬───────┘
                                    │ Chave SHA-256
                             ┌──────▼───────┐
                             │    Redis     │
                             │  cache_key   │
                             │  = SHA-256(  │
                             │    question  │
                             │   +limit     │
                             │   +doc_id)   │
                             └──────┬───────┘
                                    │
                   ┌────────────────┴────────────────┐
                   │ HIT                             MISS │
                   ▼                                  ▼
            Resposta cached                   Pipeline completo
            (~1ms)                            (ChromaDB + Gemini)
```

### Geração de Chave Determinística

```python
@staticmethod
def build_cache_key(question, limit, document_id=None) -> str:
    normalized = question.strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = normalized.rstrip("?./! ")
    raw = f"{normalized}|{limit}|{document_id or ''}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
```

Perguntas semanticamente equivalentes geram a **mesma chave**:
- `"O que é RAG?"` → mesmo hash que `"o que é rag"` e `"O que é RAG  "`

### TTL e Ciclo de Vida

| Chave | Formato | TTL | Origem |
|---|---|---|---|
| RAG query cache | `SHA-256(question|limit|doc_id)` | 3600s (1h) | `rag_service.py` |
| RAG result por request | `rag_response:{request_id}` | 3600s (1h) | `rabbitmq_service.py` |

### Invalidação do Cache

O cache é **invalidado automaticamente** (FLUSHDB) em dois casos:

1. **Reprocessamento em lote**: `POST /api/v1/document/reprocess`
2. **Exclusão de documento**: `DELETE /api/v1/document/{document_id}`

```python
await cache_service.clear()  # FLUSHDB — limpa todo o banco Redis
```

### Operações do CacheService

| Método | Operação Redis | Uso |
|---|---|---|
| `get(key)` | `GET key` | Verificar cache antes do pipeline |
| `set(key, value, ttl)` | `SETEX key ttl value` | Persistir resposta com expiração |
| `delete(key)` | `DEL key` | Remoção pontual |
| `exists(key)` | `EXISTS key` | Verificação de existência |
| `clear()` | `FLUSHDB` | Invalidação total |

### Benefícios

| Benefício | Impacto |
|---|---|
| **Latência** | Cache HIT: ~1ms vs MISS: 1-3s (com Gemini) |
| **Custo** | Elimina chamadas redundantes ao Google AI |
| **Consistência** | Invalidado no reprocessamento (dados atualizados) |
| **Resiliência** | Degradação graciosa (no-op) se Redis indisponível |

### Modo Degradado

Se o Redis não estiver disponível, o `CacheService` opera em **modo no-op**:
- `get()` retorna sempre `None` (força pipeline completo)
- `set()` retorna `False` (sem persistência)
- A aplicação funciona normalmente, porém sem caching

---

## Diagramas Mermaid

### Diagrama 1 — Arquitetura Completa do Sistema

```mermaid
graph TB
    subgraph CLIENT["👤 Cliente"]
        C[HTTP Request]
    end

    subgraph API["🚀 FastAPI (porta 8000)"]
        EP_UPLOAD[POST /document/upload]
        EP_RAG[POST /rag/ask]
        EP_RESULT[GET /rag/result]
        EP_TASK[GET /tasks/{id}]
        EP_SEARCH[POST /query/search]
        EP_DELETE[DELETE /document/{id}]
    end

    subgraph BROKER["🐇 RabbitMQ (porta 5672)"]
        Q_DOC[document_processing_queue]
        Q_RAG[rag_requests]
        Q_DLQ[document_dlq]
        EX[document_exchange DIRECT]
        EX_DLX[document_exchange.dlx DIRECT]
    end

    subgraph WORKER["⚙️ DocumentWorker (standalone)"]
        W_DOC[_on_message consumer]
        W_RAG[process_rag_request consumer]
        PIPELINE[Pipeline: Extract → Chunk → Embed → Store]
    end

    subgraph STORAGE["💾 Armazenamento"]
        CHROMA[(ChromaDB\nnlp_rag_collection\nhnsw:cosine)]
        REDIS[(Redis\nCache SHA-256\nTTL 3600s)]
        JSON[(tasks.json\nThread-safe)]
        DISK[data/uploads/\nArquivos físicos]
    end

    subgraph LLM["🤖 Google Gemini"]
        GEM[gemini-2.5-flash\ntemp=0.2\nmax_tokens=2048]
    end

    C --> EP_UPLOAD
    C --> EP_RAG
    C --> EP_RESULT
    C --> EP_TASK
    C --> EP_SEARCH
    C --> EP_DELETE

    EP_UPLOAD --> DISK
    EP_UPLOAD --> JSON
    EP_UPLOAD --> EX
    EX -->|document.process| Q_DOC
    Q_DOC -->|reject requeue=false| EX_DLX
    EX_DLX --> Q_DLQ

    EP_RAG --> JSON
    EP_RAG -->|default_exchange| Q_RAG
    Q_RAG -->|reject requeue=false| EX_DLX

    EP_RESULT --> REDIS
    EP_TASK --> JSON
    EP_SEARCH --> CHROMA

    Q_DOC --> W_DOC
    Q_RAG --> W_RAG

    W_DOC --> PIPELINE
    PIPELINE --> CHROMA
    PIPELINE --> JSON

    W_RAG --> REDIS
    W_RAG --> CHROMA
    W_RAG --> GEM
    W_RAG --> JSON
    W_RAG --> REDIS
```

---

### Diagrama 2 — Pipeline RAG Detalhado

```mermaid
flowchart TD
    START([Pergunta do Usuário]) --> DETECT{is_cv_query?}

    DETECT -->|Sim| CV_EXPAND[Expansão de query:\n+ experiência profissional\n+ empresas, cargo, carreira]
    DETECT -->|Não| CACHE_CHECK

    CV_EXPAND --> CACHE_CHECK

    CACHE_CHECK[Consultar Redis Cache\nSHA-256 key] --> CACHE_HIT{Cache HIT?}

    CACHE_HIT -->|Sim| RETURN_CACHED([Resposta Cached\n~1ms])
    CACHE_HIT -->|Não| EMBED[Gerar Embedding\nall-MiniLM-L6-v2\n384 dims]

    EMBED --> SEARCH[ChromaDB Query\nn_results = 5× limit\nCosine Similarity]

    SEARCH --> DEDUP[Deduplicação\npor texto normalizado]

    DEDUP --> RERANK[Re-ranking Híbrido\n70% cosine + 30% overlap\n+ keyword boost CV]

    RERANK --> THRESHOLD{Score ≥\nThreshold\nDinâmico?}

    THRESHOLD -->|Sim| FILTER_OK[Chunks aprovados]
    THRESHOLD -->|Não/Vazio| TOPK[Fallback Top-K\n3 melhores chunks]

    FILTER_OK --> CV_MIN{CV query e\n<10 chunks?}
    CV_MIN -->|Sim| GUARANTEE[Garante TOP 10 chunks]
    CV_MIN -->|Não| CONTEXT_LIMIT
    GUARANTEE --> CONTEXT_LIMIT
    TOPK --> CONTEXT_LIMIT

    CONTEXT_LIMIT[Controle de Janela\n≤ 25.000 chars] --> LLM_CHECK{LLM\nDisponível?}

    LLM_CHECK -->|Sim| GEMINI[Google Gemini\ngemini-2.5-flash\nPrompt corporativo PT-BR]
    LLM_CHECK -->|Não| FALLBACK[Fallback:\nRetorna contexto bruto]

    GEMINI --> SAVE_CACHE[Salvar no Redis\nTTL 3600s]
    FALLBACK --> SAVE_CACHE

    SAVE_CACHE --> UPDATE_TASK[Atualizar TaskService\nCOMPLETED 100%]
    UPDATE_TASK --> RETURN_ANSWER([Resposta + Sources])
```

---

### Diagrama 3 — Topologia RabbitMQ

```mermaid
graph LR
    PROD_DOC[API: publish_document_processing] -->|routing_key: document.process| EX
    PROD_RAG[API: publish_rag_request] -->|routing_key: rag_requests\nvia default_exchange| Q_RAG

    EX[document_exchange\nDIRECT\ndurable] -->|document.process| Q_DOC

    Q_DOC[document_processing_queue\ndurable\nprefetch=1] --> CONS_DOC[Worker: _on_message]
    Q_RAG[rag_requests\ndurable\nprefetch=1] --> CONS_RAG[Consumer: process_rag_request\nAPI lifespan OU Worker]

    CONS_DOC -->|reject requeue=false| EX_DLX
    CONS_RAG -->|reject requeue=false| EX_DLX

    EX_DLX[document_exchange.dlx\nDIRECT\ndurable] -->|document_dlq| Q_DLQ[document_dlq\ndurable]

    style Q_DLQ fill:#ff6b6b,color:#fff
    style EX_DLX fill:#ffd93d,color:#000
    style EX fill:#6bcb77,color:#000
```

---

### Diagrama 4 — Ciclo de Vida de uma Tarefa

```mermaid
stateDiagram-v2
    [*] --> QUEUED: POST /upload ou /rag/ask\nTaskService.create_task()

    QUEUED --> PROCESSING: Worker consome mensagem\nprogress=10%

    PROCESSING --> PROCESSING: Progresso incremental\n30% → 60% → 80%

    PROCESSING --> COMPLETED: Pipeline concluído\nprogress=100%

    PROCESSING --> FAILED: Erro após 3 tentativas\nou timeout 300s

    COMPLETED --> [*]: Resultado disponível via\n/tasks/{id} ou Redis

    FAILED --> [*]: Mensagem enviada\npara DLQ

    note right of QUEUED
        data/tasks.json
        status: QUEUED
        progress: 0
    end note

    note right of COMPLETED
        data/tasks.json
        status: COMPLETED
        progress: 100
        result: {answer, sources}
    end note
```

---

### Diagrama 5 — Fluxo de Cache Redis

```mermaid
sequenceDiagram
    participant RS as RAGService
    participant CS as CacheService
    participant REDIS as Redis

    RS->>CS: build_cache_key(question, limit, doc_id)
    CS->>CS: SHA-256(normalized_question|limit|doc_id)
    CS-->>RS: "3f4a1b2c..."

    RS->>CS: get("3f4a1b2c...")
    CS->>REDIS: GET 3f4a1b2c...

    alt Cache HIT
        REDIS-->>CS: JSON serializado
        CS-->>RS: RAGResponse (desserializado)
        RS-->>RS: Retorna com cache_hit=True, latency=1ms
    else Cache MISS
        REDIS-->>CS: nil
        CS-->>RS: None
        RS->>RS: Executa pipeline completo...
        RS->>CS: set("3f4a1b2c...", response_dict, ttl=3600)
        CS->>REDIS: SETEX 3f4a1b2c... 3600 {json}
        REDIS-->>CS: OK
    end
```

---

## RELATÓRIO DE CONSISTÊNCIA — Arquitetura Event-Driven

### 1. Funcionalidades Event-Driven Encontradas no Código

| # | Componente | Implementação |
|---|---|---|
| 1 | Exchange DIRECT `document_exchange` | `rabbitmq_service.py:101` |
| 2 | Fila `document_processing_queue` com DLX | `rabbitmq_service.py:108-115` |
| 3 | Fila `rag_requests` com DLX | `rabbitmq_service.py:125-133` |
| 4 | Exchange DLX `document_exchange.dlx` | `rabbitmq_service.py:80-83` |
| 5 | Dead Letter Queue `document_dlq` | `rabbitmq_service.py:87-92` |
| 6 | `prefetch_count=1` (Fair Dispatch) | `rabbitmq_service.py:48` |
| 7 | Mensagens PERSISTENT | `rabbitmq_service.py:166` |
| 8 | Reconexão automática (`connect_robust`) | `rabbitmq_service.py:37` |
| 9 | Callback `_on_reconnect` | `rabbitmq_service.py:62` |
| 10 | Retry via `x-retry-count` header (máx 3) | `document_worker.py:209` |
| 11 | Consumer no lifespan da API (`rag_requests`) | `main.py:34-40` |
| 12 | Worker dual-queue (doc + RAG) | `document_worker.py:324-333` |
| 13 | `asyncio.to_thread()` para CPU-bound | `document_worker.py:101` |
| 14 | Timeout 300s no pipeline | `document_worker.py:214` |
| 15 | Shutdown gracioso SIGINT/SIGTERM | `document_worker.py:46-53` |

### 2. Divergências no README_EVENT_DRIVEN Anterior

| Divergência | Detalhe |
|---|---|
| Exchange type incorreto nos diagramas | Exchange `document_exchange` é DIRECT, não FANOUT |
| Fila `rag_requests` não documentada completamente | DLX configurado mas não aparecia no diagrama |
| Consumer RAG no lifespan da API não documentado | `main.py` inicializa consumer além do worker |
| Retry com `x-retry-count` não documentado | Mecanismo real de retry com header customizado |
| Publicação RAG via `default_exchange` | `rag_requests` usa default_exchange, não `document_exchange` |

### 3. Sugestões de Melhoria na Infraestrutura Event-Driven

1. **Monitoramento de DLQ**: Implementar alertas quando mensagens chegam à `document_dlq`
2. **TTL de mensagens**: Adicionar `x-message-ttl` nas filas para evitar acúmulo indefinido
3. **Delayed Retry**: Em vez de retry imediato, implementar backoff exponencial via plugin `rabbitmq_delayed_message_exchange`
4. **Métricas Prometheus**: Expor métricas de fila (mensagens em aguardo, taxa de processamento)
5. **Separação de DLQ por fila**: `document_dlq` e `rag_dlq` separadas para facilitar triagem de erros
