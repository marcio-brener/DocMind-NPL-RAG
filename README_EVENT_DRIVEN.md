# DocMind — Arquitetura Event-Driven com RabbitMQ, RAG e Redis

> **Documentação Técnica e Acadêmica — Versão 2.0**
> Plataforma Corporativa de Processamento de Linguagem Natural com RAG, Banco Vetorial e Mensageria Assíncrona.

---

## Índice

1. [Visão Geral](#1-visão-geral)
2. [Arquitetura Geral](#2-arquitetura-geral)
3. [Fluxo de Processamento de Documentos](#3-fluxo-de-processamento-de-documentos)
4. [NLP Event Worker](#4-nlp-event-worker)
5. [RabbitMQ — Topologia de Mensageria](#5-rabbitmq--topologia-de-mensageria)
6. [Pipeline RAG](#6-pipeline-rag)
7. [Observabilidade](#7-observabilidade)
8. [Redis Cache](#8-redis-cache)
9. [Banco Vetorial — ChromaDB](#9-banco-vetorial--chromadb)
10. [Benefícios da Arquitetura Event-Driven](#10-benefícios-da-arquitetura-event-driven)
11. [Tecnologias Utilizadas](#11-tecnologias-utilizadas)
12. [Fluxo Completo do Sistema](#12-fluxo-completo-do-sistema)
13. [Endpoints da API](#13-endpoints-da-api)
14. [Variáveis de Ambiente](#14-variáveis-de-ambiente)
15. [Execução e Operação](#15-execução-e-operação)
16. [Estrutura de Arquivos](#16-estrutura-de-arquivos)
17. [Conclusão](#17-conclusão)

---

## 1. Visão Geral

### 1.1 Objetivo do DocMind

O **DocMind** é uma plataforma corporativa de Processamento de Linguagem Natural (NLP) construída sobre o paradigma **RAG — Retrieval-Augmented Generation**. Seu objetivo central é permitir que usuários façam perguntas em linguagem natural sobre documentos técnicos e empresariais (PDF e Markdown), obtendo respostas contextualizadas e fundamentadas exclusivamente no conteúdo dos documentos ingeridos.

A plataforma integra:
- **Ingestão assíncrona** de documentos via mensageria
- **Banco vetorial** para recuperação semântica por similaridade de cosseno
- **LLM corporativo** (Google Gemini) para geração de respostas
- **Cache Redis** para redução de latência e custo operacional
- **Observabilidade** completa do pipeline NLP

### 1.2 Problema Resolvido

A abordagem ingênua de processamento de documentos (upload → processamento síncrono → resposta) apresenta limitações críticas em ambientes de produção:

| Problema | Abordagem Síncrona | Abordagem Event-Driven |
|---|---|---|
| Latência de upload | Alta (bloqueia cliente) | Nula (resposta imediata) |
| Timeout de conexão | Risco real com PDFs grandes | Eliminado |
| Escalabilidade | Limitada ao server pool | Horizontal via múltiplos workers |
| Tolerância a falhas | Zero (falha = perda) | Alta (retry + DLQ) |
| Experiência do usuário | Degradada | Fluida |

### 1.3 Motivação para Uso de Event-Driven Architecture (EDA)

O processamento de documentos no DocMind envolve operações computacionalmente intensivas:

1. **Extração de texto** (PyPDF para PDFs de múltiplas páginas)
2. **Chunking semântico** (LangChain RecursiveCharacterTextSplitter)
3. **Geração de embeddings** (HuggingFace `all-MiniLM-L6-v2`, vetores 384 dimensões)
4. **Persistência vetorial** (ChromaDB com índice HNSW cosine)

Essas operações podem levar de 5 a 60+ segundos para documentos grandes, tornando o processamento síncrono inviável em produção.

A **Event-Driven Architecture** resolve isso de forma elegante: o servidor FastAPI registra a intenção de processamento, publica um evento no broker e retorna imediatamente ao cliente com um `task_id` para rastreamento assíncrono.

### 1.4 Benefícios da Abordagem Adotada

- **Desacoplamento total**: FastAPI não conhece os detalhes de processamento; o Worker não conhece a API
- **Escalabilidade horizontal**: múltiplos workers podem processar documentos em paralelo
- **Rastreabilidade**: cada documento tem uma tarefa com status e progresso em tempo real
- **Resiliência**: falhas no processamento não afetam a disponibilidade da API
- **Retry automático**: até 3 tentativas automáticas com headers `x-retry-count`
- **Dead Letter Queue (DLQ)**: mensagens com falha permanente são preservadas para análise
- **Graceful Shutdown**: o worker encerra conexões de forma segura ao receber `SIGINT`/`SIGTERM`

---

## 2. Arquitetura Geral

### 2.1 Componentes Principais

| Componente | Tecnologia | Responsabilidade |
|---|---|---|
| **API Gateway** | FastAPI + Uvicorn | Receber requisições HTTP, validar, publicar eventos |
| **Message Broker** | RabbitMQ 3 (aio-pika) | Intermediar mensagens entre produtor e consumidor |
| **NLP Worker** | Python asyncio + aio-pika | Consumir fila, executar pipeline de ingestão |
| **RAG Consumer** | FastAPI Lifespan (asyncio) | Consumir fila `rag_requests` e executar pipeline RAG |
| **Banco Vetorial** | ChromaDB (PersistentClient) | Indexar e recuperar chunks por similaridade vetorial |
| **Cache** | Redis (redis.asyncio) | Cachear respostas RAG com TTL configurável |
| **LLM** | Google Gemini 2.5 Flash | Gerar respostas em linguagem natural a partir do contexto |
| **Embedding Model** | HuggingFace all-MiniLM-L6-v2 | Gerar vetores de 384 dimensões para chunks e queries |
| **Task Tracker** | TaskService (JSON file) | Rastrear status e progresso de cada tarefa |

### 2.2 Diagrama de Arquitetura Completa

```mermaid
graph TB
    subgraph Cliente["🌐 Cliente HTTP"]
        U[Usuário / Aplicação]
    end

    subgraph API["🚀 FastAPI Application (Port 8000)"]
        direction TB
        EP_UPLOAD["POST /api/v1/document/upload"]
        EP_RAG["POST /api/v1/rag/ask"]
        EP_RESULT["GET /api/v1/rag/result/{id}"]
        EP_TASKS["GET /api/v1/tasks/{task_id}"]
        EP_HEALTH["GET /api/v1/health"]
        TS[TaskService<br/>📄 data/tasks.json]
        PROD[RabbitMQ Producer<br/>aio-pika]
    end

    subgraph BROKER["📨 RabbitMQ Broker"]
        direction LR
        EX["Exchange: document_exchange<br/>(DIRECT, durable)"]
        Q_DOC["Queue: document_processing_queue<br/>(durable, DLX configurado)"]
        Q_RAG["Queue: rag_requests<br/>(durable, DLX configurado)"]
        EX_DLX["DLX: document_exchange.dlx<br/>(DIRECT, durable)"]
        Q_DLQ["DLQ: document_dlq<br/>(durable)"]
    end

    subgraph WORKER["⚙️ NLP Document Worker (Processo Separado)"]
        DW[DocumentWorker<br/>asyncio consumer]
        PIPE["Pipeline de Ingestão<br/>asyncio.to_thread"]
    end

    subgraph RAG_CONSUMER["🧠 RAG Consumer (FastAPI Lifespan)"]
        RC[process_rag_request<br/>callback assíncrono]
    end

    subgraph STORAGE["💾 Armazenamento"]
        CHROMA["ChromaDB<br/>nlp_rag_collection<br/>(cosine HNSW)"]
        REDIS["Redis Cache<br/>TTL: 3600s"]
        FS["Sistema de Arquivos<br/>data/uploads/"]
    end

    subgraph LLM_SVC["🤖 Google Gemini API"]
        GEMINI["gemini-2.5-flash<br/>LangChain Chain"]
    end

    U -->|"POST /upload"| EP_UPLOAD
    U -->|"POST /rag/ask"| EP_RAG
    U -->|"GET /rag/result/{id}"| EP_RESULT
    U -->|"GET /tasks/{id}"| EP_TASKS

    EP_UPLOAD --> TS
    EP_UPLOAD --> FS
    EP_UPLOAD --> PROD
    EP_RAG --> TS
    EP_RAG --> PROD

    PROD -->|"routing_key: document.process"| EX
    PROD -->|"default_exchange"| Q_RAG

    EX -->|"binding"| Q_DOC
    Q_DOC --> DW
    Q_RAG --> RC

    DW --> PIPE
    PIPE --> FS
    PIPE --> CHROMA
    PIPE --> TS

    RC --> REDIS
    RC --> CHROMA
    RC --> LLM_SVC
    RC --> TS

    EP_RESULT --> REDIS

    Q_DOC -.->|"reject/nack"| EX_DLX
    Q_RAG -.->|"reject/nack"| EX_DLX
    EX_DLX -->|"routing_key: document_dlq"| Q_DLQ

    CHROMA --> GEMINI

    style BROKER fill:#ff6b35,color:#fff
    style WORKER fill:#2d6a4f,color:#fff
    style RAG_CONSUMER fill:#1b4332,color:#fff
    style LLM_SVC fill:#7b2d8b,color:#fff
    style STORAGE fill:#1a472a,color:#fff
```

---

## 3. Fluxo de Processamento de Documentos

### 3.1 Descrição Detalhada

O fluxo de ingestão de documentos é inteiramente assíncrono e composto por duas fases distintas: **publicação** (FastAPI) e **consumo** (Worker).

#### Fase 1 — Upload e Enfileiramento (FastAPI)

1. **Recepção do arquivo**: O cliente faz `POST /api/v1/document/upload` com arquivo PDF ou Markdown
2. **Validação**: Verificação de extensão (`.pdf`, `.md`) e tamanho (máx. 10 MB)
3. **Persistência em disco**: O arquivo é salvo em `data/uploads/{uuid}_{filename}`
4. **Criação da tarefa**: `TaskService.create_task()` cria entrada em `data/tasks.json` com `status=QUEUED`
5. **Publicação no broker**: `RabbitMQService.publish_document_processing()` publica JSON na exchange `document_exchange` com `routing_key=document.process`
6. **Resposta imediata**: HTTP 202 com `document_id` e `task_id` para rastreamento

#### Fase 2 — Processamento pelo Worker

O `DocumentWorker` opera como processo independente, consumindo mensagens da fila `document_processing_queue`:

| Etapa | Progresso | Status | Descrição |
|---|---|---|---|
| Criação da tarefa | 0% | `QUEUED` | TaskService registra intenção de processamento |
| Localização do arquivo | 10% | `PROCESSING` | Worker localiza arquivo em `UPLOAD_DIR` |
| Extração de texto | 30% | `PROCESSING` | PyPDF (PDF) ou leitura direta (Markdown) |
| Chunking semântico | 60% | `PROCESSING` | `RecursiveCharacterTextSplitter` (LangChain) |
| Persistência no ChromaDB | 80% | `PROCESSING` | Geração de embeddings + `upsert` no ChromaDB |
| Concluído | 100% | `COMPLETED` | N chunks indexados com sucesso |

### 3.2 Diagrama do Fluxo de Processamento

```mermaid
sequenceDiagram
    actor Cliente
    participant API as FastAPI<br/>(document.py)
    participant TS as TaskService<br/>(tasks.json)
    participant FS as Filesystem<br/>(data/uploads/)
    participant RMQP as RabbitMQ<br/>(Producer)
    participant RMQB as RabbitMQ<br/>(Broker)
    participant RMQC as RabbitMQ<br/>(Consumer/Worker)
    participant DW as DocumentWorker<br/>(_execute_pipeline)
    participant SP as SemanticProcessor
    participant ES as EmbeddingService<br/>(HuggingFace)
    participant VS as VectorStore<br/>(ChromaDB)

    Cliente->>+API: POST /upload (arquivo PDF/MD)
    API->>API: Validação (extensão, tamanho)
    API->>FS: Salva arquivo em disco
    API->>TS: create_task(QUEUED, 0%)
    API->>RMQP: publish_document_processing(doc_id, filepath)
    RMQP->>RMQB: exchange.publish(routing_key="document.process")
    API-->>-Cliente: HTTP 202 {document_id, task_id, status:"queued"}

    Note over RMQB,RMQC: Processamento Assíncrono<br/>(pode ocorrer milissegundos depois)

    RMQB->>RMQC: Entrega mensagem da fila document_processing_queue
    RMQC->>DW: _on_message(message)
    DW->>TS: update_task(PROCESSING, 10%) — Localizando arquivo
    DW->>FS: Verifica existência do arquivo
    DW->>TS: update_task(PROCESSING, 30%) — Extraindo texto
    DW->>DW: extract_text() [asyncio.to_thread]
    DW->>DW: clean_text() [asyncio.to_thread]
    DW->>TS: update_task(PROCESSING, 60%) — Chunking
    DW->>SP: process_text_into_chunks(doc_id, text, metadata)
    SP->>ES: embed_documents(text_chunks) [batch]
    ES-->>SP: List[List[float]] (vetores 384 dims)
    SP-->>DW: SemanticProcessResponse(chunks)
    DW->>TS: update_task(PROCESSING, 80%) — Persistindo ChromaDB
    DW->>VS: delete_document_chunks(doc_id) [asyncio.to_thread]
    DW->>VS: upsert_chunks(chunks) [asyncio.to_thread]
    VS-->>DW: True (sucesso)
    DW->>TS: update_task(COMPLETED, 100%)
    DW->>RMQC: message.ack()

    Cliente->>API: GET /api/v1/tasks/{task_id}
    API->>TS: get_task(task_id)
    TS-->>API: TaskResponse{status, progress, message}
    API-->>Cliente: JSON {status:"COMPLETED", progress:100, chunks: N}
```

### 3.3 Payload da Mensagem RabbitMQ (Ingestão)

```json
{
  "document_id": "3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c",
  "filename": "relatorio_anual.pdf",
  "filepath": "/app/data/uploads/3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c_relatorio_anual.pdf",
  "uploaded_at": "2026-06-11T21:38:05.123456"
}
```

---

## 4. NLP Event Worker

### 4.1 Responsabilidades

O `DocumentWorker` ([workers/document_worker.py](workers/document_worker.py)) é um **processo standalone assíncrono** que opera de forma completamente independente da API FastAPI. Suas responsabilidades são:

- Consumir mensagens da fila `document_processing_queue` via **aio-pika** (asyncio)
- Executar o pipeline completo de ingestão de documentos
- Rastrear e atualizar o progresso via `TaskService`
- Implementar **retry automático** (até 3 tentativas) via headers `x-retry-count`
- Encaminhar mensagens irrecuperáveis para a **Dead Letter Queue (DLQ)**
- Reconectar automaticamente ao broker em caso de falha de rede
- Encerrar graciosamente ao receber sinais `SIGINT`/`SIGTERM`

### 4.2 Diagrama Interno do Worker

```mermaid
stateDiagram-v2
    [*] --> Inicializando : python -m workers.document_worker
    Inicializando --> ConectandoRMQ : setup_signal_handlers()

    ConectandoRMQ --> Consumindo : rabbitmq_service.connect() == True
    ConectandoRMQ --> Aguardando : Broker indisponível
    Aguardando --> ConectandoRMQ : asyncio.sleep(5s)

    Consumindo --> ParseandoMensagem : Mensagem recebida da fila
    ParseandoMensagem --> ValidandoCampos : JSON válido
    ParseandoMensagem --> DLQ : JSON inválido → reject(requeue=False)

    ValidandoCampos --> ExecutandoPipeline : Campos obrigatórios presentes
    ValidandoCampos --> DLQ : Campos ausentes → reject(requeue=False)

    ExecutandoPipeline --> Sucesso : Pipeline concluído (< 300s)
    ExecutandoPipeline --> Timeout : asyncio.TimeoutError (>= 300s)
    ExecutandoPipeline --> Falha : Exception genérica

    Sucesso --> ACK : message.ack()
    ACK --> Consumindo

    Timeout --> VerificandoRetry
    Falha --> VerificandoRetry

    VerificandoRetry --> Reenfileirando : retry_count < max_retries (3)
    VerificandoRetry --> DLQ : retry_count >= max_retries → reject(requeue=False)

    Reenfileirando --> Consumindo : message.ack() + republish com x-retry-count++

    DLQ --> Consumindo : Task atualizada para FAILED

    Consumindo --> Encerrando : SIGINT / SIGTERM
    Encerrando --> [*] : rabbitmq_service.close()
```

### 4.3 Estratégia de Retry e DLQ

O Worker implementa um mecanismo de resiliência baseado em **headers de controle**:

```python
# Leitura do contador de retentativas via header AMQP
headers = dict(message.headers or {})
retry_count = headers.get("x-retry-count", 0)
max_retries = 3

if retry_count < max_retries:
    # Re-publica com x-retry-count incrementado
    headers["x-retry-count"] = retry_count + 1
    # message.ack() da mensagem antiga + nova publicação
else:
    # reject(requeue=False) → DLX → DLQ
    task_service.update_task(task_id, status=TaskStatus.FAILED, ...)
```

**Timeout de processamento**: cada mensagem tem limite de **300 segundos** via `asyncio.wait_for()`. Documentos que excedam esse tempo são tratados como falha e submetidos ao retry.

### 4.4 Processamento Assíncrono com `asyncio.to_thread`

Operações bloqueantes (I/O em disco, CPU-bound para embeddings) são executadas em **thread pools** para não bloquear o event loop:

```python
# Extração de texto em thread pool (I/O bound)
raw_text, page_count = await asyncio.to_thread(extract_text)
cleaned_text = await asyncio.to_thread(document_processor.clean_text, raw_text)

# Chunking e embeddings em thread pool (CPU bound)
semantic_response = await asyncio.to_thread(
    semantic_processor.process_text_into_chunks, ...
)

# Persistência no ChromaDB em thread pool (I/O bound)
await asyncio.to_thread(vector_store.delete_document_chunks, document_id)
await asyncio.to_thread(vector_store.upsert_chunks, semantic_response.chunks)
```

### 4.5 Benefícios da Desacoplagem

- **Zero impacto na API**: falhas no worker não afetam a disponibilidade da API FastAPI
- **Escalabilidade independente**: `docker-compose up --scale worker=N` instancia N workers em paralelo
- **Sem compartilhamento de estado**: worker e API se comunicam exclusivamente via broker + arquivo JSON
- **Hot restart**: o worker pode ser reiniciado sem perda de mensagens (fila durable)

---

## 5. RabbitMQ — Topologia de Mensageria

### 5.1 Topologia Declarada

O DocMind declara sua topologia de filas de forma **idempotente** na inicialização do `RabbitMQService`, garantindo que todas as estruturas existam independentemente da ordem de inicialização dos serviços.

```mermaid
graph LR
    subgraph EXCHANGES["Exchanges"]
        EX["document_exchange<br/>(DIRECT, durable)"]
        DLX["document_exchange.dlx<br/>(DIRECT, durable)"]
    end

    subgraph QUEUES["Queues"]
        Q_MAIN["document_processing_queue<br/>(durable)<br/>x-dead-letter-exchange: document_exchange.dlx<br/>x-dead-letter-routing-key: document_dlq"]
        Q_RAG["rag_requests<br/>(durable)<br/>x-dead-letter-exchange: document_exchange.dlx<br/>x-dead-letter-routing-key: document_dlq"]
        Q_DLQ["document_dlq<br/>(durable)"]
    end

    subgraph PRODUCERS["Produtores"]
        P1["FastAPI<br/>(upload_document)"]
        P2["FastAPI<br/>(ask_question)"]
    end

    subgraph CONSUMERS["Consumidores"]
        C1["DocumentWorker<br/>(processo separado)"]
        C2["process_rag_request<br/>(FastAPI Lifespan)"]
    end

    P1 -->|"routing_key: document.process"| EX
    P2 -->|"default_exchange"| Q_RAG

    EX -->|"binding: document.process"| Q_MAIN
    DLX -->|"binding: document_dlq"| Q_DLQ

    Q_MAIN -->|"prefetch=1"| C1
    Q_RAG -->|"prefetch=1"| C2

    Q_MAIN -.->|"nack/reject"| DLX
    Q_RAG -.->|"nack/reject"| DLX

    style EXCHANGES fill:#ff6b35,color:#fff
    style QUEUES fill:#2d6a4f,color:#fff
    style PRODUCERS fill:#1a6495,color:#fff
    style CONSUMERS fill:#7b2d8b,color:#fff
```

### 5.2 Configuração Detalhada das Filas

#### Fila Principal — `document_processing_queue`

| Parâmetro | Valor | Descrição |
|---|---|---|
| Nome | `document_processing_queue` | Configurável via `RABBITMQ_QUEUE` |
| Durabilidade | `durable=True` | Sobrevive a restart do broker |
| Persistência da mensagem | `delivery_mode=PERSISTENT` | Mensagem salva em disco pelo broker |
| Prefetch Count | `1` | Fair dispatch — worker processa uma mensagem por vez |
| ACK | Manual | Confirmado somente após pipeline concluído com sucesso |
| Dead Letter Exchange | `document_exchange.dlx` | Destino de mensagens rejeitadas |
| DLQ Routing Key | `document_dlq` | Rota para a fila de falhas permanentes |

#### Fila RAG — `rag_requests`

| Parâmetro | Valor | Descrição |
|---|---|---|
| Nome | `rag_requests` | Configurável via `RABBITMQ_RAG_QUEUE` |
| Durabilidade | `durable=True` | Sobrevive a restart do broker |
| Persistência da mensagem | `delivery_mode=PERSISTENT` | Garantia de entrega |
| Consumidor | FastAPI Lifespan (processo API) | Consumer inicializado no startup da API |
| Dead Letter Exchange | `document_exchange.dlx` | Destino de falhas no pipeline RAG |

#### Dead Letter Queue — `document_dlq`

| Parâmetro | Valor | Descrição |
|---|---|---|
| Nome | `document_dlq` | Fila de análise post-mortem |
| Durabilidade | `durable=True` | Preserva mensagens para análise |
| Fonte | `document_exchange.dlx` | Alimentada por falhas nas filas principais |
| Binding | `routing_key=document_dlq` | Rota da DLX para a DLQ |

### 5.3 Exchanges

| Exchange | Tipo | Durabilidade | Uso |
|---|---|---|---|
| `document_exchange` | DIRECT | `durable=True` | Rota mensagens de ingestão de documentos |
| `document_exchange.dlx` | DIRECT | `durable=True` | Dead Letter Exchange para mensagens rejeitadas |

### 5.4 Payload da Mensagem RAG

```json
{
  "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "question": "Quais são as experiências profissionais listadas no currículo?",
  "limit": 10,
  "filter_document_id": "3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c",
  "timestamp": "2026-06-11T21:38:05.123456"
}
```

### 5.5 Reconexão Robusta

O `RabbitMQService` utiliza `aio_pika.connect_robust()` que fornece **reconexão automática** nativa. Adicionalmente, um callback `_on_reconnect` é registrado para logging corporativo de eventos de reconexão:

```python
self.connection = await aio_pika.connect_robust(
    settings.RABBITMQ_URL,
    timeout=10
)
self.connection.reconnect_callbacks.add(self._on_reconnect)
```

---

## 6. Pipeline RAG

### 6.1 Visão Geral do Pipeline

O pipeline RAG (Retrieval-Augmented Generation) é o núcleo intelectual do DocMind. Ele implementa um fluxo de 13 etapas que vai desde a recepção da pergunta até a geração da resposta final, com múltiplas camadas de otimização.

```mermaid
flowchart TD
    A["🎤 Pergunta do Usuário<br/>POST /api/v1/rag/ask"] --> B["🔑 Publicar na Fila rag_requests<br/>task_id = UUID gerado"]
    B --> C["📨 Consumer process_rag_request<br/>(FastAPI Lifespan)"]
    C --> D{{"🔍 Detecção de Tipo de Query<br/>is_cv_query()?"}}

    D -->|"CV/Experiência"| E1["limit = 12<br/>candidate_limit = 60<br/>boost keywords ativos"]
    D -->|"Query Padrão"| E2["limit = request.limit<br/>candidate_limit = 25+"]

    E1 --> F["🔑 Build Cache Key<br/>SHA-256(question + limit + doc_id)"]
    E2 --> F

    F --> G{{"⚡ Redis Cache Hit?"}}
    G -->|"HIT"| H["✅ Retorno Imediato<br/>latência em ms"]
    G -->|"MISS"| I["🔢 Geração de Embedding<br/>EmbeddingService.embed_query()"]

    I --> J["🗄️ Busca Vetorial ChromaDB<br/>similarity_search(candidate_limit)"]
    J --> K["🔄 Deduplicação de Chunks<br/>por texto normalizado"]
    K --> L["🎯 Re-ranking Híbrido<br/>70% cosine + 30% token overlap<br/>+ keyword boost (CV)"]
    L --> M["🌡️ Filtro por Threshold Dinâmico<br/>max_score × 0.50 (CV) / 0.70 (padrão)"]
    M --> N["📏 Controle de Contexto<br/>max 25.000 caracteres"]
    N --> O["📝 Montagem do Prompt<br/>_build_context_string()"]
    O --> P["🤖 Chamada ao Google Gemini<br/>gemini-2.5-flash"]
    P --> Q["💾 Salvar no Redis<br/>TTL = 3600s"]
    Q --> R["📊 Log de Observabilidade<br/>[OBSERVABILIDADE] Resumo RAG Pipeline"]
    R --> S["✅ Resposta Final + Fontes<br/>task_id COMPLETED no TaskService"]
```

### 6.2 Etapas Detalhadas do Pipeline

#### Etapa 1 — Detecção de Tipo de Query (`is_cv_query`)

O pipeline detecta automaticamente se a pergunta é sobre **currículo/experiência profissional** para ajustar dinamicamente os parâmetros de recuperação:

```python
keywords = ["experiencia", "trabalhou", "empresa", "empresas", "emprego",
            "historico profissional", "carreira", "curriculo", "trabalho", "cargo"]
```

- **CV query**: `limit=12`, `candidate_limit=60`, `threshold=max_score×0.50`, keyword boost de até +0.30
- **Query padrão**: `limit=request.limit`, `candidate_limit=max(limit×5, 25)`, `threshold=max(max_score×0.70, RAG_MIN_SIMILARITY)`

#### Etapa 2 — Cache Redis com Chave Determinística

Antes de executar qualquer busca vetorial, o pipeline verifica o cache Redis. A chave é gerada via SHA-256 para evitar colisões e garantir consistência:

```python
# Normalização: lowercase + remoção de espaços extras + strip de pontuação
raw = f"{normalized_question}|{limit}|{document_id or ''}"
cache_key = hashlib.sha256(raw.encode("utf-8")).hexdigest()
```

**Cache HIT**: retorno em milissegundos, sem custo de API, sem consumo de tokens.
**Cache MISS**: execução do pipeline completo.

#### Etapa 3 — Geração do Embedding da Query

```python
query_vector = embedding_service.embed_query(embed_query_text)
# Resultado: List[float] com 384 dimensões (all-MiniLM-L6-v2)
# Vetores normalizados (L2) para similaridade de cosseno consistente
```

Para queries de CV, o texto da query é expandido com termos relacionados antes da geração do embedding para aumentar o recall:

```python
embed_query_text = f"{question} experiência profissional empresas trabalhou cargo..."
```

#### Etapa 4 — Busca Vetorial no ChromaDB

```python
raw_results = vector_store.similarity_search(
    query_vector=query_vector,
    limit=candidate_limit,    # Recupera N×5 candidatos
    where_filter=where_filter  # {"source_doc_id": doc_id} se fornecido
)
```

ChromaDB converte distâncias cosine (`1 - cosine_similarity`) em similaridade `[0, 1]`.

#### Etapa 5 — Deduplicação de Chunks

Chunks com texto idêntico (após normalização) são removidos antes do re-ranking, evitando redundância no contexto enviado ao LLM.

#### Etapa 6 — Re-ranking Híbrido

O score final de cada chunk combina **similaridade vetorial** e **sobreposição de tokens** da query:

```
hybrid_score = max(cosine_similarity, 0.7×cosine_similarity + 0.3×token_overlap_ratio)
```

Para queries de CV, um **keyword boost** adicional de até `+0.30` é aplicado a chunks que contêm palavras-chave profissionais.

#### Etapa 7 — Filtro por Threshold Dinâmico Adaptativo

O threshold é calculado dinamicamente com base no score máximo encontrado:

| Condição | Threshold |
|---|---|
| Query de CV (`is_cv=True`) | `max_hybrid_score × 0.50` |
| Query padrão, max_score < `RAG_MIN_SIMILARITY` | `max_hybrid_score × 0.70` |
| Query padrão, max_score ≥ `RAG_MIN_SIMILARITY` | `max(max_hybrid_score × 0.70, RAG_MIN_SIMILARITY)` |

Se nenhum chunk passar no threshold, o **fallback Top-K** retorna os 3 melhores chunks automaticamente, garantindo que o LLM sempre tenha algum contexto.

> **⚠️ Evolução Importante**: Em versões anteriores do pipeline, apenas os primeiros chunks recuperados eram enviados ao modelo (truncamento prematuro). A implementação atual envia **todos os chunks relevantes** após a filtragem por threshold, limitados apenas pelo controle de janela de contexto (25.000 caracteres). Isso aumenta significativamente a qualidade contextual das respostas, especialmente para documentos como currículos onde informações estão distribuídas por múltiplos chunks.

#### Etapa 8 — Controle de Tamanho de Contexto

Para evitar estouro da janela de contexto do LLM, o pipeline limita o contexto a **25.000 caracteres** antes de enviar ao Gemini:

```python
MAX_CONTEXT_CHARS = 25000
safe_chunks = []
current_chars = 0
for chunk in final_chunks:
    if current_chars + len(chunk["text"]) > MAX_CONTEXT_CHARS:
        break
    safe_chunks.append(chunk)
    current_chars += len(chunk["text"])
```

#### Etapa 9 — Montagem do Prompt e Chamada ao Gemini

O prompt é estruturado com regras obrigatórias para que o LLM consolide informações de múltiplos fragmentos, não apenas o primeiro:

```
FRAGMENTOS DE CONTEXTO:
[Fragmento 1 - Fonte: relatorio.pdf]
<texto do chunk 1>

---

[Fragmento 2 - Fonte: relatorio.pdf]
<texto do chunk 2>
```

**LLM Chain (LangChain)**: `ChatPromptTemplate → ChatGoogleGenerativeAI → StrOutputParser`

- Modelo: `gemini-2.5-flash` (configurável)
- Temperature: `0.2` (respostas determinísticas)
- Max tokens: `2048`

#### Etapa 10 — Persistência no Redis e Resposta Final

Após geração da resposta, o resultado é persistido no Redis com `TTL=3600s` e a tarefa é atualizada para `COMPLETED` no `TaskService` com o payload completo da resposta.

### 6.3 Diagrama de Sequência do Pipeline RAG

```mermaid
sequenceDiagram
    actor Cliente
    participant API as FastAPI<br/>(rag.py)
    participant RMQB as RabbitMQ<br/>(rag_requests)
    participant RAG as RAGService<br/>(rag_service.py)
    participant REDIS as Redis Cache
    participant ES as EmbeddingService
    participant VS as VectorStore<br/>(ChromaDB)
    participant GEMINI as Google Gemini API
    participant TS as TaskService

    Cliente->>+API: POST /rag/ask {question, limit, filter_document_id}
    API->>TS: create_task(QUEUED)
    API->>RMQB: publish_rag_request(task_id, question, limit, filter_doc)
    API-->>-Cliente: HTTP 202 {task_id, request_id}

    RMQB->>RAG: process_rag_request(message)
    RAG->>TS: update_task(PROCESSING, 50%)
    RAG->>RAG: is_cv_query(question) → ajusta limit e candidate_limit
    RAG->>REDIS: get(SHA256_cache_key)

    alt Cache HIT
        REDIS-->>RAG: RAGResponse serializado
        RAG->>TS: update_task(COMPLETED, 100%)
        Note over RAG: Latência em ms, zero custo de token
    else Cache MISS
        REDIS-->>RAG: None
        RAG->>ES: embed_query(question)
        ES-->>RAG: List[float] (384 dims)
        RAG->>VS: similarity_search(query_vector, candidate_limit)
        VS-->>RAG: List[Dict] chunks com similarity scores
        RAG->>RAG: Deduplicação por texto
        RAG->>RAG: Re-ranking híbrido (cosine + token overlap + keyword boost)
        RAG->>RAG: Filtro threshold dinâmico adaptativo
        RAG->>RAG: Controle de contexto (máx 25.000 chars)
        RAG->>GEMINI: ainvoke({context, question})
        GEMINI-->>RAG: answer_text
        RAG->>REDIS: set(cache_key, response, TTL=3600)
        RAG->>TS: update_task(COMPLETED, 100%, result=payload)
        RAG->>RMQB: message.ack()
    end

    Cliente->>API: GET /rag/result/{request_id}
    API->>REDIS: get("rag_response:{request_id}")
    REDIS-->>API: resultado serializado
    API-->>Cliente: {status:"COMPLETED", answer, sources}
```

---

## 7. Observabilidade

### 7.1 Logs de Observabilidade Implementados

O pipeline RAG emite um **bloco de observabilidade estruturado** em cada execução, permitindo diagnóstico completo sem necessidade de ferramentas externas:

```
[OBSERVABILIDADE] Resumo RAG Pipeline:
- Question: <pergunta do usuário>
- Limit: <chunks solicitados>
- Document ID: <filtro aplicado ou None>
- Chunks Recuperados (ChromaDB): <N candidatos brutos>
- Chunks Filtrados (Threshold): <N chunks aprovados>
- Threshold Final: <0.XXXX>
- Scores: [0.XXXX, 0.XXXX, ...]
- Tempo Execucao (Total): XXX.XXms
- Cache: HIT | MISS
```

### 7.2 Métricas Capturadas por Etapa

| Evento | Log gerado | Informações capturadas |
|---|---|---|
| Pergunta recebida | `[RABBITMQ] Mensagem recebida` | Question (60 chars), task_id, filter_doc |
| Verificação de cache | `[REDIS] Verificando cache` | Chave SHA-256 (16 chars preview) |
| Cache HIT | `[REDIS] Cache HIT → Resposta encontrada` | Latência total em ms |
| Cache MISS | `[REDIS] Cache MISS → Pipeline completo` | — |
| Busca vetorial | `[RETRIEVAL] Busca vetorial concluída` | Tempo em ms, N candidatos |
| Chunks recuperados | `===== CHUNKS RECUPERADOS DO CHROMADB =====` | chunk_id, similarity, filename, texto inicial |
| Antes do re-ranking | `===== CHUNKS ANTES DO RE-RANKING =====` | similarity, overlap_ratio, hybrid_score |
| Após re-ranking | `===== CHUNKS APÓS O RE-RANKING =====` | Nova ordem por hybrid_score |
| Threshold calculado | `Threshold calculado: X.XXXX` | Valor adaptativo, is_cv flag |
| Chunks descartados | `===== CHUNKS DESCARTADOS =====` | chunk_id, hybrid_score, arquivo, texto |
| Controle de contexto | `[CONTEXT] Controle de tamanho` | Chars selecionados / limite |
| Chamada ao LLM | `[LLM_CALL] Preparando chamada ao Gemini` | Chunks enviados, tokens estimados |
| Tempo total | `Pipeline RAG concluído em Xms` | LLM usado: True/False |

### 7.3 Logs do Worker

| Evento | Log gerado |
|---|---|
| Worker iniciado | `[Worker] DocMind Asynchronous Document Worker Iniciado` |
| Mensagem recebida | `[CONSUMER] Documento recebido da fila: {payload}` |
| Arquivo localizado | `[WORKER] Arquivo localizado: {filepath} | Tamanho: N bytes` |
| Texto extraído | `[WORKER] Extração de texto concluída. Caracteres: N` |
| Chunking concluído | `[WORKER] Segmentação semântica concluída. Chunks: N` |
| Documento processado | `[WORKER] Documento processado com sucesso: task_id | doc_id | chunks` |
| Retry acionado | `[RABBITMQ] Redirecionando para retry N/3` |
| Falha permanente | `[WORKER] Limite de retentativas atingido. Enviando para DLQ` |

### 7.4 Relevância para LLMOps

As métricas de observabilidade implementadas suportam práticas de **LLMOps** (Machine Learning Operations aplicado a LLMs):

- **Monitoramento de qualidade de retrieval**: scores de similaridade e distribuição de threshold permitem identificar degradação no índice vetorial
- **Análise de custo**: cache HIT/MISS ratio informa a eficiência do cache e o custo estimado de chamadas ao Gemini
- **Detecção de problemas**: chunks descartados em excesso indicam possível necessidade de reavaliação do threshold ou do modelo de embeddings
- **Rastreabilidade de resposta**: cada resposta possui referências completas (chunk_id, filename, similarity) para auditoria e validação
- **Latência por componente**: tempos separados para busca vetorial (`vector_search_ms`) e LLM (`llm_ms`) permitem identificar gargalos

---

## 8. Redis Cache

### 8.1 Objetivo

O Redis atua como camada de **cache semântico** para respostas do pipeline RAG. Perguntas semanticamente equivalentes (mesmo texto normalizado + mesmo document_id + mesmo limit) retornam imediatamente do cache, sem nova busca vetorial ou chamada ao LLM.

### 8.2 Estratégia de Cache

O `CacheService` ([app/services/cache_service.py](app/services/cache_service.py)) implementa dois padrões de cache:

#### Padrão 1 — Cache RAG Semântico (via RAGService)

- **Chave**: `SHA-256(question_normalizada | limit | document_id)`
- **Valor**: `RAGResponse` serializado em JSON
- **TTL**: `3600s` (1 hora) — configurável via `CACHE_TTL_SECONDS`

#### Padrão 2 — Cache RAG por Request ID (via RabbitMQ Consumer)

- **Chave**: `rag_response:{request_id}`
- **Valor**: `{request_id, answer, sources[]}`
- **TTL**: `3600s` (hardcoded no consumer)

### 8.3 Normalização da Chave

```python
# Normalização garante que variações ortográficas não gerem chaves diferentes
normalized = question.strip().lower()          # "Quem é..." → "quem é..."
normalized = re.sub(r"\s+", " ", normalized)  # Espaços múltiplos → espaço único
normalized = normalized.rstrip("?./! ")       # Remove pontuação final
raw = f"{normalized}|{limit}|{document_id or ''}"
cache_key = hashlib.sha256(raw.encode("utf-8")).hexdigest()  # 64 chars hex
```

### 8.4 Operações Suportadas

| Método | Descrição | Comportamento em Falha |
|---|---|---|
| `get(key)` | Recupera valor ou `None` | `None` (gracioso) |
| `set(key, value, ttl)` | Persiste com expiração | `False` (gracioso) |
| `delete(key)` | Remove entrada específica | `False` (gracioso) |
| `exists(key)` | Verifica existência | `False` (gracioso) |
| `clear()` | FLUSHDB — limpa tudo | `False` (gracioso) |

### 8.5 Degradação Graciosa (Graceful Degradation)

O `CacheService` opera em **modo degradado** quando o Redis não está disponível — todos os métodos retornam `None` ou `False` sem lançar exceções, garantindo que o pipeline RAG continue funcionando mesmo sem cache:

```python
def _connect(self) -> None:
    if not REDIS_AVAILABLE:
        # Cache desativado — no-op
        return
    try:
        self._client = aioredis.from_url(settings.REDIS_URL, ...)
        self._available = True
    except Exception:
        self._available = False  # Modo degradado
```

### 8.6 Benefícios Mensuráveis

| Benefício | Impacto |
|---|---|
| **Redução de custo de tokens** | Perguntas repetidas não consomem tokens do Gemini |
| **Latência** | Cache HIT: ~5ms vs Pipeline completo: 2.000–10.000ms |
| **Throughput** | Múltiplos usuários com mesmas perguntas não sobrecarregam o LLM |
| **Proteção contra rate limiting** | Menos chamadas à API Gemini por período |

---

## 9. Banco Vetorial — ChromaDB

### 9.1 Configuração

O `VectorStoreService` ([app/services/vector_store.py](app/services/vector_store.py)) utiliza o ChromaDB em modo `PersistentClient` com armazenamento em `data/chromadb/`:

```python
self.client = chromadb.PersistentClient(path=settings.CHROMADB_PATH)
self.collection = self.client.get_or_create_collection(
    name="nlp_rag_collection",
    metadata={"hnsw:space": "cosine"}  # Distância cosine para similaridade semântica
)
```

### 9.2 Estrutura dos Embeddings e Metadados

Cada chunk indexado no ChromaDB possui:

```json
{
  "id": "3f8a2b1c-4d5e-6f7a_chunk_42",
  "embedding": [0.123, -0.456, ...],
  "document": "...texto do chunk...",
  "metadata": {
    "source_doc_id": "3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c",
    "filename": "relatorio_anual.pdf",
    "chunk_index": 42,
    "char_count": 987,
    "total_chunks": 158,
    "uploaded_at": "2026-06-11T21:38:05.123456"
  }
}
```

### 9.3 Modelo de Embeddings

| Propriedade | Valor |
|---|---|
| Modelo | `sentence-transformers/all-MiniLM-L6-v2` |
| Dimensão | 384 |
| Normalização | L2 (vetores unitários) |
| Geração | Batch (`embed_documents`) ou unitária (`embed_query`) |
| Fallback | Hash SHA-256 determinístico (offline) |

### 9.4 Processo de Chunking

O texto é segmentado via `RecursiveCharacterTextSplitter` (LangChain):

| Parâmetro | Valor (padrão) | Configuração |
|---|---|---|
| `chunk_size` | 1000 chars | `CHUNK_SIZE` |
| `chunk_overlap` | 150 chars | `CHUNK_OVERLAP` |
| `separators` | `["\n\n", "\n", " ", ""]` | Prioriza parágrafos |

### 9.5 Busca Vetorial

```python
results = self.collection.query(
    query_embeddings=[query_vector],
    n_results=limit,
    where=where_filter,       # Filtro opcional por document_id
    include=["documents", "metadatas", "distances"]
)
```

A conversão de distância cosine para similaridade:

```python
similarity = max(0.0, 1.0 - distance)  # ChromaDB retorna distância cosine
```

### 9.6 Relação com o Pipeline RAG

```mermaid
graph LR
    INGEST["Ingestão<br/>(Worker)"] -->|"upsert_chunks()"| CHROMA[(ChromaDB<br/>nlp_rag_collection)]
    RAG["Pipeline RAG<br/>(RAGService)"] -->|"similarity_search()"| CHROMA
    CHROMA -->|"chunks + scores"| RERANK["Re-ranking<br/>Híbrido"]
    RERANK --> GEMINI["Google Gemini<br/>(Geração)"]

    style CHROMA fill:#2d6a4f,color:#fff
```

---

## 10. Benefícios da Arquitetura Event-Driven

### 10.1 Escalabilidade Horizontal

A separação entre produtor (FastAPI) e consumidor (Worker) permite escalar cada componente de forma independente:

```bash
# Escalar workers para processamento paralelo de documentos
docker-compose up -d --scale worker=5

# A API FastAPI permanece em escala fixa, atendendo requisições HTTP
```

Múltiplos workers podem processar documentos simultaneamente sem conflito, pois cada mensagem é consumida por exatamente um worker (fair dispatch com `prefetch_count=1`).

### 10.2 Baixo Acoplamento

Os componentes se comunicam exclusivamente via **contrato de mensagem** (JSON payload), sem dependência direta de código:

```mermaid
graph LR
    A["FastAPI"] -->|"JSON via AMQP"| B["RabbitMQ"]
    B -->|"JSON via AMQP"| C["Worker"]

    A -.->|"Sem dependência direta"| C
```

- A API pode ser atualizada sem reiniciar o worker
- O worker pode ser substituído por implementação em outra linguagem sem alteração na API
- Novos consumidores podem ser adicionados à mesma fila sem modificar o produtor

### 10.3 Processamento Assíncrono

O cliente HTTP obtém resposta imediata (HTTP 202) independentemente do tamanho do documento. O processamento ocorre em background, e o progresso é consultável via `GET /api/v1/tasks/{task_id}`.

### 10.4 Tolerância a Falhas

| Cenário de Falha | Comportamento |
|---|---|
| Worker crasheia durante processamento | Mensagem retorna à fila (sem ACK) → reprocessamento |
| Broker RabbitMQ reinicia | Filas durable preservam mensagens; workers reconectam automaticamente |
| ChromaDB indisponível | Tarefa marcada FAILED, mensagem vai para retry |
| Google Gemini timeout/erro | Fallback de contexto (sem LLM) ativado automaticamente |
| Redis indisponível | Pipeline funciona normalmente sem cache (modo degradado) |
| 3 retries esgotados | Mensagem enviada para DLQ; task marcada FAILED para análise |

### 10.5 Facilidade de Manutenção

- **Observabilidade nativa**: logs estruturados (Loguru) em cada etapa do pipeline
- **Rastreabilidade**: `task_id` vincula toda operação ao seu status em tempo real
- **Reprocessamento**: endpoint `POST /api/v1/document/reprocess` reindexar todos os documentos
- **Invalidação de cache**: operações de delete e reprocess limpam o Redis automaticamente
- **Management UI do RabbitMQ**: `http://localhost:15672` para monitorar filas em tempo real

---

## 11. Tecnologias Utilizadas

| Tecnologia | Versão Mínima | Função no Projeto |
|---|---|---|
| **Python** | 3.11+ | Linguagem principal da plataforma |
| **FastAPI** | ≥0.111.0 | Framework web assíncrono para a API REST |
| **Uvicorn** | ≥0.29.0 | Servidor ASGI de alto desempenho |
| **Pydantic** | ≥2.7.0 | Validação de schemas e configurações (Settings) |
| **pydantic-settings** | ≥2.2.1 | Gestão de variáveis de ambiente tipadas |
| **RabbitMQ** | 3-management | Message broker AMQP para eventos assíncronos |
| **aio-pika** | ≥9.4.1 | Cliente assíncrono AMQP para RabbitMQ (asyncio) |
| **pika** | ≥1.3.2 | Cliente síncrono AMQP (dependência transitiva) |
| **Redis** | 7+ | Cache semântico de respostas RAG |
| **redis[asyncio]** | ≥5.0.4 | Cliente Redis assíncrono para Python |
| **ChromaDB** | ≥0.5.0 | Banco vetorial com índice HNSW cosine |
| **LangChain** | ≥0.2.1 | Framework para construção do pipeline RAG e prompt templates |
| **langchain-community** | ≥0.2.1 | Integração HuggingFace Embeddings |
| **langchain-google-genai** | ≥1.0.6 | Integração LangChain com Google Gemini |
| **Google Gemini** | gemini-2.5-flash | LLM para geração de respostas em linguagem natural |
| **google-generativeai** | ≥0.5.4 | SDK oficial Google para AI Generativa |
| **sentence-transformers** | ≥2.6.0 | Modelo `all-MiniLM-L6-v2` para geração de embeddings locais |
| **PyPDF** | ≥4.2.0 | Extração de texto de documentos PDF |
| **Loguru** | ≥0.7.2 | Logging estruturado com interceptação de frameworks |
| **Docker** | 24+ | Containerização de todos os serviços |
| **Docker Compose** | 3.9 | Orquestração local de múltiplos containers |
| **Pytest** | ≥8.1.1 | Framework de testes unitários e de integração |
| **pytest-asyncio** | ≥0.23.6 | Suporte a testes assíncronos com asyncio |
| **httpx** | ≥0.27.0 | Cliente HTTP assíncrono para testes de integração |

---

## 12. Fluxo Completo do Sistema

O diagrama abaixo representa o fluxo end-to-end do DocMind, desde o upload do documento até a obtenção da resposta RAG.

```mermaid
graph TB
    subgraph USUARIO["👤 Usuário"]
        U1["Upload de Documento<br/>PDF ou Markdown"]
        U2["Pergunta em<br/>Linguagem Natural"]
        U3["Consulta Resultado<br/>via task_id"]
    end

    subgraph API_LAYER["🚀 FastAPI — Camada de API (Port 8000)"]
        direction LR
        EP1["POST /document/upload<br/>Valida + Salva + Cria Task"]
        EP2["POST /rag/ask<br/>Cria Task + Publica"]
        EP3["GET /rag/result/{id}<br/>Consulta Redis"]
        EP4["GET /tasks/{task_id}<br/>Consulta TaskService"]
    end

    subgraph MQ["📨 RabbitMQ Broker"]
        direction LR
        Q1["document_processing_queue<br/>(durable, DLX)"]
        Q2["rag_requests<br/>(durable, DLX)"]
        DLQ["document_dlq<br/>(Dead Letters)"]
    end

    subgraph WORKERS["⚙️ Processamento Assíncrono"]
        W1["DocumentWorker<br/>Processo Standalone<br/>asyncio + aio-pika"]
        W2["RAGConsumer<br/>FastAPI Lifespan<br/>asyncio callback"]
    end

    subgraph INGEST_PIPE["🔄 Pipeline de Ingestão"]
        direction TB
        P1["1. Extrai Texto<br/>(PyPDF / Markdown)"]
        P2["2. Limpa e Normaliza<br/>(clean_text)"]
        P3["3. Chunking Semântico<br/>(RecursiveCharacterTextSplitter<br/>chunk_size=1000, overlap=150)"]
        P4["4. Gera Embeddings em Lote<br/>(HuggingFace all-MiniLM-L6-v2<br/>384 dimensões)"]
        P5["5. Upsert no ChromaDB<br/>(HNSW cosine index)"]
    end

    subgraph RAG_PIPE["🧠 Pipeline RAG"]
        direction TB
        R1["1. Detecta Tipo de Query<br/>(is_cv_query?)"]
        R2["2. Verifica Cache Redis<br/>(SHA-256 key)"]
        R3["3. Embedding da Query<br/>(embed_query → 384 dims)"]
        R4["4. Busca Vetorial ChromaDB<br/>(similarity_search)"]
        R5["5. Deduplica + Re-rank<br/>(70% cosine + 30% overlap)"]
        R6["6. Filtro Threshold Dinâmico<br/>(max_score × 0.50~0.70)"]
        R7["7. Controle de Contexto<br/>(máx 25.000 chars)"]
        R8["8. Monta Prompt + Chama Gemini<br/>(gemini-2.5-flash)"]
        R9["9. Salva no Redis<br/>(TTL=3600s)"]
    end

    subgraph STORAGE["💾 Armazenamento Persistente"]
        CHROMA[("ChromaDB<br/>nlp_rag_collection<br/>./data/chromadb")]
        REDIS_DB[("Redis<br/>Cache Semântico<br/>redis://localhost:6379/0")]
        FS_DB[("Filesystem<br/>./data/uploads/")]
        TASKS_DB[("tasks.json<br/>./data/tasks.json")]
    end

    subgraph GEMINI_SVC["🤖 Google Gemini API"]
        G["gemini-2.5-flash<br/>Temperature: 0.2<br/>Max Tokens: 2048"]
    end

    U1 --> EP1
    U2 --> EP2
    U3 --> EP3
    U3 --> EP4

    EP1 -->|"publica"| Q1
    EP1 -->|"persiste"| FS_DB
    EP1 -->|"cria tarefa"| TASKS_DB

    EP2 -->|"publica"| Q2
    EP2 -->|"cria tarefa"| TASKS_DB

    EP3 -->|"consulta"| REDIS_DB
    EP4 -->|"consulta"| TASKS_DB

    Q1 --> W1
    Q2 --> W2

    W1 --> INGEST_PIPE
    P1 --> P2 --> P3 --> P4 --> P5
    P5 -->|"upsert"| CHROMA
    W1 -->|"atualiza"| TASKS_DB

    W2 --> RAG_PIPE
    R1 --> R2
    R2 -->|"MISS"| R3
    R3 --> R4
    R4 -->|"candidatos"| R5
    R5 --> R6
    R6 --> R7
    R7 --> R8
    R8 -->|"salva"| R9
    R9 -->|"persiste"| REDIS_DB
    W2 -->|"atualiza"| TASKS_DB

    R4 <-->|"busca vetorial"| CHROMA
    R8 <-->|"geração"| G

    Q1 -.->|"falha permanente"| DLQ
    Q2 -.->|"falha permanente"| DLQ

    style MQ fill:#ff6b35,color:#fff
    style WORKERS fill:#2d6a4f,color:#fff
    style GEMINI_SVC fill:#7b2d8b,color:#fff
    style STORAGE fill:#1a4a6b,color:#fff
```

---

## 13. Endpoints da API

### 13.1 Documentos

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `POST` | `/api/v1/document/upload` | 202 | Upload + enfileiramento assíncrono via RabbitMQ |
| `POST` | `/api/v1/document/{id}/process` | 200 | Processamento síncrono manual (fallback) |
| `POST` | `/api/v1/document/reprocess` | 200 | Reindexar todos os documentos em batch |
| `DELETE` | `/api/v1/document/{id}` | 200 | Remover documento (ChromaDB + disco + cache) |

### 13.2 RAG — Perguntas e Respostas

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `POST` | `/api/v1/rag/ask` | 202 | Enviar pergunta (assíncrono via RabbitMQ) |
| `GET` | `/api/v1/rag/result/{request_id}` | 200 | Consultar resposta no Redis |

### 13.3 Tarefas

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `GET` | `/api/v1/tasks/{task_id}` | 200 | Status e progresso da tarefa |

### 13.4 Saúde e Busca

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `GET` | `/api/v1/health` | 200 | Health check da aplicação |
| `GET` | `/api/v1/query` | 200 | Busca semântica direta (sem LLM) |

### 13.5 Exemplos de Uso

#### Upload de Documento

```bash
curl -X POST "http://localhost:8000/api/v1/document/upload" \
  -F "file=@relatorio_anual.pdf"
```

```json
{
  "document_id": "3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c",
  "status": "queued",
  "message": "Documento enviado para processamento"
}
```

#### Acompanhar Progresso

```bash
curl "http://localhost:8000/api/v1/tasks/3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c"
```

```json
{
  "task_id": "3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c",
  "document_id": "3f8a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c",
  "filename": "relatorio_anual.pdf",
  "status": "COMPLETED",
  "progress": 100,
  "message": "Processamento concluído com sucesso. 158 chunks indexados no ChromaDB.",
  "created_at": "2026-06-11T21:38:05",
  "updated_at": "2026-06-11T21:38:47"
}
```

#### Fazer uma Pergunta (RAG)

```bash
curl -X POST "http://localhost:8000/api/v1/rag/ask" \
  -H "Content-Type: application/json" \
  -d '{"question": "Qual o objetivo do projeto?", "limit": 10}'
```

```json
{
  "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "PROCESSING",
  "timestamp": "2026-06-11T21:38:05.123456"
}
```

#### Obter Resposta RAG

```bash
curl "http://localhost:8000/api/v1/rag/result/a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

```json
{
  "status": "COMPLETED",
  "request_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "answer": "O projeto DocMind é uma plataforma corporativa de NLP...",
  "sources": [
    {
      "chunk_id": "3f8a2b1c_chunk_5",
      "filename": "relatorio_anual.pdf",
      "excerpt": "O objetivo principal do DocMind é...",
      "similarity": 0.8923
    }
  ]
}
```

---

## 14. Variáveis de Ambiente

```env
# Aplicação
APP_NAME=Plataforma NLP RAG Enterprise
APP_ENV=development
DEBUG=True
HOST=0.0.0.0
PORT=8000

# RabbitMQ
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_QUEUE=document_processing_queue
RABBITMQ_EXCHANGE=document_exchange
RABBITMQ_ROUTING_KEY=document.process
RABBITMQ_RAG_QUEUE=rag_requests
RABBITMQ_DOCUMENT_QUEUE=document_processing

# Redis
REDIS_URL=redis://localhost:6379/0
CACHE_TTL_SECONDS=3600

# ChromaDB
CHROMADB_PATH=./data/chromadb
UPLOAD_DIR=./data/uploads
MAX_FILE_SIZE_MB=10

# Embeddings
EMBEDDING_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2
CHUNK_SIZE=1000
CHUNK_OVERLAP=150

# Google Gemini
GOOGLE_API_KEY=sua-api-key-aqui
GEMINI_MODEL=gemini-2.5-flash
LLM_TEMPERATURE=0.2
LLM_MAX_TOKENS=2048

# RAG Pipeline
DEFAULT_CONTEXT_CHUNKS=10
RAG_MIN_SIMILARITY=0.25
EXCERPT_LENGTH=400
```

---

## 15. Execução e Operação

### 15.1 Desenvolvimento Local

```bash
# 1. Ativar ambiente virtual
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Linux/Mac

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Iniciar RabbitMQ e Redis via Docker
docker run -d --name docmind-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --name docmind-redis -p 6379:6379 redis:7

# 4. Iniciar a API FastAPI (Terminal 1)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 5. Iniciar o Worker (Terminal 2)
python -m workers.document_worker
```

### 15.2 Docker Compose (Todos os Serviços)

```bash
# Subir todos os serviços (RabbitMQ + API + Worker)
docker-compose up -d

# Escalar workers horizontalmente
docker-compose up -d --scale worker=3

# Acompanhar logs em tempo real
docker-compose logs -f

# Parar e remover containers
docker-compose down
```

### 15.3 Serviços Disponíveis

| Serviço | URL | Descrição |
|---|---|---|
| API FastAPI | `http://localhost:8000` | Aplicação principal |
| Swagger UI | `http://localhost:8000/docs` | Documentação interativa da API |
| ReDoc | `http://localhost:8000/redoc` | Documentação alternativa |
| RabbitMQ Management | `http://localhost:15672` | Monitoramento de filas (guest/guest) |

### 15.4 Testes

```bash
# Executar todos os testes
pytest tests/ -v

# Testes assíncronos com asyncio
pytest tests/ -v --asyncio-mode=auto

# Teste específico
pytest tests/test_filter_propagation.py -v
```

---

## 16. Estrutura de Arquivos

```
DocMind/
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── endpoints/
│   │       │   ├── document.py       # Upload, process, reprocess, delete
│   │       │   ├── rag.py            # /rag/ask e /rag/result/{id}
│   │       │   ├── tasks.py          # GET /tasks/{task_id}
│   │       │   ├── query.py          # Busca semântica direta
│   │       │   └── health.py         # Health check
│   │       └── router.py             # Agrega todos os endpoints v1
│   ├── core/
│   │   ├── config.py                 # Settings via pydantic-settings
│   │   └── logging.py                # Loguru + InterceptHandler
│   ├── schemas/
│   │   ├── document.py               # DocumentUploadResponse, DocumentMetadata
│   │   ├── rag.py                    # RAGRequest, RAGResponse, SourceReference
│   │   ├── semantic.py               # Chunk, SemanticProcessResponse
│   │   └── task.py                   # TaskStatus, TaskCreate, TaskResponse
│   ├── services/
│   │   ├── rabbitmq_service.py       # RabbitMQService + process_rag_request callback
│   │   ├── rag_service.py            # RAGService — pipeline RAG completo (13 etapas)
│   │   ├── cache_service.py          # CacheService Redis (SHA-256 key)
│   │   ├── vector_store.py           # VectorStoreService ChromaDB
│   │   ├── embedding_service.py      # EmbeddingService HuggingFace + fallback
│   │   ├── semantic_processor.py     # SemanticProcessorService (chunking + embeddings)
│   │   ├── document_processor.py     # Extração PDF/MD + limpeza de texto
│   │   ├── task_service.py           # TaskService (tasks.json, thread-safe)
│   │   └── reprocess_cli.py          # CLI de reprocessamento em lote
│   └── main.py                       # FastAPI app + Lifespan (RabbitMQ connect + RAG consumer)
├── workers/
│   └── document_worker.py            # DocumentWorker — processo standalone assíncrono
├── tests/                            # Testes unitários e de integração
├── data/
│   ├── chromadb/                     # Banco vetorial persistente
│   ├── uploads/                      # Arquivos enviados pelos usuários
│   └── tasks.json                    # Rastreamento de tarefas
├── docker-compose.yml                # RabbitMQ + API + Worker
├── requirements.txt                  # Dependências Python
├── .env                              # Variáveis de ambiente (não versionado)
└── .env.example                      # Template de configuração
```

---

## 17. Conclusão

O DocMind representa uma implementação madura e completa de uma **plataforma corporativa de NLP** orientada a eventos, combinando as melhores práticas de arquitetura de software moderno:

### Event-Driven Architecture (EDA)
A adoção do **RabbitMQ como broker central** desacopla completamente a camada de recepção HTTP do processamento computacional intensivo. O uso de `aio-pika` com `asyncio` garante consumo não-bloqueante com alta eficiência de I/O. A topologia com **Dead Letter Queue (DLQ)** e **retry automático** (até 3 tentativas via headers) proporciona resiliência real em cenários de falha.

### RAG Corporativo
O **pipeline RAG de 13 etapas** vai além de uma implementação ingênua de "busca + LLM". O re-ranking híbrido (70% cosine + 30% token overlap), a detecção automática de tipo de query (`is_cv_query`), o threshold dinâmico adaptativo e o controle de janela de contexto (25.000 chars) resultam em respostas de alta qualidade contextual. A evolução crítica de enviar **todos os chunks relevantes** ao LLM (e não apenas os primeiros) eliminou um gargalo significativo de qualidade.

### Banco Vetorial (ChromaDB)
O ChromaDB com índice **HNSW cosine** permite recuperação semântica eficiente em documentos de qualquer tamanho. O modelo `all-MiniLM-L6-v2` (384 dimensões) oferece excelente trade-off entre qualidade semântica e performance. O fallback determinístico (SHA-256) garante operação offline para desenvolvimento e CI/CD.

### Cache Redis Semântico
O `CacheService` com normalização SHA-256 implementa **deduplicação semântica** transparente: perguntas ligeiramente reformuladas (maiúsculas, pontuação, espaços extras) são mapeadas para a mesma chave de cache, maximizando o hit rate e minimizando o custo de tokens. A **degradação graciosa** garante disponibilidade mesmo sem Redis.

### Observabilidade Aplicada ao Pipeline NLP
O sistema emite logs estruturados em cada etapa crítica do pipeline, incluindo scores de similaridade, tempo de execução por componente, ratio de cache HIT/MISS e tokens estimados. Essas métricas habilitam práticas de **LLMOps** — monitoramento contínuo da qualidade de retrieval, custo operacional e performance do sistema.

### Escalabilidade
A arquitetura suporta escalabilidade em múltiplas dimensões:
- **Horizontal de workers**: `docker-compose up --scale worker=N`
- **Escalabilidade do LLM**: substituição do Gemini por qualquer modelo LangChain sem alterar o pipeline
- **Escalabilidade de armazenamento**: ChromaDB suporta milhões de vetores com HNSW
- **Escalabilidade de cache**: Redis Cluster para ambientes de alta disponibilidade

---

> **Documentação gerada em:** Junho de 2026
> **Versão do código analisado:** Branch principal — DocMind v1.0.0
> **Autor da análise:** Arquitetura verificada contra código-fonte em `c:\ADS\ADS - Semestre06\DocMind`

---

### Notas de Divergência e Consistência

> [!NOTE]
> A seguir, divergências e pontos de atenção identificados entre o código atual e documentações anteriores:

1. **`task_id` vs `document_id`**: Na implementação atual, `task_id == document_id` para tarefas de ingestão (acoplamento explícito no código). Documentações anteriores sugeriam campos separados — o código atual os unifica por design.

2. **Filas RabbitMQ**: O config possui tanto `RABBITMQ_DOCUMENT_QUEUE=document_processing` quanto `RABBITMQ_QUEUE=document_processing_queue`. O Worker e o Docker Compose utilizam `RABBITMQ_QUEUE` (`document_processing_queue`). O nome `document_processing` em `RABBITMQ_DOCUMENT_QUEUE` não é utilizado como fila principal.

3. **Consumer RAG**: A versão anterior documentava apenas o `DocumentWorker`. Na implementação atual, o **consumer da fila `rag_requests` roda dentro da API FastAPI** (Lifespan), não em processo separado — comportamento documentado corretamente nesta versão.

4. **Re-ranking Híbrido**: Feature não documentada em versões anteriores — adicionada nesta documentação com base no código implementado em `rag_service.py`.

5. **Redis na ingestão**: O Lifespan da API (`main.py`) não inicializa Redis — apenas RabbitMQ e TaskService. O Redis é utilizado exclusivamente no pipeline RAG (`rag_service.py` e `rabbitmq_service.py/process_rag_request`).
