# DocMind — Plataforma NLP RAG Enterprise

> **Plataforma Corporativa de Processamento de Linguagem Natural com RAG e Mensageria Assíncrona**

[![Python](https://img.shields.io/badge/Python-3.10-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.111+-green)](https://fastapi.tiangolo.com)
[![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3--management-orange)](https://rabbitmq.com)
[![Redis](https://img.shields.io/badge/Redis-7--alpine-red)](https://redis.io)
[![ChromaDB](https://img.shields.io/badge/ChromaDB-0.5+-purple)](https://trychroma.com)
[![Gemini](https://img.shields.io/badge/Gemini-2.5--flash-yellow)](https://ai.google.dev)

---

## Visão Geral

### Objetivo do Sistema

O **DocMind** é uma plataforma de **Processamento de Linguagem Natural (NLP)** com arquitetura **RAG (Retrieval-Augmented Generation)** orientada a eventos. O sistema permite que usuários corporativos façam upload de documentos (PDF e Markdown), processem-nos semanticamente e façam perguntas em linguagem natural, recebendo respostas geradas pelo **Google Gemini** fundamentadas exclusivamente no conteúdo dos documentos ingeridos.

### Problema Resolvido

Empresas acumulam grandes volumes de documentos técnicos, currículos, manuais, relatórios e contratos. Encontrar informações específicas nesse volume é lento e impreciso. O DocMind resolve isso ao:

- **Indexar documentos automaticamente** em um banco vetorial (ChromaDB) via pipeline assíncrono
- **Responder perguntas em linguagem natural** com base no conteúdo real dos documentos
- **Citar as fontes** dos trechos utilizados em cada resposta
- **Cachear respostas** para consultas frequentes, reduzindo latência e custo de LLM
- **Escalar horizontalmente** via Docker e workers assíncronos

---

## Arquitetura Geral

```
┌─────────────┐    HTTP     ┌──────────────┐    AMQP     ┌──────────────────┐
│   Cliente   │ ──────────► │  FastAPI API  │ ──────────► │  RabbitMQ Broker │
└─────────────┘             └──────┬───────┘             └────────┬─────────┘
                                   │                               │
                            ┌──────▼───────┐             ┌────────▼─────────┐
                            │  Redis Cache │             │  Document Worker  │
                            └─────────────┘             └────────┬─────────┘
                                                                  │
                                                         ┌────────▼─────────┐
                                                         │    ChromaDB       │
                                                         │  (Banco Vetorial) │
                                                         └──────────────────┘
                                                                  │
                                                         ┌────────▼─────────┐
                                                         │  Google Gemini   │
                                                         │  (LLM / Geração) │
                                                         └──────────────────┘
```

### Componentes Principais

| Componente | Tecnologia | Papel |
|---|---|---|
| API REST | FastAPI 0.111+ | Interface HTTP, orquestração de fluxos |
| Broker de Mensagens | RabbitMQ 3-management | Desacoplamento assíncrono de tarefas |
| Cache | Redis 7-alpine | Cache de respostas RAG com TTL |
| Banco Vetorial | ChromaDB 0.5+ | Persistência e busca vetorial por similaridade de cosseno |
| LLM | Google Gemini 2.5-flash | Geração de respostas a partir do contexto recuperado |
| Embeddings | sentence-transformers/all-MiniLM-L6-v2 | Vetorização de textos e queries (384 dimensões) |
| Worker | Python asyncio + aio-pika | Consumidor assíncrono das filas RabbitMQ |

---

## Funcionalidades Implementadas

### Upload e Ingestão de Documentos
- Upload de arquivos **PDF** e **Markdown** (`.pdf`, `.md`)
- Validação de tipo de arquivo e tamanho máximo (configurável, padrão 10 MB)
- Geração de `document_id` único (UUID v4)
- Salvamento físico em `data/uploads/` com prefixo `{document_id}_{filename}`
- Publicação assíncrona na fila `document_processing_queue` via RabbitMQ
- Criação de tarefa de rastreamento no `TaskService` com status `QUEUED`

### Processamento Semântico Assíncrono (Worker)
- Consumo da fila `document_processing_queue` pelo `DocumentWorker`
- Extração de texto de PDF via `pypdf`
- Leitura direta de arquivos Markdown
- Limpeza de texto (normalização de espaços e quebras de linha)
- **Chunking semântico** via `RecursiveCharacterTextSplitter` (LangChain):
  - `CHUNK_SIZE`: 500 (configurável via `.env`)
  - `CHUNK_OVERLAP`: 50 (configurável via `.env`)
  - Separadores: `\n\n`, `\n`, ` `, ``
- Geração de embeddings em lote (modelo HuggingFace local ou fallback determinístico offline)
- Persistência no ChromaDB via `upsert` com metadados por chunk:
  - `source_doc_id`, `filename`, `chunk_index`, `char_count`, `total_chunks`, `uploaded_at`
- Atualização de progresso por etapas: QUEUED → PROCESSING (10%, 30%, 60%, 80%) → COMPLETED (100%)
- Retry automático (até 3 tentativas) com cabeçalho `x-retry-count`
- Envio para **Dead Letter Queue (DLQ)** após esgotamento de tentativas

### Pipeline RAG (Retrieval-Augmented Generation)

O pipeline RAG é acionado via endpoint `/api/v1/rag/ask` e executado de forma assíncrona:

1. **Recepção da pergunta** → geração de `task_id` + `request_id`
2. **Publicação na fila** `rag_requests` via RabbitMQ
3. **Consumer** (`process_rag_request`) processa a mensagem:
   - Detecção de tipo de pergunta (**CV/carreira** vs. **geral**)
   - Expansão de query para perguntas de currículo/carreira
   - **Consulta ao Redis** (cache HIT retorna imediatamente)
   - Geração de embedding da query (384 dimensões)
   - **Busca vetorial no ChromaDB** com limite ampliado de candidatos (5× o limite final)
   - **Remoção de duplicados** por normalização de texto
   - **Re-ranking híbrido**: 70% similaridade de cosseno + 30% token overlap ratio
   - **Keyword boost** para perguntas de currículo (+0.1 por palavra-chave até +0.3)
   - **Threshold dinâmico adaptativo**:
     - CV: `max_score * 0.50` (garante mínimo 10 chunks)
     - Geral (score ≥ baseline): `max(max_score * 0.70, RAG_MIN_SIMILARITY)`
     - Geral (score < baseline): `max_score * 0.70`
   - **Fallback Top-K**: 3 melhores chunks caso nenhum passe no threshold
   - **Controle de janela de contexto**: limite de 25.000 caracteres totais
   - Chamada ao **Google Gemini** via LangChain chain (Prompt → LLM → StrOutputParser)
   - Fallback sem LLM (retorna contexto bruto quando `GOOGLE_API_KEY` não configurada)
   - **Persistência do resultado no Redis** com chave `rag_response:{request_id}` (TTL: 3600s)
   - Atualização do `TaskService` para `COMPLETED` com resultado

### Cache Redis
- Chave determinística via **SHA-256** de `(question_normalizada | limit | document_id)`
- Normalização inclui: strip, lowercase, remoção de pontuação final, colapso de espaços
- `GET`/`SET`/`DELETE`/`EXISTS`/`FLUSHDB` via `redis.asyncio`
- Degradação graciosa (no-op) quando Redis indisponível
- **Cache invalidado** automaticamente no reprocessamento e exclusão de documentos

### Rastreamento de Tarefas
- Persistência de estado em `data/tasks.json` (thread-safe via `threading.Lock`)
- Estados: `QUEUED` → `PROCESSING` → `COMPLETED` | `FAILED`
- Campos: `task_id`, `document_id`, `filename`, `status`, `progress` (0-100%), `message`, `created_at`, `updated_at`, `error_detail`, `result`
- Compartilhado entre API e Workers via arquivo JSON

### Reprocessamento em Lote
- Endpoint `POST /api/v1/document/reprocess` varre `data/uploads/`
- Reextrai, rechunka e reindexa todos os documentos existentes
- Invalida o cache Redis ao final (`FLUSHDB`)
- Script CLI equivalente: `python -m app.services.reprocess_cli`

### Exclusão de Documentos
- `DELETE /api/v1/document/{document_id}`
- Remove todos os chunks do ChromaDB (`where source_doc_id = document_id`)
- Remove o arquivo físico de `data/uploads/`
- Invalida o cache Redis (`FLUSHDB`)
- Retorna contagem de chunks removidos e status de remoção do arquivo

---

## Arquitetura RAG

### Fluxo Completo de Recuperação

```
Pergunta do Usuário
       │
       ▼
[Normalização + Detecção de Tipo]
  is_cv_query() — keywords: experiência, currículo, empresa, cargo...
       │
       ▼
[Consulta ao Redis Cache]
  Chave: SHA-256(question|limit|doc_id)
  ├── HIT  ──► Resposta imediata (latência ~1ms)
  └── MISS ──► Continua pipeline
       │
       ▼
[Geração de Embedding]
  Modelo: sentence-transformers/all-MiniLM-L6-v2 (384 dims)
  Expansão de query para CV queries
  Fallback determinístico offline (hash-based)
       │
       ▼
[Busca Vetorial ChromaDB]
  n_results = max(limit × 5, 25) — até 50 para CV
  where_filter: {source_doc_id: filter_document_id} (opcional)
  Métrica: Cosine Similarity (hnsw:space=cosine)
       │
       ▼
[Remoção de Duplicados]
  Normalização por texto único (strip + lower)
       │
       ▼
[Re-Ranking Híbrido]
  Score = max(cosine_sim, 0.7×cosine + 0.3×token_overlap)
  Keyword boost para CV: +0.1 por keyword (máx +0.3)
  Ordenação decrescente por hybrid_score
       │
       ▼
[Filtragem por Threshold Dinâmico]
  Limiar adaptativo baseado no max_hybrid_score
  Fallback Top-K (3 chunks) se nenhum passar
  Garantia de mínimo 10 chunks para CV queries
       │
       ▼
[Controle de Janela de Contexto]
  Limite: 25.000 caracteres
  Chunks descartados se excederem o limite
       │
       ▼
[Chamada ao Gemini]
  Modelo: gemini-2.5-flash
  Temperatura: 0.2
  Max tokens: 2048
  Prompt corporativo em Português com regras obrigatórias
  Fallback: contexto bruto se sem API key
       │
       ▼
[Persistência no Redis + Resposta]
  TTL: 3600 segundos (1 hora)
```

### Embeddings
- **Modelo**: `sentence-transformers/all-MiniLM-L6-v2`
- **Dimensão**: 384
- **Normalização**: L2 (embeddings normalizados para consistência de cosseno)
- **Processamento em lote**: `embed_documents(texts)` para indexação
- **Fallback offline**: embedding determinístico via SHA-256 + seed RNG (sem internet/GPU)

### Chunking
- **Estratégia**: `RecursiveCharacterTextSplitter` (LangChain)
- **CHUNK_SIZE**: 500 chars (padrão `.env`, configurável)
- **CHUNK_OVERLAP**: 50 chars (padrão `.env`, configurável)
- **Separadores**: `\n\n`, `\n`, ` `, `""`
- **Metadados por chunk**: `source_doc_id`, `filename`, `chunk_index`, `char_count`, `total_chunks`, `uploaded_at`

### Re-Ranking Híbrido
O pipeline aplica um re-ranking em duas etapas após a recuperação inicial do ChromaDB:

1. **Score Híbrido**: combina similaridade vetorial com sobreposição de tokens da query
2. **Threshold Dinâmico**: limiar adaptativo baseado no melhor score encontrado

**Stop Words**: conjunto bilíngue (PT-BR + EN) para filtrar tokens irrelevantes no cálculo de overlap.

---

## Observabilidade

Todos os logs são emitidos via **Loguru** com interceptação de logs do Uvicorn/FastAPI.

### Formato de Log
```
2026-06-14 14:11:20.236 | INFO     | module:function:line - Mensagem
```
- **Desenvolvimento**: colorido, formato legível
- **Produção**: JSON estruturado serializado (`serialize=True`)

### Logs de Recuperação RAG
O pipeline emite logs detalhados em cada etapa:
```
[RETRIEVAL] Iniciando busca vetorial... Limite de candidatos: 50
===== CHUNKS RECUPERADOS DO CHROMADB =====
[1] Chunk ID: ... | Cosine Similarity: 0.82 | Arquivo: ... | Texto: ...
[RETRIEVAL] Remoção de duplicados: 50 -> 45 chunks únicos
===== CHUNKS ANTES DO RE-RANKING =====
chunk_id=... | similarity=0.82 | overlap=0.45 | hybrid_score=0.71
===== CHUNKS APÓS O RE-RANKING =====
[RETRIEVAL] Score máx: 0.82 | Threshold: 0.57 | Passaram: 12/45 chunks
===== CHUNKS DESCARTADOS =====
===== CHUNKS ENVIADOS AO GEMINI =====
```

### Log de Observabilidade (Resumo RAG)
```
[OBSERVABILIDADE] Resumo RAG Pipeline:
- Question: ...
- Limit: 10
- Document ID: None
- Chunks Recuperados (ChromaDB): 45
- Chunks Filtrados (Threshold): 12
- Threshold Final: 0.5740
- Scores: [0.82, 0.79, ...]
- Tempo Execucao (Total): 1243.56ms
- Cache: MISS
```

### Métricas de Cache
```
[REDIS] Cache HIT  → chave: 3f4a1b2c...
[REDIS] Cache MISS → chave: 3f4a1b2c...
[REDIS] Salvando resposta → chave: 3f4a1b2c... | TTL: 3600s
```

### Métricas de Tempo
- `vector_search_ms`: tempo da busca vetorial no ChromaDB
- `llm_ms`: tempo da chamada ao Gemini
- `latency_ms`: tempo total do pipeline RAG
- `elapsed_ms` no cache hit: latência de recuperação do Redis

---

## Estrutura de Pastas

```
DocMind/
├── app/
│   ├── __init__.py
│   ├── main.py                        # Entrypoint FastAPI + lifespan (RabbitMQ consumer)
│   ├── api/
│   │   ├── __init__.py
│   │   ├── router.py                  # Roteador raiz (/api/v1)
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── router.py              # Inclusão de todos os routers v1
│   │       └── endpoints/
│   │           ├── __init__.py
│   │           ├── document.py        # Upload, process, reprocess, delete
│   │           ├── health.py          # Health check
│   │           ├── query.py           # Busca semântica direta
│   │           ├── rag.py             # RAG ask + result
│   │           └── tasks.py           # Consulta de status de tarefas
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py                  # Settings (pydantic-settings, .env)
│   │   └── logging.py                 # Loguru + InterceptHandler
│   ├── schemas/
│   │   ├── __init__.py
│   │   ├── document.py                # DocumentMetadata, UploadResponse, etc.
│   │   ├── health.py                  # HealthResponse, ServiceStatus
│   │   ├── rag.py                     # RAGRequest, RAGResponse, RAGAskResponse, etc.
│   │   ├── search.py                  # SearchRequest, SearchResponse, SearchResultItem
│   │   ├── semantic.py                # Chunk, SemanticProcessResponse
│   │   └── task.py                    # TaskStatus, TaskCreate, TaskResponse
│   ├── services/
│   │   ├── __init__.py
│   │   ├── cache_service.py           # Redis async (get, set, delete, exists, clear)
│   │   ├── document_processor.py     # Extração PDF/MD, limpeza de texto
│   │   ├── embedding_service.py      # HuggingFace embeddings + fallback offline
│   │   ├── rabbitmq_service.py       # aio-pika: connect, publish, consume, DLQ
│   │   ├── rag_service.py            # Pipeline RAG completo (re-ranking, Gemini)
│   │   ├── reprocess_cli.py          # CLI standalone de reprocessamento em lote
│   │   ├── semantic_processor.py     # Chunking (LangChain) + embedding em lote
│   │   ├── task_service.py           # Rastreamento de tarefas via JSON (thread-safe)
│   │   └── vector_store.py           # ChromaDB: upsert, query, delete, stats
│   └── workers/
│       └── __init__.py
├── workers/
│   └── document_worker.py            # Worker standalone (asyncio + aio-pika)
├── tests/
│   ├── __init__.py
│   ├── conftest.py                    # Fixture TestClient
│   ├── test_cv_recall.py             # Testes de recall para perguntas de currículo
│   ├── test_document.py              # Testes de upload, processamento e exclusão
│   ├── test_filter_propagation.py   # Testes de propagação do filtro de documento
│   ├── test_health.py               # Testes do endpoint de health
│   ├── test_rag.py                   # Testes do pipeline RAG (unitários + integração)
│   └── test_vector_store.py         # Testes do ChromaDB VectorStore
├── data/
│   ├── chromadb/                     # Persistência ChromaDB (gerado em runtime)
│   ├── uploads/                      # Arquivos físicos enviados (gerado em runtime)
│   └── tasks.json                    # Estado das tarefas (gerado em runtime)
├── .env                              # Variáveis de ambiente (desenvolvimento)
├── .env.example                      # Template de variáveis de ambiente
├── .gitignore
├── .dockerignore
├── Dockerfile                        # Imagem base Python 3.10-slim
├── docker-compose.yml               # Orquestração local: api, worker, rabbitmq, redis
├── requirements.txt
├── README.md
└── README_EVENT_DRIVEN.md
```

---

## Instalação

### Pré-requisitos
- Python 3.10+
- Docker e Docker Compose (para ambiente containerizado)
- Conta Google AI Studio com API Key (para respostas via Gemini)

### Ambiente Local (sem Docker)

#### 1. Clone e configure o ambiente virtual
```bash
git clone <repo-url>
cd DocMind
python -m venv venv
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate
```

#### 2. Instale as dependências
```bash
pip install -r requirements.txt
```

#### 3. Configure as variáveis de ambiente
```bash
cp .env.example .env
# Edite o .env e configure:
# GOOGLE_API_KEY=sua-chave-real-aqui
# RABBITMQ_URL=amqp://guest:guest@localhost:5672/
# REDIS_URL=redis://localhost:6379/0
```

#### 4. Inicie RabbitMQ e Redis via Docker (separadamente)
```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

---

## Execução

### API (FastAPI)
```bash
uvicorn app.main:app --reload
# Acesse: http://localhost:8000/docs
```

A API inicializa automaticamente o consumer da fila `rag_requests` no startup (via `lifespan`).

### Worker de Documentos (standalone)
```bash
python -m workers.document_worker
```

O worker consome duas filas simultaneamente:
- `document_processing_queue` — processamento de documentos
- `rag_requests` — pipeline RAG assíncrono

### Reprocessamento em Lote (CLI)
```bash
python -m app.services.reprocess_cli
```

### Docker Compose (Ambiente Completo)
```bash
# Iniciar todos os serviços
docker-compose up -d

# Acompanhar logs
docker-compose logs -f

# Parar todos os serviços
docker-compose down
```

Serviços no Docker Compose:
| Serviço | Porta | Descrição |
|---|---|---|
| `api` | 8000 | FastAPI Application |
| `worker` | — | Document Worker (sem porta exposta) |
| `rabbitmq` | 5672 / 15672 | AMQP + Management UI |
| `redis` | 6379 | Cache |

### Gerenciamento via RabbitMQ Management UI
```
URL: http://localhost:15672
User: guest
Password: guest
```

---

## Testes

### Como Executar
```bash
# Todos os testes
pytest tests/ -v

# Suite específica
pytest tests/test_rag.py -v
pytest tests/test_document.py -v
pytest tests/test_health.py -v
pytest tests/test_vector_store.py -v
pytest tests/test_filter_propagation.py -v
pytest tests/test_cv_recall.py -v
```

### Estrutura da Suíte

| Arquivo | Cobertura |
|---|---|
| `conftest.py` | Fixture `TestClient` (escopo de módulo) |
| `test_health.py` | Endpoint GET `/health` |
| `test_document.py` | Upload, processamento semântico, exclusão, reprocessamento |
| `test_rag.py` | Pipeline RAG unitário e integração via API |
| `test_vector_store.py` | ChromaDB upsert, busca, deleção |
| `test_filter_propagation.py` | Propagação correta do `filter_document_id` |
| `test_cv_recall.py` | Recall e completude para perguntas de currículo/carreira |

### Casos de Teste do Pipeline RAG (`test_rag.py`)
- Fallback sem contexto (banco vetorial vazio)
- Fallback sem LLM (retorna contexto bruto)
- Filtro por similaridade mínima
- Pipeline completo com LLM mockado
- Validação de payload incompleto (422)
- Validação de pergunta muito curta (422)
- Threshold dinâmico e filtro de `filter_document_id`
- Novas features: threshold adaptativo, fallback Top-K, busca com/sem filtro
- Detecção de query de resumo (`is_summary_query`)
- Filtro de diversidade Jaccard
- Controle de limite de tokens (MAX_CONTEXT_TOKENS)

---

## Endpoints

### Health

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health` | Status da API e componentes de infraestrutura |

**Response `200`**:
```json
{
  "status": "ok",
  "environment": "development",
  "version": "1.0.0",
  "services": {
    "chromadb": {"status": "healthy", "latency_ms": 0.5},
    "redis": {"status": "not_connected"},
    "rabbitmq": {"status": "not_connected"}
  }
}
```

---

### Documentos

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `POST` | `/api/v1/document/upload` | 202 | Upload assíncrono de PDF ou Markdown |
| `POST` | `/api/v1/document/{document_id}/process` | 200 | Processamento semântico síncrono (fallback manual) |
| `POST` | `/api/v1/document/reprocess` | 200 | Reprocessar e reindexar todos os documentos |
| `DELETE` | `/api/v1/document/{document_id}` | 200 | Excluir documento (ChromaDB + arquivo + cache) |

**POST `/api/v1/document/upload`**
- Body: `multipart/form-data` com campo `file` (`.pdf` ou `.md`)
- Limite: `MAX_FILE_SIZE_MB` (padrão 10 MB)
- Response `202`:
```json
{
  "document_id": "uuid-v4",
  "status": "queued",
  "message": "Documento enviado para processamento"
}
```

**DELETE `/api/v1/document/{document_id}`**
- Response `200`:
```json
{
  "success": true,
  "document_id": "uuid-v4",
  "chunks_removed": 24,
  "file_removed": true,
  "message": "Documento removido com sucesso."
}
```

---

### RAG

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `POST` | `/api/v1/rag/ask` | 202 | Enfileira pergunta RAG e retorna `task_id`/`request_id` |
| `GET` | `/api/v1/rag/result/{request_id}` | 200 | Consulta resultado no Redis pelo `request_id` |

**POST `/api/v1/rag/ask`**
```json
// Request
{
  "question": "Quais são as experiências profissionais do candidato?",
  "limit": 10,
  "filter_document_id": "uuid-opcional"
}
// Response 202
{
  "task_id": "uuid-v4",
  "request_id": "uuid-v4",
  "status": "PROCESSING",
  "timestamp": "2026-06-14T17:00:00.000000"
}
```

**GET `/api/v1/rag/result/{request_id}`**
```json
// PROCESSING (ainda em fila)
{ "status": "PROCESSING" }

// COMPLETED
{
  "status": "COMPLETED",
  "request_id": "uuid-v4",
  "answer": "Resposta gerada pelo Gemini...",
  "sources": [
    {
      "chunk_id": "doc-id_chunk_0",
      "filename": "curriculo.pdf",
      "excerpt": "Trecho do documento...",
      "similarity": 0.8734
    }
  ]
}
```

---

### Busca Semântica

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `POST` | `/api/v1/query/search` | 200 | Busca semântica direta no ChromaDB (sem LLM) |

**POST `/api/v1/query/search`**
```json
// Request
{
  "query": "experiência com Python e FastAPI",
  "limit": 5,
  "filter_document_id": "uuid-opcional"
}
// Response 200
{
  "query": "experiência com Python e FastAPI",
  "total_results": 3,
  "results": [
    {
      "chunk_id": "doc-id_chunk_3",
      "text": "Trecho do chunk...",
      "similarity": 0.8234,
      "metadata": {"filename": "...", "chunk_index": 3}
    }
  ]
}
```

---

### Tarefas

| Método | Rota | Status | Descrição |
|---|---|---|---|
| `GET` | `/api/v1/tasks/{task_id}` | 200 | Consulta status e progresso de tarefa |

**GET `/api/v1/tasks/{task_id}`**
```json
{
  "task_id": "uuid-v4",
  "document_id": "uuid-v4",
  "filename": "documento.pdf",
  "status": "PROCESSING",
  "progress": 60,
  "message": "Executando segmentação semântica (chunking)...",
  "created_at": "2026-06-14T17:00:00",
  "updated_at": "2026-06-14T17:00:05",
  "error_detail": null,
  "result": null
}
```

**Status possíveis**:
| Status | Progress | Significado |
|---|---|---|
| `QUEUED` | 0% | Mensagem publicada, aguardando worker |
| `PROCESSING` | 10–80% | Worker processando (localizando, extraindo, chunkando, indexando) |
| `COMPLETED` | 100% | Processamento concluído com sucesso |
| `FAILED` | 0% | Falha após esgotamento de retentativas |

---

### Raiz

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/` | Redireciona para `/docs` (Swagger UI) |
| `GET` | `/docs` | Swagger UI interativo |
| `GET` | `/redoc` | ReDoc (documentação alternativa) |

---

## Tecnologias

| Categoria | Tecnologia | Versão | Uso |
|---|---|---|---|
| Framework Web | FastAPI | ≥ 0.111.0 | API REST, routers, middlewares |
| ASGI Server | Uvicorn | ≥ 0.29.0 | Servidor assíncrono de produção |
| Validação | Pydantic v2 | ≥ 2.7.0 | Schemas, validação de dados |
| Configuração | pydantic-settings | ≥ 2.2.1 | `.env` settings com validação |
| Logging | Loguru | ≥ 0.7.2 | Logs estruturados + interceptação |
| Broker | RabbitMQ (aio-pika) | ≥ 9.4.1 | Mensageria assíncrona AMQP |
| Broker (sync) | pika | ≥ 1.3.2 | Dependência sync (não usado diretamente) |
| Cache | redis[asyncio] | ≥ 5.0.4 | Cache de respostas RAG |
| Banco Vetorial | ChromaDB | ≥ 0.5.0 | Persistência e busca vetorial |
| LLM | Google Gemini 2.5-flash | — | Geração de respostas |
| LLM Framework | LangChain | ≥ 0.2.1 | Chain Prompt→LLM→Parser |
| LLM Provider | langchain-google-genai | ≥ 1.0.6 | Integração Gemini |
| LLM SDK | google-generativeai | ≥ 0.5.4 | SDK Google AI |
| Embeddings | sentence-transformers | ≥ 2.6.0 | Modelo all-MiniLM-L6-v2 local |
| Embeddings Framework | langchain-community | ≥ 0.2.1 | HuggingFaceEmbeddings |
| Text Splitting | langchain (LangChain) | ≥ 0.2.1 | RecursiveCharacterTextSplitter |
| PDF | pypdf | ≥ 4.2.0 | Extração de texto de PDF |
| HTTP Client | httpx | ≥ 0.27.0 | Testes e requests HTTP |
| Upload | python-multipart | ≥ 0.0.9 | Suporte `multipart/form-data` |
| Tests | pytest + pytest-asyncio | ≥ 8.1.1 | Suite de testes |
| Auth Libs | PyJWT + passlib[bcrypt] | ≥ 2.8.0 | Presentes em requirements (não ativo na API atual) |
| Container | Docker + Docker Compose | 3.9 | Orquestração de serviços |

---

## Variáveis de Ambiente

Arquivo: `.env` (template em `.env.example`)

| Variável | Padrão | Descrição |
|---|---|---|
| `APP_NAME` | `Plataforma NLP RAG Enterprise` | Nome da aplicação |
| `APP_ENV` | `development` | Ambiente (`development`/`production`) |
| `DEBUG` | `true` | Ativa logs DEBUG e modo desenvolvimento |
| `API_V1_STR` | `/api/v1` | Prefixo da API v1 |
| `SECRET_KEY` | `super-secret-key-change-in-production` | Chave secreta (JWT, futuro) |
| `HOST` | `0.0.0.0` | Host do servidor |
| `PORT` | `8000` | Porta do servidor |
| `BACKEND_CORS_ORIGINS` | `["http://localhost:3000","http://localhost:8000"]` | Origins permitidos no CORS |
| `CHROMADB_PATH` | `./data/chromadb` | Diretório de persistência do ChromaDB |
| `PERSIST_DIRECTORY` | `./data/chromadb` | Alias para CHROMADB_PATH |
| `UPLOAD_DIR` | `./data/uploads` | Diretório de arquivos enviados |
| `MAX_FILE_SIZE_MB` | `10` | Tamanho máximo de arquivo em MB |
| `CHUNK_SIZE` | `500` | Tamanho do chunk em caracteres |
| `CHUNK_OVERLAP` | `50` | Sobreposição entre chunks |
| `EMBEDDING_MODEL_NAME` | `sentence-transformers/all-MiniLM-L6-v2` | Modelo de embeddings |
| `REDIS_URL` | `redis://localhost:6379/0` | URL de conexão Redis |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | URL de conexão RabbitMQ |
| `RABBITMQ_QUEUE` | `document_processing_queue` | Fila principal de documentos |
| `RABBITMQ_DOCUMENT_QUEUE` | `document_processing` | Alias da fila de documentos |
| `RABBITMQ_RAG_QUEUE` | `rag_requests` | Fila de perguntas RAG |
| `RABBITMQ_EXCHANGE` | `document_exchange` | Exchange principal |
| `RABBITMQ_ROUTING_KEY` | `document.process` | Routing key da exchange principal |
| `CACHE_TTL_SECONDS` | `3600` | TTL padrão do cache Redis (1 hora) |
| `GOOGLE_API_KEY` | `""` | API Key do Google AI (Gemini) |
| `GEMINI_MODEL` | `gemini-2.5-flash` | Modelo Gemini utilizado |
| `LLM_TEMPERATURE` | `0.2` | Temperatura do LLM (0=determinístico) |
| `LLM_MAX_TOKENS` | `2048` | Máximo de tokens na resposta do LLM |
| `DEFAULT_CONTEXT_CHUNKS` | `10` | Chunks padrão de contexto RAG |
| `MIN_CONTEXT_CHUNKS` | `3` | Mínimo de chunks de contexto |
| `MAX_CONTEXT_CHUNKS` | `10` | Máximo de chunks de contexto |
| `MIN_CV_CONTEXT_CHUNKS` | `10` | Mínimo de chunks para queries de CV |
| `MAX_CV_CONTEXT_CHUNKS` | `12` | Máximo de chunks para queries de CV |
| `MIN_SUMMARY_CONTEXT_CHUNKS` | `10` | Mínimo de chunks para queries de resumo |
| `MAX_SUMMARY_CONTEXT_CHUNKS` | `12` | Máximo de chunks para queries de resumo |
| `MAX_CONTEXT_TOKENS` | `12000` | Máximo de tokens no contexto enviado ao LLM |
| `RAG_MIN_SIMILARITY` | `0.35` | Similaridade mínima baseline para filtro dinâmico |
| `EXCERPT_LENGTH` | `400` | Tamanho do trecho (excerpt) nas fontes citadas |

---

## Roadmap

Os seguintes itens são mencionados no código como trabalho futuro ou estão parcialmente presentes:

- **Checagem real de saúde** dos serviços no endpoint `/health` (atualmente retorna mocks)
- **Autenticação JWT** — `PyJWT` e `passlib[bcrypt]` estão no `requirements.txt` mas não integrados à API
- **Escalabilidade horizontal de workers** via Docker Swarm (infraestrutura Docker Compose pronta)
- **Configuração Docker Swarm** (`docker stack deploy`) para múltiplas réplicas de worker

---

## RELATÓRIO DE CONSISTÊNCIA

### 1. Funcionalidades Encontradas no Código

| # | Funcionalidade | Arquivo Principal |
|---|---|---|
| 1 | Upload assíncrono PDF/Markdown via RabbitMQ | `document.py` (endpoint) |
| 2 | Processamento semântico síncrono (fallback) | `document.py` (endpoint) |
| 3 | Reprocessamento em lote de todos os documentos | `document.py` (endpoint) |
| 4 | Exclusão de documento (ChromaDB + arquivo + cache) | `document.py` (endpoint) |
| 5 | Busca semântica direta no ChromaDB | `query.py` (endpoint) |
| 6 | RAG assíncrono via RabbitMQ (ask + result) | `rag.py` (endpoint) |
| 7 | Rastreamento de tarefas em JSON thread-safe | `task_service.py` |
| 8 | Pipeline RAG com re-ranking híbrido | `rag_service.py` |
| 9 | Threshold dinâmico adaptativo | `rag_service.py` |
| 10 | Detecção de queries de CV/carreira | `rag_service.py` |
| 11 | Keyword boost para CV queries | `rag_service.py` |
| 12 | Fallback Top-K (3 chunks) | `rag_service.py` |
| 13 | Controle de janela de contexto (25k chars) | `rag_service.py` |
| 14 | Cache Redis com SHA-256 determinístico | `cache_service.py` |
| 15 | Consumer RabbitMQ (aio-pika, DLQ, retry 3×) | `rabbitmq_service.py` + `document_worker.py` |
| 16 | Dead Letter Queue (DLQ) | `rabbitmq_service.py` |
| 17 | ChromaDB com cosine similarity (HNSW) | `vector_store.py` |
| 18 | Embeddings HuggingFace + fallback offline | `embedding_service.py` |
| 19 | Script CLI reprocess | `reprocess_cli.py` |
| 20 | Worker standalone (asyncio, dual queue) | `document_worker.py` |
| 21 | Logs estruturados Loguru (dev/prod) | `logging.py` |
| 22 | CORS middleware configurável | `main.py` |
| 23 | Global exception handler 500 | `main.py` |
| 24 | Consumer RAG no startup do FastAPI (lifespan) | `main.py` |
| 25 | Docker Compose com healthchecks | `docker-compose.yml` |

### 2. Funcionalidades Documentadas nos READMEs Anteriores

Os READMEs anteriores cobriam a maior parte das funcionalidades, porém com diversas imprecisões:
- Mencionavam `RAG_CONTEXT_CHUNKS` (inexistente no código — o correto é `DEFAULT_CONTEXT_CHUNKS`)
- Não documentavam o consumer RAG integrado no lifespan da API
- Não detalhavam o mecanismo de keyword boost
- Não documentavam o script CLI `reprocess_cli.py`
- Endpoints listados de forma incompleta (sem schemas de response)
- Configuração do `GEMINI_MODEL` (gemini-2.5-flash) não documentada

### 3. Funcionalidades Sem Documentação (Encontradas no Código)

| Funcionalidade | Localização |
|---|---|
| Consumer RAG integrado no `lifespan` do FastAPI (além do worker) | `main.py:34-41` |
| Script CLI `reprocess_cli.py` (standalone sem HTTP) | `app/services/reprocess_cli.py` |
| Worker consome **duas filas** simultaneamente (doc + RAG) | `document_worker.py:324-333` |
| Retry automático com header `x-retry-count` (máx 3) | `document_worker.py:209-283` |
| Fallback de embedding determinístico offline (hash-based) | `embedding_service.py:45-65` |
| `EXCERPT_LENGTH` configurável nas fontes citadas | `config.py:76`, `rag_service.py:494` |
| `GEMINI_MODEL=gemini-2.5-flash` (não `gemini-pro`) | `config.py:62` |
| `LLM_MAX_TOKENS=2048` no código vs `1024` no `.env` | divergência .env vs config.py |
| `RAG_MIN_SIMILARITY=0.25` no código vs `0.35` no `.env` | divergência .env vs config.py |

### 4. Divergências Encontradas

| Divergência | Localização |
|---|---|
| `.env` tem `LLM_MAX_TOKENS=1024`; `config.py` tem default `2048` | `.env:35`, `config.py:64` |
| `.env` tem `RAG_MIN_SIMILARITY=0.35`; `config.py` default é `0.25` | `.env:46`, `config.py:75` |
| `.env` não define `RABBITMQ_RAG_QUEUE`; `config.py` tem default `rag_requests` | `.env`, `config.py:54` |
| `.env` não define `GEMINI_MODEL`; `config.py` tem default `gemini-2.5-flash` | `.env`, `config.py:62` |
| `Dockerfile` usa `CMD ["python", "app.py"]` — arquivo inexistente no projeto | `Dockerfile:11` |
| `health.py` retorna status mockado (`mock`) para Redis e RabbitMQ | `health.py:29-36` |
| `rag_service.py` referencia `settings.RAG_CONTEXT_CHUNKS` (linha 196) — variável não existe | `rag_service.py:196` |

### 5. Sugestões de Melhoria na Documentação

1. **Sincronizar `.env` e `config.py`**: alinhar `LLM_MAX_TOKENS`, `RAG_MIN_SIMILARITY`, `GEMINI_MODEL` e `RABBITMQ_RAG_QUEUE` entre os dois arquivos
2. **Corrigir `Dockerfile`**: o `CMD` aponta para `app.py` que não existe; deveria ser `uvicorn app.main:app --host 0.0.0.0 --port 8000`
3. **Implementar health check real**: o endpoint `/health` retorna mocks para Redis e RabbitMQ — deveria verificar conexões reais
4. **Documentar `is_summary_query()`**: função presente no código (`rag_service.py`) e testada em `test_rag.py` mas não utilizada no pipeline atual (detectada nos testes, não no `answer()`)
5. **Adicionar `.env.example` completo**: incluir `RABBITMQ_RAG_QUEUE`, `GEMINI_MODEL`, `RABBITMQ_DOCUMENT_QUEUE`, `EXCERPT_LENGTH`
