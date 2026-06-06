# DocMind — Arquitetura Event-Driven com RabbitMQ

## Visão Geral

O pipeline de ingestão de documentos do DocMind foi transformado em uma **arquitetura orientada a eventos (EDA — Event-Driven Architecture)** utilizando **RabbitMQ** como broker de mensagens.

O processamento síncrono — que bloqueava o cliente HTTP durante chunking + embeddings + ChromaDB — foi substituído por um fluxo **totalmente assíncrono e desacoplado**.

---

## Arquitetura

```
┌──────────────────────────────────────────────────────────────────┐
│                          CLIENTE HTTP                            │
└────────────────────────────┬─────────────────────────────────────┘
                             │ POST /api/v1/document/upload
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                     FastAPI Application                          │
│                                                                  │
│  DocumentProcessor    ← salva arquivo, extrai texto              │
│         │                                                        │
│  TaskService          ← cria tarefa (status: QUEUED)             │
│         │                                                        │
│  RabbitMQService      ← publica na fila document_processing      │
│         │                                                        │
│  Resposta imediata → { task_id, status: "queued" }              │
└──────────────────────────────────────────────────────────────────┘
                             │
                   [RabbitMQ Broker]
                    fila: document_processing
                    durable: true
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                   DocumentWorker (Consumer)                      │
│                                                                  │
│  1. Consome mensagem da fila                                     │
│  2. Localiza arquivo em UPLOAD_DIR                               │
│  3. Extrai e limpa texto (PDF / Markdown)                        │
│  4. Executa chunking semântico (LangChain)                       │
│  5. Gera embeddings vetoriais (HuggingFace / fallback)           │
│  6. Persiste chunks no ChromaDB                                  │
│  7. Atualiza TaskService → status: COMPLETED                     │
└──────────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                         ChromaDB                                 │
│              (Banco Vetorial Persistente)                        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Fluxo Producer → Queue → Consumer

### 1. Producer (FastAPI — `RabbitMQService`)

```
POST /api/v1/document/upload
  → Salva arquivo em data/uploads/
  → Cria tarefa no TaskService (status=QUEUED)
  → Publica JSON na fila document_processing:
      {
        "document_id": "uuid-do-doc",
        "task_id": "uuid-da-task",
        "filename": "meu_documento.pdf",
        "metadata": {}
      }
  → Retorna imediatamente:
      {
        "document_id": "...",
        "task_id": "...",
        "status": "queued",
        "message": "Acompanhe em GET /api/v1/tasks/{task_id}"
      }
```

### 2. Queue (RabbitMQ)

| Configuração | Valor |
|---|---|
| Nome da fila | `document_processing` |
| Durabilidade | `durable=True` (sobrevive a restart) |
| Persistência da msg | `delivery_mode=2` (persistente) |
| Dispatch | `prefetch_count=1` (fair dispatch) |
| ACK | Manual (após processamento concluído) |

### 3. Consumer (Worker — `DocumentWorker`)

O worker opera em processo separado e executa o pipeline com progresso rastreado:

| Etapa | Progresso | Status |
|---|---|---|
| Tarefa criada | 0% | QUEUED |
| Localizando arquivo | 10% | PROCESSING |
| Extraindo texto | 30% | PROCESSING |
| Chunking semântico | 60% | PROCESSING |
| Persistindo ChromaDB | 80% | PROCESSING |
| Finalizado | 100% | COMPLETED |

---

## Como Iniciar o RabbitMQ

### Opção 1 — Docker (recomendado)

```bash
# Subir apenas o RabbitMQ
docker run -d \
  --name docmind-rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management

# Verificar se está rodando
docker ps | grep rabbitmq
```

### Opção 2 — Docker Compose (todos os serviços)

```bash
# Subir RabbitMQ + API + Worker
docker-compose up -d

# Ver logs
docker-compose logs -f

# Parar tudo
docker-compose down
```

### Management UI

Acesse: **http://localhost:15672**

| Campo | Valor |
|---|---|
| Usuário | `guest` |
| Senha | `guest` |

---

## Como Iniciar os Workers

### Desenvolvimento local

```bash
# 1. Ativar ambiente virtual
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Linux/Mac

# 2. Garantir que o RabbitMQ está rodando
# (ver seção acima)

# 3. Iniciar o worker em terminal separado
python -m workers.document_worker
```

### Em produção (Docker Compose)

O worker já está configurado no `docker-compose.yml` e sobe automaticamente.

Para escalar o número de workers:

```bash
docker-compose up -d --scale worker=3
```

---

## Como Testar as Filas

### 1. Subir infraestrutura

```bash
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

### 2. Iniciar o servidor FastAPI

```bash
uvicorn app.main:app --reload
```

### 3. Iniciar o worker

```bash
# Em outro terminal
python -m workers.document_worker
```

### 4. Fazer upload de um documento

```bash
curl -X POST "http://localhost:8000/api/v1/document/upload" \
  -F "file=@meu_documento.pdf"
```

Resposta esperada:
```json
{
  "document_id": "abc-123-...",
  "task_id": "xyz-456-...",
  "status": "queued",
  "message": "Documento enfileirado. Acompanhe em GET /api/v1/tasks/xyz-456-..."
}
```

### 5. Acompanhar o progresso

```bash
curl "http://localhost:8000/api/v1/tasks/xyz-456-..."
```

Resposta (em processamento):
```json
{
  "task_id": "xyz-456-...",
  "document_id": "abc-123-...",
  "filename": "meu_documento.pdf",
  "status": "PROCESSING",
  "progress": 60,
  "message": "Executando segmentação semântica (chunking)...",
  "created_at": "2026-06-06T18:00:00",
  "updated_at": "2026-06-06T18:00:02"
}
```

Resposta (concluído):
```json
{
  "task_id": "xyz-456-...",
  "status": "COMPLETED",
  "progress": 100,
  "message": "Processamento concluído. 158 chunks indexados no ChromaDB."
}
```

### 6. Verificar na Management UI

Acesse **http://localhost:15672** → aba **Queues** → fila `document_processing`.

Você verá as métricas de mensagens publicadas, consumidas e em espera em tempo real.

---

## Novos Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/api/v1/document/upload` | Upload + enfileiramento assíncrono |
| `GET` | `/api/v1/tasks/{task_id}` | Consultar status e progresso da tarefa |
| `POST` | `/api/v1/document/{id}/process` | Processamento síncrono manual (fallback) |

---

## Variáveis de Ambiente

```env
RABBITMQ_URL=amqp://guest:guest@localhost:5672/
RABBITMQ_DOCUMENT_QUEUE=document_processing
RABBITMQ_RAG_QUEUE=rag_requests
```

---

## Arquivos Criados / Modificados

### Novos arquivos
| Arquivo | Responsabilidade |
|---|---|
| `app/schemas/task.py` | Pydantic models: `TaskStatus`, `TaskCreate`, `TaskResponse` |
| `app/services/task_service.py` | CRUD de tarefas com persistência JSON |
| `app/services/message_queue_service.py` | `RabbitMQService` producer com pika |
| `workers/document_worker.py` | Consumer standalone com pipeline completo |
| `app/api/v1/endpoints/tasks.py` | `GET /api/v1/tasks/{task_id}` |
| `docker-compose.yml` | RabbitMQ + API + Worker |

### Arquivos modificados
| Arquivo | Mudança |
|---|---|
| `app/core/config.py` | +`RABBITMQ_DOCUMENT_QUEUE`, `RABBITMQ_RAG_QUEUE` |
| `app/schemas/document.py` | +`task_id` no `DocumentUploadResponse` |
| `app/api/v1/endpoints/document.py` | Upload → fila (async) + mantém `/process` como fallback |
| `app/api/v1/router.py` | +rota `/tasks` |
| `app/main.py` | Lifespan conecta/fecha RabbitMQ |
| `.env.example` | +variáveis de filas RabbitMQ |
