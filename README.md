# 🧠 DocMind — Plataforma NLP RAG Enterprise

> **Plataforma Corporativa de Processamento de Linguagem Natural com RAG e Agentes de IA**

DocMind é uma API REST robusta construída com **FastAPI** que implementa um pipeline completo de **RAG (Retrieval-Augmented Generation)** — combinando recuperação semântica de documentos com geração de respostas via LLM (Google Gemini). Ideal para cenários corporativos onde é necessário extrair inteligência de grandes volumes de documentos internos.

---

## 📋 Índice

- [Visão Geral](#-visão-geral)
- [Arquitetura](#-arquitetura)
- [Funcionalidades](#-funcionalidades)
- [Tecnologias](#-tecnologias)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Configuração](#-configuração)
- [Executando o Projeto](#-executando-o-projeto)
- [Endpoints da API](#-endpoints-da-api)
- [Fluxo do Pipeline RAG](#-fluxo-do-pipeline-rag)
- [Testes](#-testes)
- [Estrutura do Projeto](#-estrutura-do-projeto)

---

## 🎯 Visão Geral

O DocMind resolve o problema de **busca e compreensão de documentos corporativos** por meio de três etapas principais:

1. **Ingestão** — Upload e extração de texto de arquivos PDF e Markdown
2. **Processamento Semântico** — Segmentação inteligente (chunking) e geração de embeddings vetoriais
3. **Consulta RAG** — Busca semântica + geração de resposta contextualizada via LLM

O sistema opera com **fallback inteligente**: mesmo sem uma chave de API do Google Gemini configurada, as buscas semânticas e a recuperação de fragmentos funcionam normalmente — ideal para desenvolvimento e testes offline.

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                        Cliente / Swagger UI                  │
└──────────────────────────────┬──────────────────────────────┘
                               │ HTTP REST
┌──────────────────────────────▼──────────────────────────────┐
│                    FastAPI Application                        │
│                                                              │
│   ┌────────────┐  ┌────────────────┐  ┌──────────────────┐  │
│   │  /document │  │    /query      │  │      /rag        │  │
│   │  (Upload + │  │  (Semantic     │  │  (RAG Pipeline)  │  │
│   │  Process)  │  │   Search)      │  │                  │  │
│   └─────┬──────┘  └──────┬─────────┘  └────────┬─────────┘  │
│         │                │                      │            │
│   ┌─────▼──────────────────────────────────────▼─────────┐  │
│   │                   Services Layer                      │  │
│   │                                                       │  │
│   │  DocumentProcessor  │  EmbeddingService  │ RAGService │  │
│   │  SemanticProcessor  │  VectorStore                    │  │
│   └─────────────────────┬──────────────────────────────────┘ │
│                         │                                     │
│   ┌─────────────────────▼──────────────────────────────────┐ │
│   │              ChromaDB (Vector Store)                    │ │
│   │         sentence-transformers/all-MiniLM-L6-v2          │ │
│   │              Google Gemini (via LangChain)              │ │
│   └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

---

## ✨ Funcionalidades

- 📄 **Upload de Documentos** — Suporte a arquivos **PDF** e **Markdown** (até 10MB)
- 🔍 **Processamento Semântico** — Chunking recursivo com LangChain + embeddings HuggingFace locais
- 🗄️ **Armazenamento Vetorial** — Persistência em ChromaDB com busca por similaridade de cosseno (HNSW)
- 🤖 **Pipeline RAG Completo** — Recuperação + geração de resposta contextualizada com Google Gemini
- 🔎 **Busca Semântica Direta** — Endpoint dedicado para consultas sem geração de resposta por LLM
- 🛡️ **Fallback Inteligente** — Operação degradada graciosamente sem LLM ou sem embeddings reais
- 📊 **Health Check** — Monitoramento de integridade dos serviços
- 📝 **Logging Estruturado** — Logs coloridos (desenvolvimento) ou JSON (produção) via Loguru
- 🌐 **CORS Configurável** — Pronto para integração com frontends
- 📚 **Documentação Automática** — Swagger UI (`/docs`) e ReDoc (`/redoc`) integrados

---

## 🛠️ Tecnologias

| Camada | Tecnologia |
|---|---|
| **Framework Web** | FastAPI + Uvicorn |
| **Validação de Dados** | Pydantic v2 |
| **Banco Vetorial** | ChromaDB (persistente, HNSW cosine) |
| **Embeddings** | `sentence-transformers/all-MiniLM-L6-v2` via HuggingFace |
| **LLM** | Google Gemini (`gemini-2.5-flash`) via LangChain |
| **Chunking** | LangChain `RecursiveCharacterTextSplitter` |
| **Extração de PDF** | pypdf |
| **Logging** | Loguru |
| **Testes** | Pytest + pytest-asyncio |
| **Configuração** | pydantic-settings + python-dotenv |
| **Infraestrutura (futuro)** | Redis, RabbitMQ |

---

## ✅ Pré-requisitos

- **Python 3.10+**
- **pip** (gerenciador de pacotes)
- *(Opcional)* Chave de API do **Google AI Studio** para respostas geradas por LLM
- *(Opcional)* **Redis** e **RabbitMQ** para funcionalidades de cache e filas (planejadas)

---

## 🚀 Instalação

### 1. Clone o repositório

```bash
git clone <url-do-repositorio>
cd DocMind
```

### 2. Crie e ative um ambiente virtual

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux / macOS
python -m venv venv
source venv/bin/activate
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

> ⚠️ A instalação do `sentence-transformers` pode demorar alguns minutos pois realiza o download do modelo de embeddings na primeira execução.

---

## ⚙️ Configuração

### 1. Copie o arquivo de exemplo de variáveis de ambiente

```bash
cp .env.example .env
```

### 2. Edite o arquivo `.env` com suas configurações

```env
# Configuração da Aplicação
APP_NAME="Plataforma NLP RAG Enterprise"
APP_ENV=development
DEBUG=true
SECRET_KEY=sua-chave-secreta-aqui

# Armazenamento
CHROMADB_PATH=./data/chromadb
UPLOAD_DIR=./data/uploads
MAX_FILE_SIZE_MB=10
CHUNK_SIZE=500
CHUNK_OVERLAP=50
EMBEDDING_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2

# Google Gemini LLM (obrigatório para respostas geradas por IA)
GOOGLE_API_KEY=sua_chave_real_aqui
GEMINI_MODEL=gemini-2.5-flash
LLM_TEMPERATURE=0.2
LLM_MAX_TOKENS=1024

# Pipeline RAG
RAG_CONTEXT_CHUNKS=4
RAG_MIN_SIMILARITY=0.3

# CORS (ajuste conforme o frontend)
BACKEND_CORS_ORIGINS=["http://localhost:3000", "http://localhost:8000"]
```

> 💡 **Sem `GOOGLE_API_KEY`**: O sistema funciona normalmente em modo fallback — buscas semânticas retornam os fragmentos relevantes diretamente, sem geração de resposta pelo LLM.

---

## ▶️ Executando o Projeto

### Servidor de desenvolvimento (com hot reload)

```bash
uvicorn app.main:app --reload
```

### Servidor com host e porta customizados

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Após iniciar, acesse:

| Interface | URL |
|---|---|
| **Swagger UI** (documentação interativa) | http://localhost:8000/docs |
| **ReDoc** (documentação alternativa) | http://localhost:8000/redoc |
| **Raiz** (redireciona para /docs) | http://localhost:8000/ |

---

## 📡 Endpoints da API

Todos os endpoints estão prefixados em `/api/v1`.

### 🏥 Health Check

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/api/v1/health` | Retorna o status geral da aplicação e dos serviços |

**Resposta de exemplo:**
```json
{
  "status": "ok",
  "environment": "development",
  "version": "1.0.0",
  "services": {
    "chromadb": { "status": "healthy", "latency_ms": 0.5 },
    "redis": { "status": "not_connected" },
    "rabbitmq": { "status": "not_connected" }
  }
}
```

---

### 📄 Documentos

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/api/v1/document/upload` | Faz upload de um arquivo PDF ou Markdown |
| `POST` | `/api/v1/document/{document_id}/process` | Processa semanticamente um documento já enviado |

#### `POST /api/v1/document/upload`

Envie um arquivo via `multipart/form-data`:

```bash
curl -X POST "http://localhost:8000/api/v1/document/upload" \
     -F "file=@meu_documento.pdf"
```

**Resposta:**
```json
{
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "filename": "meu_documento.pdf",
  "status": "processed",
  "message": "Documento ingerido e texto extraído com sucesso.",
  "metadata": {
    "filename": "meu_documento.pdf",
    "file_size_bytes": 102400,
    "content_type": "application/pdf",
    "page_count": 5,
    "char_count": 8500,
    "uploaded_at": "2026-06-05T21:00:00Z"
  },
  "excerpt": "Primeiros 200 caracteres do conteúdo extraído..."
}
```

#### `POST /api/v1/document/{document_id}/process`

Gera chunks semânticos, embeddings e persiste no ChromaDB:

```bash
curl -X POST "http://localhost:8000/api/v1/document/550e8400-e29b-41d4-a716-446655440000/process"
```

**Resposta:**
```json
{
  "document_id": "550e8400-e29b-41d4-a716-446655440000",
  "total_chunks": 17,
  "chunks": [
    {
      "id": "chunk-uuid-aqui",
      "text": "Fragmento do documento...",
      "embedding": [0.023, -0.145, ...],
      "metadata": {
        "source_doc_id": "550e8400...",
        "filename": "meu_documento.pdf",
        "chunk_index": 0,
        "char_count": 487
      }
    }
  ]
}
```

---

### 🔎 Busca Semântica

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/api/v1/query/search` | Busca semântica direta no banco vetorial |

```bash
curl -X POST "http://localhost:8000/api/v1/query/search" \
     -H "Content-Type: application/json" \
     -d '{"query": "O que é aprendizado de máquina?", "limit": 5}'
```

**Resposta:**
```json
{
  "query": "O que é aprendizado de máquina?",
  "total_results": 3,
  "results": [
    {
      "chunk_id": "uuid-do-chunk",
      "text": "Fragmento relevante encontrado...",
      "similarity": 0.8734,
      "metadata": { "filename": "ml_guide.pdf", "chunk_index": 2 }
    }
  ]
}
```

---

### 🤖 Pipeline RAG

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/api/v1/rag/ask` | Pergunta ao sistema RAG com geração de resposta via LLM |

```bash
curl -X POST "http://localhost:8000/api/v1/rag/ask" \
     -H "Content-Type: application/json" \
     -d '{"question": "Quais são os principais algoritmos de classificação?", "limit": 4}'
```

**Resposta:**
```json
{
  "question": "Quais são os principais algoritmos de classificação?",
  "answer": "Com base nos documentos disponíveis, os principais algoritmos de classificação são...",
  "sources": [
    {
      "chunk_id": "uuid-do-chunk",
      "filename": "ml_algorithms.pdf",
      "excerpt": "Os algoritmos de classificação incluem...",
      "similarity": 0.9123
    }
  ],
  "context_found": true,
  "llm_used": true,
  "latency_ms": 1847.32
}
```

---

## 🔄 Fluxo do Pipeline RAG

```
Usuário faz pergunta
        │
        ▼
1. Geração do Embedding da pergunta
   (sentence-transformers/all-MiniLM-L6-v2)
        │
        ▼
2. Busca por Similaridade no ChromaDB
   (distância cosseno HNSW, top-K chunks)
        │
        ▼
3. Filtragem por Score Mínimo
   (RAG_MIN_SIMILARITY = 0.3 por padrão)
        │
        ├── Nenhum resultado → Resposta de fallback
        │
        ▼
4. Construção do Prompt com Contexto
   (chunks recuperados inseridos no system prompt)
        │
        ├── Sem GOOGLE_API_KEY → Retorna chunks diretamente (fallback)
        │
        ▼
5. Chamada ao Google Gemini via LangChain
   (temperatura configurável, max tokens)
        │
        ▼
6. Resposta final com fontes rastreáveis e latência
```

---

## 🧪 Testes

O projeto utiliza **Pytest** com cobertura de testes unitários e de integração.

### Executar todos os testes

```bash
pytest tests/ -v
```

### Executar um arquivo de teste específico

```bash
pytest tests/test_rag.py -v
pytest tests/test_document.py -v
pytest tests/test_vector_store.py -v
pytest tests/test_semantic.py -v
pytest tests/test_health.py -v
```

### Executar com relatório de cobertura

```bash
pytest tests/ -v --tb=short
```

**Suítes de teste disponíveis:**

| Arquivo | Cobertura |
|---|---|
| `test_rag.py` | Pipeline RAG completo, fallbacks, filtros de similaridade, integração via API |
| `test_document.py` | Upload, validação de tipo/tamanho, extração de texto PDF/Markdown |
| `test_vector_store.py` | Upsert de chunks, busca vetorial, contagem, deleção, estatísticas |
| `test_semantic.py` | Chunking semântico, geração de embeddings em lote, metadados dos chunks |
| `test_health.py` | Endpoint de health check, estrutura da resposta |

---

## 📁 Estrutura do Projeto

```
DocMind/
│
├── app/                          # Código-fonte principal da aplicação
│   ├── main.py                   # Ponto de entrada do FastAPI (factory pattern)
│   │
│   ├── api/                      # Camada de API REST
│   │   ├── router.py             # Roteador global da API
│   │   └── v1/
│   │       ├── router.py         # Roteador da versão 1
│   │       └── endpoints/        # Handlers de cada domínio
│   │           ├── health.py     # GET /health
│   │           ├── document.py   # POST /document/upload e /process
│   │           ├── query.py      # POST /query/search
│   │           └── rag.py        # POST /rag/ask
│   │
│   ├── core/                     # Infraestrutura e configurações
│   │   ├── config.py             # Settings via pydantic-settings (.env)
│   │   └── logging.py            # Configuração do Loguru
│   │
│   ├── schemas/                  # Modelos Pydantic (request/response)
│   │   ├── document.py           # DocumentMetadata, DocumentUploadResponse
│   │   ├── health.py             # HealthResponse, ServiceStatus
│   │   ├── rag.py                # RAGRequest, RAGResponse, SourceReference
│   │   ├── search.py             # SearchRequest, SearchResponse
│   │   └── semantic.py           # Chunk, SemanticProcessResponse
│   │
│   ├── services/                 # Lógica de negócio
│   │   ├── document_processor.py # Extração e limpeza de texto (PDF/Markdown)
│   │   ├── embedding_service.py  # Geração de embeddings (HuggingFace + fallback)
│   │   ├── semantic_processor.py # Chunking semântico + embedding em lote
│   │   ├── vector_store.py       # Interface com ChromaDB
│   │   └── rag_service.py        # Orquestração do pipeline RAG completo
│   │
│   └── workers/                  # Workers assíncronos (planejado)
│
├── tests/                        # Suítes de testes
│   ├── conftest.py               # Fixtures globais (TestClient)
│   ├── test_health.py
│   ├── test_document.py
│   ├── test_semantic.py
│   ├── test_vector_store.py
│   └── test_rag.py
│
├── data/                         # Dados persistentes (gerados em runtime)
│   ├── chromadb/                 # Banco vetorial ChromaDB
│   └── uploads/                  # Arquivos enviados pelos usuários
│
├── .env                          # Variáveis de ambiente (não versionado)
├── .env.example                  # Template de configuração
├── .gitignore
├── requirements.txt              # Dependências Python
└── README.md
```

---

## 🔒 Segurança

- **SECRET_KEY**: Altere para um valor seguro em produção
- **GOOGLE_API_KEY**: Nunca versione no repositório — use `.env` ou secrets do ambiente
- **CORS**: Configure `BACKEND_CORS_ORIGINS` com apenas as origens confiáveis
- **Uploads**: Validação de tipo (`.pdf`, `.md`) e tamanho máximo (`MAX_FILE_SIZE_MB`)

---

## 🗺️ Roadmap

- [ ] Autenticação JWT com `pyjwt` e `passlib`
- [ ] Cache de embeddings e respostas com **Redis**
- [ ] Processamento assíncrono de documentos com **RabbitMQ**
- [ ] Suporte a mais formatos (DOCX, TXT, HTML)
- [ ] Múltiplas coleções isoladas por usuário/projeto
- [ ] Dashboard de monitoramento de documentos ingeridos

---

## 📄 Licença

Este projeto foi desenvolvido como projeto acadêmico para a disciplina de **Análise de Dados - Análise e Desenvolvimento de Sistemas — 6º Semestre**.

---

<div align="center">
  <sub>Desenvolvido utilizando FastAPI, LangChain, ChromaDB e Google Gemini</sub>
</div>
