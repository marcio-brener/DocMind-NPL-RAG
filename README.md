# 🧠 DocMind — Plataforma NLP RAG Enterprise

> **Plataforma Corporativa de Processamento de Linguagem Natural com RAG, Cache Semântico e Arquitetura Orientada a Eventos (EDA).**

DocMind é uma solução corporativa de alta performance construída para simplificar a busca semântica e a extração de inteligência de grandes volumes de documentos de formato **PDF** e **Markdown**. A plataforma utiliza uma arquitetura baseada em eventos (EDA) com **RabbitMQ** para o desacoplamento de uploads, banco de dados vetorial **ChromaDB** para o armazenamento de embeddings locais gerados com **HuggingFace**, **Redis** para caching de respostas de geração aumentada (RAG), e **Google Gemini** para geração final de respostas contextualizadas com rastreabilidade total de fontes.

---

## 📋 Índice

- [1. Visão Geral do Projeto](#1-visão-geral-do-projeto)
- [2. Objetivo da Aplicação](#2-objetivo-da-aplicação)
- [3. Arquitetura da Solução](#3-arquitetura-da-solução)
- [4. Fluxo Completo de Processamento de Documentos](#4-fluxo-completo-de-processamento-de-documentos)
- [5. Fluxo do Pipeline RAG](#5-fluxo-do-pipeline-rag)
- [6. Fluxo do NLP Event Worker](#6-fluxo-do-nlp-event-worker)
- [7. Fluxo de Mensageria (RabbitMQ)](#7-fluxo-de-mensageria-rabbitmq)
- [8. Fluxo de Caching (Redis)](#8-fluxo-de-caching-redis)
- [9. Fluxo do Banco Vetorial (ChromaDB)](#9-fluxo-do-banco-vetorial-chromadb)
- [10. Estrutura do Projeto](#10-estrutura-do-projeto)
- [11. Tecnologias Utilizadas](#11-tecnologias-utilizadas)
- [12. Variáveis de Ambiente](#12-variáveis-de-ambiente)
- [13. Como Executar Localmente](#13-como-executar-localmente)
- [14. Execução via Docker e Docker Compose](#14-execução-via-docker-e-docker-compose)
- [15. Orquestração com Docker Swarm](#15-orquestração-com-docker-swarm)
- [16. Como Executar os Testes](#16-como-executar-os-testes)
- [17. Endpoints da API (v1)](#17-endpoints-da-api-v1)
- [18. Exemplos Práticos de Uso (cURL)](#18-exemplos-práticos-de-uso-curl)
- [19. Métricas e Observabilidade](#19-métricas-e-observabilidade)
- [20. Estratégia de Caching Semântico](#20-estratégia-de-caching-semântico)
- [21. Estratégia de Recuperação e Re-ranking Híbrido](#21-estratégia-de-recuperação-e-re-ranking-híbrido)
- [22. Checklist antes do Push para o GitHub](#22-checklist-antes-do-push-para-o-github)
- [23. Variáveis Obrigatórias para Produção](#23-variáveis-obrigatórias-para-produção)
- [24. Controle de Inconsistências e Segurança](#24-controle-de-inconsistências-e-segurança)
- [25. Melhorias Futuras (Roadmap)](#25-melhorias-futuras-roadmap)

---

## 🎯 1. Visão Geral do Projeto

A DocMind é estruturada em torno de microserviços assíncronos. A API REST expõe endpoints rápidos que aceitam cargas úteis, persistem fisicamente os documentos em disco local para fins de auditoria técnica, registram o progresso em um banco de dados de tarefas e repassam os fluxos pesados de NLP para filas gerenciadas. Workers dedicados realizam a extração de dados, limpeza estrutural, chunking e indexação vetorial. As perguntas submetidas ao pipeline de RAG consultam o cache Redis antes de disparar embeddings do HuggingFace e buscar blocos de texto por similaridade de cosseno no ChromaDB, que posteriormente abastecem o prompt do Google Gemini.

---

## 🚀 2. Objetivo da Aplicação

O objetivo principal da DocMind é fornecer uma infraestrutura de NLP e busca corporativa que seja resiliente, desacoplada e veloz. Ela foi projetada para sanar os problemas comuns de sistemas RAG monolíticos convencionais:
1. **Gargalo no upload de arquivos:** A API responde em milissegundos informando que o arquivo foi recebido, enquanto o processamento de texto e geração de embeddings (que exige processamento computacional elevado) roda de forma assíncrona.
2. **Custos elevados de chamada ao LLM:** Utilização de cache estruturado para evitar chamadas redundantes de perguntas idênticas ou semanticamente mapeadas.
3. **Falhas por timeout ou sobrecarga:** Filas com controle de prefetch, retentativas exponenciais automáticas e redirecionamento para DLQ em caso de arquivos corrompidos.
4. **Precisão de resposta:** Recuperação de dados aprimorada com filtragem por threshold dinâmico e re-ranking de tokens.

---

## 🏗️ 3. Arquitetura da Solução

O ecossistema é composto por uma API FastAPI principal que lida com requisições síncronas de clientes, e workers em Python que realizam as operações assíncronas de NLP consumindo do RabbitMQ. A persistência baseia-se em volumes locais para armazenamento físico de arquivos e no ChromaDB (banco vetorial corporativo persistido em disco). O Redis gerencia o ciclo de cache.

```mermaid
graph TD
    Client[Cliente / Swagger UI] <-->|HTTP REST| API[FastAPI Application]
    
    %% API Services Interaction
    API <-->|Leitura/Escrita| TaskService[TaskService / tasks.json]
    API <-->|Cache HIT/MISS| Redis[(Redis Cache)]
    API <-->|Busca Direta| VectorStore[VectorStore ChromaDB]
    
    %% Messaging flow
    API --->|Publicar Ingestão| RMQ_Doc[Fila: document_processing_queue]
    API --->|Publicar Pergunta RAG| RMQ_Rag[Fila: rag_requests]
    
    %% Broker Routing
    subgraph RabbitMQ Broker
        RMQ_Doc
        RMQ_Rag
        DLQ[Fila: document_dlq]
        RMQ_Doc -.->|Falhas Críticas| DLQ
        RMQ_Rag -.->|Falhas Críticas| DLQ
    end
    
    %% Async Processing
    RMQ_Doc ===>|Consumir Ingestão| Worker[Document NLP Worker]
    RMQ_Rag ===>|Consumir RAG| API
    
    %% Worker Internal Actions
    Worker -->|1. Ler Físico| LocalUploads[Uploads Directory /data/uploads]
    Worker -->|2. Chunking & Local Embeddings| HF[HuggingFace local model]
    Worker -->|3. Upsert Chunks| VectorStore
    Worker -->|4. Atualizar Progresso| TaskService
    
    %% RAG Execution Pipeline
    API -->|1. Embed Query| HF
    API -->|2. Busca Cosseno HNSW| VectorStore
    API -->|3. Re-ranking & Threshold| RAGService[RAGService]
    RAGService -->|4. Gerar Resposta| Gemini[Google Gemini LLM]
    RAGService -->|5. Salvar Resposta| Redis
    
    style Client fill:#f9f,stroke:#333,stroke-width:2px
    style API fill:#bbf,stroke:#333,stroke-width:2px
    style Worker fill:#bfb,stroke:#333,stroke-width:2px
    style RabbitMQ Broker fill:#ffc,stroke:#333,stroke-width:1px
    style VectorStore fill:#fbc,stroke:#333,stroke-width:2px
    style Redis fill:#ffb,stroke:#333,stroke-width:2px
```

---

## 📄 4. Fluxo Completo de Processamento de Documentos

A ingestão segue uma pipeline assíncrona orientada a eventos para garantir alta resiliência e evitar sobrecarga na API HTTP:

```
[Cliente] -> POST /api/v1/document/upload
   │
   ▼
[API FastAPI] 
   ├── 1. Valida tamanho (limite configurado: MAX_FILE_SIZE_MB) e extensão (.pdf, .md)
   ├── 2. Persiste o arquivo fisicamente em UPLOAD_DIR com prefixo de document_id (UUID)
   ├── 3. Registra tarefa no JSON local (status = QUEUED, progresso = 0%)
   ├── 4. Enfileira metadados e caminho do arquivo na fila 'document_processing_queue'
   └── 5. Retorna imediatamente HTTP 202 com { document_id, status: "queued" }
   
                   [RabbitMQ Broker]
                           │
                           ▼
[Document NLP Worker] (Consome a mensagem da fila)
   ├── 1. Atualiza status da tarefa para PROCESSING (progresso = 10%)
   ├── 2. Abre o arquivo local correspondente e extrai o texto bruto via pypdf (PDF) ou leitura nativa (MD)
   ├── 3. Atualiza status para PROCESSING (progresso = 30%)
   ├── 4. Limpa espaços adicionais, quebras de linhas desnecessárias e caracteres inválidos
   ├── 5. Atualiza status para PROCESSING (progresso = 60%)
   ├── 6. Divide o texto em fragmentos (chunks) usando RecursiveCharacterTextSplitter do LangChain (com CHUNK_SIZE e CHUNK_OVERLAP)
   ├── 7. Atualiza status para PROCESSING (progresso = 80%)
   ├── 8. Gera vetores de embedding locais em lote utilizando sentence-transformers/all-MiniLM-L6-v2
   ├── 9. Persiste os vetores, textos e metadados no ChromaDB
   └── 10. Atualiza tarefa para COMPLETED (progresso = 100%)
```

---

## 🔍 5. Fluxo do Pipeline RAG

A busca semântica e a síntese por IA utilizam uma pipeline otimizada com re-ranking híbrido e cache Redis:

```
[Cliente] -> POST /api/v1/rag/ask
   │
   ▼
[API FastAPI]
   ├── 1. Gera request_id (UUID) único
   ├── 2. Cria uma tarefa no TaskService (status = QUEUED)
   ├── 3. Publica a requisição na fila 'rag_requests'
   └── 4. Retorna imediatamente HTTP 202 com o task_id e request_id
   
                   [RabbitMQ Broker]
                           │
                           ▼
[Consumidor RAG (FastAPI Lifespan)] (Consome da fila 'rag_requests')
   ├── 1. Atualiza a tarefa para PROCESSING (progresso = 50%)
   ├── 2. Normaliza a pergunta do usuário e gera uma chave SHA-256 determinística
   ├── 3. Consulta o Redis com a chave do cache:
   │      ├── [CACHE HIT]
   │      │   └── Retorna a resposta e fontes do cache Redis em milissegundos
   │      │
   │      └── [CACHE MISS] (Executa o pipeline completo):
   │             ├── a. Gera embedding vetorial da pergunta usando o sentence-transformers local
   │             ├── b. Executa a busca HNSW por similaridade de cosseno no ChromaDB (trazendo candidatos)
   │             ├── c. Executa deduplicação de chunks exatos
   │             ├── d. Aplica o Re-ranking Híbrido (similaridade vetorial + token overlap)
   │             ├── e. Filtra trechos abaixo do threshold de similaridade adaptativo mínimo
   │             ├── f. Limita quantidade de chunks para evitar estouro da janela de contexto
   │             ├── g. Se não houver contexto acima do limiar: gera resposta padrão de fallback
   │             ├── h. Se houver contexto: monta prompt e aciona o modelo Google Gemini
   │             ├── i. Salva o payload de resposta no cache Redis (TTL padrão: 3600 segundos)
   │             └── j. Atualiza a tarefa local para COMPLETED (progresso = 100%) com os resultados
```

---

## ⚙️ 6. Fluxo do NLP Event Worker

O worker standalone (`document_worker.py`) é construído com base na biblioteca `aio-pika` para operações assíncronas completas.
- **Tratamento de Sinais:** Captura interrupções como `SIGINT` e `SIGTERM` para fechar conexões graciosamente sem derrubar mensagens pendentes do broker.
- **Thread Pool Execution:** As operações síncronas pesadas do PDF (extração de texto, geração local de embeddings em CPU e operações vetoriais de escrita) são envelopadas em thread pools do asyncio (`asyncio.to_thread`) para evitar o congelamento do loop de eventos principal, garantindo que o worker continue consumindo e gerenciando o canal RabbitMQ simultaneamente.
- **Redirecionamento Automático:** Caso ocorra alguma falha crítica durante as etapas do processamento do documento, o worker incrementa os cabeçalhos de tentativa da mensagem (`x-retry-count`). Após **3 retentativas**, a mensagem é rejeitada sem requeue, o que ativa o comportamento de Dead Letter Exchange (DLX) do RabbitMQ, desviando a mensagem para a fila de segurança de falhas permanentes (`document_dlq`).

---

## 📬 7. Fluxo de Mensageria (RabbitMQ)

A topologia do RabbitMQ no DocMind é configurada de forma idempotente no startup do sistema:

- **Exchanges Declaradas:**
  - `document_exchange`: Exchange do tipo `direct` (durable = True).
  - `document_exchange.dlx`: Exchange de Dead Letter (tipo `direct`, durable = True).

- **Filas Declaradas:**
  - `document_processing_queue`: Fila principal de processamento de documentos vinculada à `document_exchange` pela chave de roteamento `document.process`. Configura o argumento `"x-dead-letter-exchange": "document_exchange.dlx"` e `"x-dead-letter-routing-key": "document_dlq"`.
  - `rag_requests`: Fila de requisições do RAG para processamento assíncrono de IA, vinculada à exchange padrão com redirecionamento de falhas para a DLX.
  - `document_dlq`: Fila de mensagens mortas vinculada à exchange `document_exchange.dlx` pela routing key `document_dlq`. Mensagens que falharem permanentemente são depositadas aqui para análise posterior.

- **Configurações Adicionais:**
  - **QoS Prefetch = 1:** Evita que um worker sobrecarregue enquanto outros estão ociosos. O RabbitMQ distribui as mensagens conforme a disponibilidade (Fair Dispatch).
  - **Mensagens Persistentes (`delivery_mode=2`):** Garante que as mensagens sobrevivam a reinicializações bruscas do broker.

---

## 💾 8. Fluxo de Caching (Redis)

O DocMind integra um cache Redis robusto para as respostas de perguntas do RAG:
- **Chave de Cache Determinística:** A chave de cache é gerada através do SHA-256 da string normalizada da pergunta, concatenada com o limite de chunks e o ID do documento de filtro (caso exista). A normalização remove quebras de linha duplicadas, pontuações no fim da frase e converte todos os caracteres para minúsculo.
- **Operação de Fallback (No-Op):** O `CacheService` possui tratamento de erro global. Se o servidor Redis estiver inativo ou a biblioteca de conexão não estiver instalada, o sistema gera logs de alerta, desativa o cache e opera normalmente em modo degradado direto na base de dados, sem interromper as operações do usuário.
- **Invalidação Dinâmica:** Sempre que uma exclusão física de documento ocorre (`DELETE /api/v1/document/{id}`) ou uma rotina de reprocessamento em lote é solicitada (`POST /api/v1/document/reprocess`), o cache do Redis é limpo (`FLUSHDB`) de forma a assegurar que nenhuma resposta desatualizada seja entregue ao usuário final.

---

## 🗄️ 9. Fluxo do Banco Vetorial (ChromaDB)

- **Coleção de Dados:** Coleção persistente nomeada `nlp_rag_collection`.
- **Métrica de Distância:** Configurada via metadados HNSW para similaridade de cosseno (`"hnsw:space": "cosine"`). O ChromaDB calcula o score através da fórmula: $1.0 - \text{cossine\_distance}$.
- **Geração de Embeddings:** O sentence-transformers local (`all-MiniLM-L6-v2`) gera vetores de **384 dimensões**.
- **Metadados Anexados:** Cada vetor possui metadados estruturados que facilitam filtros rápidos e auditoria:
  - `source_doc_id`: UUID do documento original.
  - `filename`: Nome original do arquivo importado.
  - `chunk_index`: Índice sequencial do fragmento no documento.
  - `char_count`: Tamanho do fragmento em caracteres.
  - `total_chunks`: Quantidade de fragmentos gerados para o documento.
  - `uploaded_at`: ISO timestamp da data de upload.

---

## 📁 10. Estrutura do Projeto

```
DocMind/
│
├── app/                          # Código-fonte principal da aplicação
│   ├── __init__.py
│   ├── main.py                   # Ponto de entrada do FastAPI, rotas de redirecionamento e Lifespan
│   │
│   ├── api/                      # Camada de Endpoints REST
│   │   ├── __init__.py
│   │   ├── router.py             # Roteador raiz que inclui a versão 1
│   │   └── v1/
│   │       ├── __init__.py
│   │       ├── router.py         # Mapeia os prefixos dos endpoints da v1
│   │       └── endpoints/        # Rotas individuais por domínio
│   │           ├── __init__.py
│   │           ├── health.py     # Checagem de integridade (/health)
│   │           ├── document.py   # Upload, processamento síncrono, exclusão e reprocessamento
│   │           ├── query.py      # Busca semântica direta no ChromaDB
│   │           ├── rag.py        # Pergunta RAG (/ask) e consulta Redis (/result/{id})
│   │           └── tasks.py      # Acompanhamento do progresso das tarefas (/tasks/{id})
│   │
│   ├── core/                     # Configurações globais e utilitários de infraestrutura
│   │   ├── __init__.py
│   │   ├── config.py             # Settings do Pydantic carregando o arquivo .env
│   │   └── logging.py            # Loguru estruturado para desenvolvimento e produção
│   │
│   ├── schemas/                  # Validação e documentação de payloads Pydantic
│   │   ├── __init__.py
│   │   ├── document.py           # Respostas de upload, reprocessamento e metadados
│   │   ├── health.py             # Respostas e latency models para o healthcheck
│   │   ├── rag.py                # Request e response models do pipeline RAG
│   │   ├── search.py             # Filtros e respostas da busca semântica
│   │   ├── semantic.py           # Estruturação de chunks e vetores de embeddings
│   │   └── task.py               # Estruturação de tarefas no TaskService
│   │
│   └── services/                 # Regras de Negócio e Serviços Externos
│       ├── __init__.py
│       ├── cache_service.py      # Cache Redis assíncrono degradável
│       ├── document_processor.py # Processador de texto, limpezas e parser PDF/Markdown
│       ├── embedding_service.py  # Gerador de embeddings locais com fallback determinístico
│       ├── rabbitmq_service.py   # Conector Robust do aio-pika, topologia e fila de RAG
│       ├── rag_service.py        # Pipeline RAG: re-ranking híbrido, threshold e LangChain
│       ├── reprocess_cli.py      # Script CLI standalone para reindexação geral do banco
│       ├── semantic_processor.py # Divisor de chunks semânticos e gerador de lote
│       ├── task_service.py       # CRUD Thread-Safe em arquivo JSON para progresso assíncrono
│       └── vector_store.py       # Gerenciador da coleção e comandos do ChromaDB
│
├── workers/                      # Diretorio de Workers assíncronos da aplicação
│   ├── __init__.py
│   └── document_worker.py        # Standalone consumer de ingestão de arquivos
│
├── tests/                        # Coleção de testes integrados e unitários
│   ├── __init__.py
│   ├── conftest.py               # Fixture para instanciar o TestClient do FastAPI
│   ├── test_cv_recall.py         # Teste de recall de termos em currículos
│   ├── test_document.py          # Teste de arquivos muito grandes e extração
│   ├── test_filter_propagation.py# Teste de propagação de filtros no banco
│   ├── test_health.py            # Teste de retorno do endpoint de saúde
│   ├── test_rag.py               # Testes das regras do pipeline RAG e do endpoint RAG
│   └── test_vector_store.py      # Testes de upserts e remoção no ChromaDB
│
├── data/                         # Volumes de armazenamento (gerados em runtime — ignore no git!)
│   ├── chromadb/                 # Arquivos persistidos do banco ChromaDB
│   ├── uploads/                  # Arquivos físicos mantidos no servidor
│   └── tasks.json                # Banco de dados de progresso das tarefas locais
│
├── docker-compose.yml            # Orquestração do ambiente de desenvolvimento Docker
├── requirements.txt              # Bibliotecas de dependências do Python
├── .env.example                  # Template com chaves e opções padrão
├── .env                          # Arquivo local com chaves de produção (Não commitar!)
└── .gitignore                    # Regras de exclusão de arquivos no repositório
```

---

## 🛠️ 11. Tecnologias Utilizadas

O ecossistema do DocMind utiliza tecnologias modernas voltadas para aplicações de IA distribuídas:

- **FastAPI (v0.111.0+)**: Framework web assíncrono em Python para construção de APIs eficientes com validação automática de dados OpenAPI e documentação imediata via Swagger UI.
- **Uvicorn (v0.29.0+)**: Servidor ASGI de alta performance para execução da aplicação FastAPI.
- **Pydantic (v2.7.0+) & Pydantic-Settings**: Validação robusta de tipos e carregamento de configurações de ambiente estruturadas de forma segura.
- **ChromaDB (v0.5.0+)**: Banco de dados vetorial embutido e persistente, operando com busca HNSW de similaridade por cosseno.
- **HuggingFace Embeddings & Sentence-Transformers (v2.6.0+)**: Gerador local de vetores densos usando o modelo `all-MiniLM-L6-v2` sem necessidade de requisições de rede.
- **LangChain (v0.2.1+) & LangChain-Google-Genai (v1.0.6+)**: Orquestrador de pipelines RAG, templates de prompt estruturados e gerador de respostas via Gemini.
- **Google GenerativeAI (v0.5.4+)**: Biblioteca oficial para conexões e chamadas à API do modelo Gemini.
- **Redis (v5.0.4+)**: Utilizado para armazenamento em cache de respostas prontas e redução de latência/custos de infraestrutura.
- **RabbitMQ (v3-management)**: Broker de mensageria assíncrona que gerencia e distribui tarefas por tópicos e filas confiáveis.
- **aio-pika (v9.4.1+) & pika**: Biblioteca de conexões e gerenciamento do RabbitMQ de forma totalmente assíncrona.
- **PyPDF (v4.2.0+)**: Parser e extrator local de fluxos de dados textuais e metadados de arquivos em formato PDF.
- **Pytest (v8.1.1+) & pytest-asyncio**: Framework de execução de testes unitários e de integração assíncronos.
- **Loguru (v0.7.2+)**: Gerenciador de logs coloridos, estruturados e rotativos para auditoria de requests e observabilidade em tempo real.

---

## ⚙️ 12. Variáveis de Ambiente

Todas as variáveis que regem o comportamento da DocMind estão declaradas no arquivo `.env.example`:

| Variável | Padrão | Tipo | Descrição |
|---|---|---|---|
| `APP_NAME` | `"Plataforma NLP RAG Enterprise"` | `str` | Nome da aplicação exibido na documentação Swagger. |
| `APP_ENV` | `development` | `str` | Ambiente ativo (`development`, `staging`, `production`). |
| `DEBUG` | `true` | `bool` | Ativa depuração de erros. |
| `API_V1_STR` | `/api/v1` | `str` | Prefixo global dos endpoints REST. |
| `SECRET_KEY` | `super-secret-key-change-in-production` | `str` | Chave criptográfica usada para autenticação JWT e hashes. |
| `HOST` | `0.0.0.0` | `str` | Endereço de escuta do servidor Uvicorn. |
| `PORT` | `8000` | `int` | Porta TCP para requisições HTTP. |
| `BACKEND_CORS_ORIGINS` | `["http://localhost:3000"]` | `list` | Origens autorizadas a realizar chamadas CORS (JSON array). |
| `CHROMADB_PATH` | `./data/chromadb` | `str` | Diretório físico dos dados vetoriais do ChromaDB. |
| `UPLOAD_DIR` | `./data/uploads` | `str` | Diretório físico de armazenamento dos arquivos locais. |
| `MAX_FILE_SIZE_MB` | `10` | `int` | Limite de tamanho máximo de arquivo de upload (MB). |
| `CHUNK_SIZE` | `1000` | `int` | Tamanho máximo de caracteres por chunk de texto. |
| `CHUNK_OVERLAP` | `150` | `int` | Sobreposição de caracteres entre chunks adjacentes. |
| `EMBEDDING_MODEL_NAME`| `sentence-transformers/all-MiniLM-L6-v2` | `str` | Nome do modelo sentence-transformers para embeddings. |
| `REDIS_URL` | `redis://localhost:6379/0` | `str` | String de conexão para o servidor de caching Redis. |
| `CACHE_TTL_SECONDS` | `3600` | `int` | Tempo de expiração do cache no Redis em segundos. |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | `str` | URL de conexão com o broker RabbitMQ. |
| `RABBITMQ_QUEUE` | `document_processing_queue` | `str` | Fila principal de processamento assíncrono. |
| `RABBITMQ_EXCHANGE` | `document_exchange` | `str` | Exchange para direcionar as mensagens de ingestão. |
| `RABBITMQ_ROUTING_KEY`| `document.process` | `str` | Chave de roteamento para associar exchange e fila. |
| `RABBITMQ_RAG_QUEUE` | `rag_requests` | `str` | Fila assíncrona de requisições de perguntas RAG. |
| `GOOGLE_API_KEY` | `""` | `str` | Chave de acesso à API do Google Gemini. |
| `GEMINI_MODEL` | `gemini-2.5-flash` | `str` | Modelo LLM do Google usado nas respostas. |
| `LLM_TEMPERATURE` | `0.2` | `float` | Temperatura de geração do LLM (criatividade). |
| `LLM_MAX_TOKENS` | `2048` | `int` | Quantidade máxima de tokens gerados pelo LLM. |
| `DEFAULT_CONTEXT_CHUNKS`| `10` | `int` | Quantidade recomendada de chunks enviados ao LLM. |
| `RAG_MIN_SIMILARITY` | `0.25` | `float` | Score de corte mínimo de similaridade para RAG. |
| `EXCERPT_LENGTH` | `400` | `int` | Tamanho máximo do trecho da fonte na resposta (chars). |

---

## 💻 13. Como Executar Localmente

### 1. Requisitos Prévios
Certifique-se de possuir o Python 3.10 ou superior instalado na máquina, bem como o Docker para iniciar a infraestrutura de mensageria e caching.

### 2. Configurar Ambiente Virtual
No diretório raiz da aplicação, execute:
```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux / macOS
python -m venv venv
source venv/bin/activate
```

### 3. Instalar Dependências
```bash
pip install -r requirements.txt
```

### 4. Configurar Variáveis de Ambiente
Copie o template do arquivo e preencha com a sua chave do Gemini (`GOOGLE_API_KEY`):
```bash
cp .env.example .env
```

### 5. Subir a Infraestrutura (Containers)
Inicie o RabbitMQ local via Docker:
```bash
docker run -d --name docmind-rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```
*(Opcional)* Inicie o Redis local via Docker:
```bash
docker run -d --name docmind-redis -p 6379:6379 redis:alpine
```

### 6. Executar o Servidor FastAPI
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### 7. Executar o Worker de Processamento
Em um segundo terminal (com o ambiente virtual ativo), inicie o worker:
```bash
python -m workers.document_worker
```

### 8. Executar Reprocessamento via CLI
Caso altere variáveis como `CHUNK_SIZE` ou `CHUNK_OVERLAP` no `.env` e queira reindexar todos os documentos que estão na pasta de uploads local para o ChromaDB, execute a ferramenta CLI integrada:
```bash
python -m app.services.reprocess_cli
```

---

## 🐳 14. Execução via Docker e Docker Compose

O arquivo `docker-compose.yml` orquestra a aplicação em um único comando.

### Executando em Segundo Plano
```bash
docker-compose up -d
```

### Escalonando Workers
Para aumentar a vazão de processamento de documentos sob alta carga de uploads, você pode escalar a quantidade de containers workers que consomem da fila RabbitMQ:
```bash
docker-compose up -d --scale worker=3
```

### Gerenciando e Derrubando o Ambiente
```bash
# Exibir os logs em tempo real
docker-compose logs -f

# Derrubar a infraestrutura e remover volumes temporários
docker-compose down
```

---

## 🕸️ 15. Orquestração com Docker Swarm

Para implantar a DocMind em ambientes distribuídos corporativos multicontêineres usando **Docker Swarm**:

### 1. Inicializar o Cluster Swarm
```bash
docker swarm init
```

### 2. Implantar a Stack
A stack lê o arquivo `docker-compose.yml` e o distribui nos nós do cluster.
```bash
docker stack deploy -c docker-compose.yml docmind_stack
```

### 3. Escalar Serviços no Swarm
Aumente o número de réplicas do worker de NLP de forma dinâmica:
```bash
docker service scale docmind_stack_worker=5
```

### 4. Remover a Stack do Cluster
```bash
docker stack rm docmind_stack
```

---

## 🧪 16. Como Executar os Testes

A DocMind possui testes unitários e de integração completos com Pytest. Devido a conflitos na criação do executável direto do pytest em alguns sistemas Windows, é recomendado chamar os testes utilizando a flag de módulo do executável do Python:

```bash
# Executar a suíte completa de testes de forma detalhada
.\venv\Scripts\python -m pytest tests/ -v

# Executar apenas um arquivo de testes específico
.\venv\Scripts\python -m pytest tests/test_health.py -v
.\venv\Scripts\python -m pytest tests/test_document.py -v
.\venv\Scripts\python -m pytest tests/test_vector_store.py -v
.\venv\Scripts\python -m pytest tests/test_rag.py -v
```

---

## 📡 17. Endpoints da API (v1)

Todos os endpoints estão documentados no OpenAPI/Swagger na rota `/docs`.

### 🏥 Health
- **`GET /api/v1/health`**: Verifica a integridade da API FastAPI e exibe o estado de conexão e latência dos serviços integrados (ChromaDB, Redis, RabbitMQ).

### 📄 Documents
- **`POST /api/v1/document/upload`**: Recebe arquivos PDF ou MD via form-data. Envia para uploads físicos, cadastra a tarefa no TaskService e publica no RabbitMQ. Retorna o ID da tarefa para acompanhamento assíncrono.
- **`POST /api/v1/document/{document_id}/process`**: Ingestão síncrona manual de arquivo. Processa o arquivo físico correspondente gerando chunks e embeddings locais, atualizando o banco vetorial diretamente. Utilizado para fallbacks técnicos de depuração.
- **`POST /api/v1/document/reprocess`**: Varre o diretório `/data/uploads`, apaga os dados antigos correspondentes do ChromaDB, re-computa chunks e embeddings sob novas configurações do `.env` e limpa o cache Redis.
- **`DELETE /api/v1/document/{document_id}`**: Exclui fisicamente o arquivo em disco, deleta todos os chunks associados do ChromaDB e executa a limpeza no cache Redis.

### 🔎 Search
- **`POST /api/v1/query/search`**: Executa uma pesquisa vetorial baseada na similaridade de cosseno diretamente no ChromaDB a partir de uma frase de entrada e do limite solicitado. Útil para buscas brutas sem processamento do LLM Gemini. Aceita o campo opcional `filter_document_id` no payload.

### 🤖 RAG Pipeline
- **`POST /api/v1/rag/ask`**: Aceita uma pergunta do usuário no payload. Enfileira a pergunta na fila assíncrona do RabbitMQ e retorna imediatamente o identificador único da tarefa para checagem.
- **`GET /api/v1/rag/result/{request_id}`**: Endpoint síncrono rápido que lê do Redis pelo ID da requisição para verificar se a resposta de IA já foi processada pelo worker assíncrono.

### 📝 Tasks
- **`GET /api/v1/tasks/{task_id}`**: Endpoint para polling do progresso das tarefas de ingestão de documentos ou respostas do RAG. Retorna status (`QUEUED`, `PROCESSING`, `COMPLETED`, `FAILED`), porcentagem de progresso e payloads de resultados ou logs de erros detalhados.

---

## 🔌 18. Exemplos Práticos de Uso (cURL)

### Ingestão de Documento (Upload Assíncrono)
```bash
curl -X POST "http://localhost:8000/api/v1/document/upload" \
     -H "accept: application/json" \
     -H "Content-Type: multipart/form-data" \
     -F "file=@manual_ti.pdf;type=application/pdf"
```
*Resposta esperada (HTTP 202 Accepted):*
```json
{
  "document_id": "87c4f420-ba12-421c-81bf-6548a3ee264d",
  "status": "queued",
  "message": "Documento enviado para processamento"
}
```

### Consultar Progresso da Tarefa de Ingestão
```bash
curl -X GET "http://localhost:8000/api/v1/tasks/87c4f420-ba12-421c-81bf-6548a3ee264d" \
     -H "accept: application/json"
```
*Resposta esperada (HTTP 200 OK — Concluído):*
```json
{
  "task_id": "87c4f420-ba12-421c-81bf-6548a3ee264d",
  "document_id": "87c4f420-ba12-421c-81bf-6548a3ee264d",
  "filename": "manual_ti.pdf",
  "status": "COMPLETED",
  "progress": 100,
  "message": "Processamento concluído com sucesso. 14 chunks indexados no ChromaDB.",
  "created_at": "2026-06-11T20:25:00",
  "updated_at": "2026-06-11T20:25:05",
  "error": null,
  "result": null
}
```

### Fazer uma Pergunta ao Sistema RAG (Assíncrono)
```bash
curl -X POST "http://localhost:8000/api/v1/rag/ask" \
     -H "accept: application/json" \
     -H "Content-Type: application/json" \
     -d '{"question": "Quais são os protocolos de rede recomendados?", "limit": 5}'
```
*Resposta esperada (HTTP 202 Accepted):*
```json
{
  "task_id": "f2923ea0-da41-48e0-a7d5-e51c8b3ee14f",
  "request_id": "f2923ea0-da41-48e0-a7d5-e51c8b3ee14f",
  "status": "PROCESSING",
  "timestamp": "2026-06-11T20:26:00.000000"
}
```

### Consultar Resultado do RAG via ID da Tarefa
```bash
curl -X GET "http://localhost:8000/api/v1/tasks/f2923ea0-da41-48e0-a7d5-e51c8b3ee14f" \
     -H "accept: application/json"
```
*Resposta esperada (HTTP 200 OK — Resposta de IA com fontes detalhadas):*
```json
{
  "task_id": "f2923ea0-da41-48e0-a7d5-e51c8b3ee14f",
  "document_id": null,
  "filename": null,
  "status": "COMPLETED",
  "progress": 100,
  "message": "Resposta gerada com sucesso",
  "created_at": "2026-06-11T20:26:00",
  "updated_at": "2026-06-11T20:26:02",
  "error": null,
  "result": {
    "request_id": "f2923ea0-da41-48e0-a7d5-e51c8b3ee14f",
    "answer": "De acordo com as diretrizes do manual_ti.pdf, os protocolos recomendados são HTTPS e SSH.",
    "sources": [
      {
        "chunk_id": "87c4f420-ba12-421c-81bf-6548a3ee264d_chunk_3",
        "filename": "manual_ti.pdf",
        "excerpt": "Para comunicações seguras na rede, é recomendado o uso do protocolo HTTPS para tráfego web e SSH para conexões de console...",
        "similarity": 0.895
      }
    ]
  }
}
```

### Busca Semântica Direta no ChromaDB (Sem LLM)
```bash
curl -X POST "http://localhost:8000/api/v1/query/search" \
     -H "accept: application/json" \
     -H "Content-Type: application/json" \
     -d '{"query": "criptografia de dados", "limit": 2, "filter_document_id": "87c4f420-ba12-421c-81bf-6548a3ee264d"}'
```
*Resposta esperada (HTTP 200 OK):*
```json
{
  "query": "criptografia de dados",
  "total_results": 2,
  "results": [
    {
      "chunk_id": "87c4f420-ba12-421c-81bf-6548a3ee264d_chunk_7",
      "text": "Todos os dados em trânsito e em repouso devem ser cifrados utilizando chaves robustas padrão AES-256.",
      "similarity": 0.8142,
      "metadata": {
        "source_doc_id": "87c4f420-ba12-421c-81bf-6548a3ee264d",
        "filename": "manual_ti.pdf",
        "chunk_index": 7
      }
    }
  ]
}
```

---

## 📊 19. Métricas e Observabilidade

O DocMind incorpora logging estruturado utilizando a biblioteca **Loguru**:
- **Níveis e Cores de Log:** Desenvolvido de forma amigável com formatação colorida no console em desenvolvimento, identificando etapas de `lifespan` da aplicação, conexões de infraestrutura, `HIT/MISS` de cache e thresholds de corte.
- **Rastreabilidade de Busca (Recall):** Durante a fase de recuperação semântica no banco vetorial, a aplicação imprime no terminal os identificadores, nomes de arquivos, similaridades de cosseno e o início do texto de cada um dos chunks retornados. Chunks descartados por ficarem abaixo do threshold dinâmico ou por restrições de limite de tamanho de contexto também são explicitamente logados com seus respectivos motivos.
- **Auditoria de Requisições:** Auditoria estruturada no log (`[REQUEST_AUDIT]`) de todos os payloads recebidos pelos endpoints da API e das estruturas de dados enviadas ao Google Gemini (`[LLM_CALL]`), contendo informações das dimensões do embedding gerado, modelo ativo e quantidade de caracteres e tokens estimados submetidos ao LLM.

---

## 💾 20. Estratégia de Caching Semântico

O cache do pipeline RAG otimiza a latência e reduz as chamadas caras à API externa do Gemini:
1. **Deduplicação de Perguntas Identificadas:** O SHA-256 é computado a partir de perguntas normalizadas (minúsculo, sem pontuação terminal e sem espaços repetidos).
2. **Ciclo de Expiração (TTL):** As chaves são persistidas por 3600 segundos (1 hora) por padrão. O TTL reinicia a cada gravação.
3. **Desativação Inteligente (Graceful Degradation):** Em caso de falha de conexão com o cluster Redis local, o pipeline RAG captura a exceção, desativa o cache de forma transparente e repassa todas as buscas e gerações diretamente no banco vetorial e no modelo de IA, sem gerar timeouts ou erros de resposta para o cliente web.
4. **Flushing do Cache:** O comando Redis `FLUSHDB` é invocado para invalidar chaves antigas sempre que novos parâmetros são calculados, quando documentos são excluídos ou em reprocessing CLI.

---

## 🎯 21. Estratégia de Recuperação e Re-ranking Híbrido

O sistema implementa regras estritas de recuperação baseadas nas especificidades e tipos de perguntas submetidas:

1. **Classificação e Boost por Palavras-Chave de Currículo (CV):**
   O RAGService verifica dinamicamente através de regex se a pergunta tem intenção de encontrar dados profissionais, experiências, históricos de carreira, habilidades ou tópicos de formação (como `"currículo"`, `"experiência"`, `"trabalhou"`).
   - Se for uma pergunta sobre currículo:
     - Ele força a recuperação de um limite dinâmico de **12 chunks** (em vez do limite padrão de 10 da API).
     - Ele executa uma expansão de query injetando palavras-chave estruturadas no embedding (`"experiência profissional empresas trabalhou cargo emprego histórico profissional carreira currículo trabalho cargo"`).
     - Ele calcula um boost de score de até `+0.3` proporcional à quantidade de palavras-chave encontradas nos fragmentos candidatos.
     - Ele reduz o limiar mínimo de similaridade dinâmico do threshold (baseline dividida por 2, ex.: `max_similarity * 0.50`) e força o envio de **pelo menos os 10 primeiros fragmentos** ao LLM Gemini para garantir que toda a linha temporal de contratações do profissional seja apresentada de forma unificada e lógica, evitando respostas que omitam cargos antigos.

2. **Re-ranking Híbrido:**
   Cada candidato de similaridade vetorial do ChromaDB passa por um cálculo híbrido de reordenação:
   $$\text{Score Híbrido} = (0.7 \times \text{Similaridade Vetorial Cosseno}) + (0.3 \times \text{Token Overlap Ratio})$$
   Onde o *Token Overlap Ratio* é a interseção entre o conjunto de termos (tokens) únicos da pergunta e o conjunto de termos do fragmento de texto analisado, limpando stop-words clássicas de Português e Inglês.

3. **Filtragem por Threshold Dinâmico:**
   O limiar de corte de blocos de texto não é fixo. Ele adapta-se conforme o maior score híbrido encontrado nos resultados recuperados:
   - Se o maior score híbrido da lista for inferior à baseline (`settings.RAG_MIN_SIMILARITY`), o threshold mínimo de corte é definido como `max_hybrid_score * 0.70`.
   - Se o maior score híbrido for superior, o limiar é definido como o maior valor entre `max_hybrid_score * 0.70` e a baseline.
   Isso permite que perguntas difíceis (com pontuações vetoriais baixas) ainda consigam contexto, e perguntas altamente específicas façam um filtro rigoroso.

4. **Fallback Top-K:**
   Caso o filtro dinâmico descarte todos os chunks recuperados, o RAGService reativa os **3 primeiros chunks** do topo da lista HNSW como fallback de garantia, assegurando que o Gemini possua ao menos alguma fonte próxima de informação técnica para responder.

5. **Controle de Caracteres Máximos de Contexto:**
   O tamanho acumulado dos chunks selecionados é validado. Se a inserção de um chunk adicional fizer o contexto ultrapassar **25.000 caracteres**, o processamento interrompe a inclusão e descarta os chunks subsequentes para evitar o estouro da janela de contexto aceita pelo Gemini.

---

## 📋 22. Checklist antes do Push para o GitHub

Antes de realizar o commit e push de novas alterações para o GitHub, siga rigorosamente as etapas de validação abaixo para mitigar riscos de vazamento de credenciais e quebra de builds:

- [ ] **Limpeza de Credenciais Sensíveis:**
  - Verifique se a variável `GOOGLE_API_KEY` do arquivo `.env` não foi exposta em commits do git ou em arquivos temporários.
  - Verifique se a variável `SECRET_KEY` foi alterada para um valor padrão seguro nas configurações globais e se as chaves em produção são injetadas exclusivamente por variáveis de ambiente de runtime do servidor.
- [ ] **Configuração do .gitignore:**
  - Garanta que o diretório de dados locais (`data/`) esteja completamente listado no `.gitignore` para evitar o commit de arquivos PDFs confidenciais dos usuários em `data/uploads/`, bases do banco vetorial em `data/chromadb/` e logs de tarefas de processamento em `data/tasks.json`.
- [ ] **Integridade e Execução das Suítes de Testes:**
  - Certifique-se de executar todas as suítes de testes automatizados locais para identificar bugs e regressões funcionais na API:
    ```bash
    .\venv\Scripts\python -m pytest tests/ -v
    ```
- [ ] **Validação do startup local:**
  - Execute a inicialização do FastAPI e do worker em segundo plano para constatar que as configurações e injeções de variáveis de ambiente não causam quebras de importação ou travamentos inesperados.

---

## 🔒 23. Variáveis Obrigatórias para Produção

Em ambientes de produção, as seguintes variáveis de ambiente **devem ser explicitamente configuradas e injetadas** no servidor host ou orquestrador (Docker/Kubernetes):

1. **`GOOGLE_API_KEY`**: Chave de produção com permissão de leitura nos modelos Gemini do Google AI Studio. **Nunca armazene essa chave de forma fixa no código.**
2. **`SECRET_KEY`**: Chave forte de criptografia aleatória usada para geração de tokens corporativos seguros.
3. **`APP_ENV`**: Deve ser definido obrigatoriamente como `production` para desligar detalhes de debug e logs informais no console, ativando logs estruturados em formato JSON compatível com agregadores de mercado.
4. **`DEBUG`**: Deve ser definido como `false`.
5. **`REDIS_URL`**: URL apontando para a instância Redis de produção (ex.: `redis://redis-cluster.internal:6379/0`).
6. **`RABBITMQ_URL`**: URL contendo credenciais de produção e restrições de rede isoladas (ex.: `amqp://api_user:prod_pass@rabbitmq-broker.internal:5672/vhost_prod`).

---

## 🔍 24. Controle de Inconsistências e Segurança

Ao analisar o código da plataforma DocMind, foram encontradas as seguintes vulnerabilidades técnicas e divergências estruturais que precisam ser sanadas em ciclos futuros de desenvolvimento:

### 🚨 Riscos de Segurança Identificados
1. **Vazamento do Arquivo `data/tasks.json` no Controle de Versão (Git):**
   O arquivo `data/tasks.json` (que armazena o histórico e progresso de tarefas da aplicação em tempo de execução) está atualmente versionado e sendo rastreado pelo Git. Ele é modificado a cada requisição de processamento, o que gera ruídos nos commits do repositório, conflitos frequentes de merge e expõe dados internos de auditoria e payloads das requisições corporativas dos usuários.
2. **Exposição de Chaves Sensíveis no `.env` Local:**
   O arquivo de desenvolvimento `.env` (que contém uma chave `GOOGLE_API_KEY` válida ou seu formato antigo de testes) não está sendo excluído de forma adequada em commits mais antigos do histórico do git caso já tenha sido commitado anteriormente.
3. **Diretórios de Uploads e Bancos Vetoriais sem Regras de Exclusão no `.gitignore`:**
   O `.gitignore` do projeto ignora caminhos genéricos como `chromadb/` e `data/chroma/`, mas não contém regras explícitas para impedir o commit das pastas `data/uploads/` e `data/chromadb/`. Isso pode fazer com que arquivos confidenciais submetidos à API de upload do DocMind e índices vetoriais pesados de desenvolvimento sejam enviados acidentalmente para o repositório público do GitHub.

### ⚠️ Inconsistências Técnicas entre Código, Testes e Configurações
1. **AttributeError Crítico com `settings.RAG_CONTEXT_CHUNKS`:**
   No arquivo `app/services/rag_service.py` (linha 196), a busca RAG tenta recuperar a variável de configuração `settings.RAG_CONTEXT_CHUNKS`. Contudo, esta variável não existe em `app/core/config.py`. Isso provoca uma falha de `AttributeError` em runtime se uma pergunta for enviada com o campo `limit` nulo no JSON.
2. **Função de Teste Inexistente (`is_summary_query`):**
   O arquivo `tests/test_rag.py` tenta testar uma função utilitária chamada `is_summary_query` importando-a de `app.services.rag_service`. Esta função não está implementada no código-fonte do serviço RAG, fazendo o teste quebrar com `ImportError`.
3. **Teste de Jaccard Diversity Sem Implementação de Código:**
   A suíte de testes `test_rag_service_jaccard_diversity_filtering` assume que o DocMind implementa filtragem por similaridade de vocabulário baseada em distância Jaccard (descartando trechos redundantes que compartilhem mais de 65% de palavras). Esta regra de negócio não está no arquivo `rag_service.py`, gerando falha de asserção nos testes.
4. **Ignorância da Variável `MAX_CONTEXT_TOKENS` no Código:**
   A configuração `settings.MAX_CONTEXT_TOKENS` (valor `12000` em `config.py` e `.env.example`) é ignorada pelo RAGService. A limitação física de tokens para evitar o estouro de contexto no Gemini é feita por uma verificação de tamanho em caracteres estática (`MAX_CONTEXT_CHARS = 25000`) no código do serviço. Isso invalida as alterações de limites realizadas pelos operadores no `.env` e faz o teste `test_rag_service_token_limit_handling` quebrar por tentar simular limites mais baixos.
5. **Divergências de Parâmetros Padrão:**
   Existem disparidades nos limites configurados por padrão nos arquivos da aplicação:
   - `GEMINI_MODEL`: Definido como `gemini-2.5-flash` em `config.py` mas listado como `gemini-1.5-pro` no antigo `.env.example`.
   - `LLM_MAX_TOKENS`: Definido como `2048` em `config.py` mas configurado como `1024` no antigo `.env.example`.
   - `RAG_MIN_SIMILARITY`: Definido como `0.25` em `config.py` mas configurado como `0.35` no `.env.example`.
   - `CHUNK_SIZE` e `CHUNK_OVERLAP`: Padrões de `1000` e `150` no `config.py`, mas `500` e `50` no antigo `.env.example`.
6. **Variáveis de Configuração Inativas (Sem Uso):**
   As variáveis `PERSIST_DIRECTORY` (redundância do ChromaDB) e `RABBITMQ_DOCUMENT_QUEUE` (nome alternativo da fila) estão declaradas no `config.py` e `.env.example`, mas nunca são lidas ou referenciadas por nenhum módulo de negócio da aplicação.

---

## 🗺️ 25. Melhorias Futuras (Roadmap)

Para as próximas iterações do desenvolvimento do DocMind, recomenda-se a inclusão dos seguintes itens no roadmap técnico:

- [ ] **Correção do Bug `RAG_CONTEXT_CHUNKS`:** Substituir a chamada indevida em `rag_service.py` para usar a variável padrão existente `settings.DEFAULT_CONTEXT_CHUNKS`.
- [ ] **Alinhamento dos Testes do RAGService:**
  - Implementar a função `is_summary_query` para que perguntas sobre resumos aumentem o tamanho do contexto de forma nativa e o teste associado passe.
  - Implementar a filtragem por diversidade de Jaccard no pipeline RAG para remover chunks semanticamente equivalentes e otimizar a diversidade de conteúdo enviado ao Gemini.
  - Ajustar o limitador de tokens do contexto em `rag_service.py` para respeitar dinamicamente o valor de `settings.MAX_CONTEXT_TOKENS` configurado no arquivo `.env`.
- [ ] **Tratamento do `.gitignore` para Dados Locais:** Remover o arquivo `data/tasks.json` do controle de versão do Git usando `git rm --cached data/tasks.json` e adicionar as regras corretas de exclusão para toda a pasta `data/` no `.gitignore`.
- [ ] **Autenticação JWT com Segurança Corporativa:** Adicionar middleware de autenticação nas rotas da API, utilizando o `pyjwt` e `passlib[bcrypt]` já listados nas dependências do projeto para controle de acesso de usuários.
- [ ] **Isolamento de Coleções por Usuário (Multi-tenant RAG):** Permitir que o ChromaDB crie coleções específicas baseadas em tokens ou metadados de projetos de usuários, separando os documentos indexados de forma lógica e segura.
- [ ] **Adicionar Suporte a Novos Formatos de Arquivos:** Estender o `DocumentProcessorService` para processar e extrair dados de planilhas Excel (`.xlsx`), documentos Word (`.docx`) e arquivos de texto simples (`.txt`).
