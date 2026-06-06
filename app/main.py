from contextlib import asynccontextmanager
from typing import AsyncGenerator
# pyrefly: ignore [missing-import]
from fastapi import FastAPI, Request, status
# pyrefly: ignore [missing-import]
from fastapi.middleware.cors import CORSMiddleware
# pyrefly: ignore [missing-import]
from fastapi.responses import JSONResponse, RedirectResponse
from app.api.router import api_router
from app.core.config import settings
from app.core.logging import BaseLogger, setup_logging
from app.services.message_queue_service import rabbitmq_service
from app.services.task_service import task_service


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    # Inicialização na inicialização do servidor
    setup_logging()
    BaseLogger.info(f"Iniciando {settings.APP_NAME} no ambiente {settings.APP_ENV}...")

    # Inicializar TaskService (garante que o arquivo tasks.json exista)
    BaseLogger.info("[Lifespan] TaskService inicializado.")
    _ = task_service  # acesso ao singleton dispara __init__

    # Conectar ao RabbitMQ (falha não é bloqueante — app continua sem fila)
    connected = rabbitmq_service.connect()
    if connected:
        BaseLogger.info(
            f"[Lifespan] RabbitMQ conectado. "
            f"Filas: '{settings.RABBITMQ_DOCUMENT_QUEUE}', '{settings.RABBITMQ_RAG_QUEUE}'"
        )
    else:
        BaseLogger.warning(
            "[Lifespan] RabbitMQ indisponível no startup. "
            "O upload assync estará degradado até o broker ficar ativo."
        )

    yield

    # Limpeza e encerramento de conexões
    rabbitmq_service.close()
    BaseLogger.info(f"Encerrando {settings.APP_NAME}...")



def create_application() -> FastAPI:
    """
    Cria e configura a instância principal do FastAPI.
    """
    app = FastAPI(
        title=settings.APP_NAME,
        description="Plataforma Corporativa de Processamento de Linguagem Natural com RAG e Agentes de IA.",
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # Configuração de CORS Middleware
    if settings.BACKEND_CORS_ORIGINS:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # Inclusão do roteador de APIs
    app.include_router(api_router)

    # Tratamento de exceções globais para resposta padronizada de erro 500
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        BaseLogger.error(f"Erro interno não tratado em {request.url.path}: {str(exc)}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "Um erro interno ocorreu no servidor.",
                "type": "InternalServerError",
            },
        )

    # Rota raiz - Redirecionamento amigável para o Swagger UI (/docs)
    @app.get("/", include_in_schema=False)
    async def root_redirect() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    return app


app = create_application()
