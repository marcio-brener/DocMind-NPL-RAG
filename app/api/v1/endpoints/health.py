import time
# pyrefly: ignore [missing-import]
from fastapi import APIRouter, status
from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.health import HealthResponse, ServiceStatus

router = APIRouter()


@router.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Verificar saúde da API",
    description="Retorna o status geral da aplicação e a integridade de seus componentes de infraestrutura.",
)
async def check_health() -> HealthResponse:
    BaseLogger.debug("Health check requisitado.")

    # Medição preliminar simulada para dependências que serão configuradas nas próximas etapas
    # Na Etapa 5 e 6, implementaremos checagens reais de conexão.
    services_status = {
        "chromadb": ServiceStatus(
            status="healthy (mock)",
            latency_ms=0.5,
            details=f"Pasta de persistência definida: {settings.CHROMADB_PATH}",
        ),
        "redis": ServiceStatus(
            status="not_connected (mock)",
            details="Conexão com Redis será estabelecida na Etapa 5",
        ),
        "rabbitmq": ServiceStatus(
            status="not_connected (mock)",
            details="Conexão com RabbitMQ será estabelecida na Etapa 5",
        ),
    }

    return HealthResponse(
        status="ok",
        environment=settings.APP_ENV,
        version="1.0.0",
        services=services_status,
    )
