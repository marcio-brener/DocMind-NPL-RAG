from typing import Dict, Optional
from pydantic import BaseModel, Field


class ServiceStatus(BaseModel):
    status: str = Field(..., description="Status do serviço específico (ex: 'healthy', 'unhealthy')")
    latency_ms: Optional[float] = Field(None, description="Latência de resposta em milissegundos")
    details: Optional[str] = Field(None, description="Detalhes adicionais ou mensagens de erro")


class HealthResponse(BaseModel):
    status: str = Field(..., description="Status geral do sistema (ex: 'ok', 'degraded', 'error')")
    environment: str = Field(..., description="Ambiente de execução atual")
    version: str = Field("1.0.0", description="Versão atual da API")
    services: Dict[str, ServiceStatus] = Field(
        default_factory=dict, 
        description="Estado detalhado de cada serviço dependente (Banco Vetorial, Redis, RabbitMQ)"
    )
