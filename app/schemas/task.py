from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class TaskStatus(str, Enum):
    """
    Enum de status possíveis para uma tarefa de processamento de documento.
    """
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TaskCreate(BaseModel):
    """
    Schema para criação de uma nova tarefa de processamento.
    """
    task_id: str = Field(..., description="Identificador único da tarefa (UUID)")
    document_id: Optional[str] = Field(None, description="ID do documento associado à tarefa")
    filename: Optional[str] = Field(None, description="Nome do arquivo que será processado")


class TaskResponse(BaseModel):
    """
    Schema de resposta para consulta de status de uma tarefa.
    """
    task_id: str = Field(..., description="Identificador único da tarefa")
    document_id: Optional[str] = Field(None, description="ID do documento associado")
    filename: Optional[str] = Field(None, description="Nome do arquivo sendo processado")
    status: TaskStatus = Field(..., description="Status atual da tarefa")
    progress: int = Field(
        default=0,
        ge=0,
        le=100,
        description="Progresso percentual do processamento (0-100)"
    )
    message: str = Field(
        default="",
        description="Mensagem descritiva sobre o estado atual da tarefa"
    )
    created_at: datetime = Field(..., description="Data e hora de criação da tarefa (UTC)")
    updated_at: datetime = Field(..., description="Data e hora da última atualização (UTC)")
    error_detail: Optional[str] = Field(
        default=None,
        description="Detalhes do erro em caso de falha (status=FAILED)"
    )
    result: Optional[dict] = Field(
        default=None,
        description="Resultado final retornado pela tarefa (ex: resposta RAG)"
    )
    error: Optional[str] = Field(
        default=None,
        description="Mensagem de erro em caso de falha"
    )
