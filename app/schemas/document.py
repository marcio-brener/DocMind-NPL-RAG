from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class DocumentMetadata(BaseModel):
    filename: str = Field(..., description="Nome original do arquivo")
    file_size_bytes: int = Field(..., description="Tamanho do arquivo em bytes")
    content_type: str = Field(..., description="Tipo MIME do arquivo")
    page_count: Optional[int] = Field(None, description="Número de páginas extraídas (apenas para PDF)")
    char_count: int = Field(..., description="Quantidade total de caracteres extraídos")
    uploaded_at: datetime = Field(default_factory=datetime.utcnow, description="Data e hora do upload (UTC)")


class DocumentUploadResponse(BaseModel):
    document_id: str = Field(..., description="Identificador único (UUID) do documento gerado")
    status: str = Field(..., description="Status do processamento (ex: 'queued', 'processed', 'failed')")
    message: str = Field(..., description="Mensagem de retorno informativa")


class DocumentReprocessResponse(BaseModel):
    message: str = Field(..., description="Mensagem de status do reprocessamento")
    total_processed: int = Field(..., description="Quantidade total de documentos reprocessados com sucesso")
    details: list = Field(..., description="Detalhes de cada documento reprocessado")
