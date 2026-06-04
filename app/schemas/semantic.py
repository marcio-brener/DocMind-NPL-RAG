from typing import Any, Dict, List
from pydantic import BaseModel, Field


class Chunk(BaseModel):
    id: str = Field(..., description="Identificador único (UUID) do chunk")
    text: str = Field(..., description="Conteúdo textual recortado")
    embedding: List[float] = Field(..., description="Vetor denso (embedding) gerado para o chunk")
    metadata: Dict[str, Any] = Field(
        default_factory=dict, 
        description="Metadados herdados do documento (ex: filename, char_count, chunk_index)"
    )


class SemanticProcessResponse(BaseModel):
    document_id: str = Field(..., description="ID único do documento original ingerido")
    total_chunks: int = Field(..., description="Total de fragmentos gerados após a divisão")
    chunks: List[Chunk] = Field(..., description="Lista completa de fragmentos semânticos e seus vetores")
