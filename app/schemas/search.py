from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str = Field(
        ...,
        min_length=3,
        description="Pergunta ou texto para busca semântica no banco vetorial"
    )
    limit: int = Field(
        default=4,
        ge=1,
        le=20,
        description="Número máximo de resultados a retornar (entre 1 e 20)"
    )
    filter_document_id: Optional[str] = Field(
        default=None,
        description="Filtrar resultados por ID de documento específico (opcional)"
    )


class SearchResultItem(BaseModel):
    chunk_id: str = Field(..., description="Identificador único do chunk retornado")
    text: str = Field(..., description="Conteúdo textual do fragmento encontrado")
    similarity: float = Field(..., ge=0.0, le=1.0, description="Score de similaridade de cosseno (0 a 1)")
    metadata: Dict[str, Any] = Field(..., description="Metadados associados ao chunk")


class SearchResponse(BaseModel):
    query: str = Field(..., description="A query original do usuário")
    total_results: int = Field(..., description="Total de resultados retornados")
    results: List[SearchResultItem] = Field(..., description="Lista de chunks mais relevantes")
