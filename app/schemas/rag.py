from typing import List, Optional
from pydantic import BaseModel, Field


class RAGRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        description="Pergunta do usuário para ser respondida com base nos documentos ingeridos"
    )
    limit: int = Field(
        default=4,
        ge=1,
        le=10,
        description="Número máximo de chunks de contexto a usar na geração da resposta"
    )


class SourceReference(BaseModel):
    chunk_id: str = Field(..., description="ID único do chunk utilizado como fonte")
    filename: str = Field(..., description="Nome do documento de origem")
    excerpt: str = Field(..., description="Trecho do fragmento relevante (até 200 chars)")
    similarity: float = Field(..., ge=0.0, le=1.0, description="Score de similaridade com a pergunta")


class RAGResponse(BaseModel):
    question: str = Field(..., description="Pergunta original do usuário")
    answer: str = Field(..., description="Resposta gerada pelo LLM com base no contexto recuperado")
    sources: List[SourceReference] = Field(..., description="Fragmentos de documentos utilizados como contexto")
    context_found: bool = Field(..., description="Indica se contexto relevante foi encontrado no banco vetorial")
    llm_used: bool = Field(..., description="Indica se o LLM foi chamado para gerar a resposta")
    cache_hit: bool = Field(default=False, description="Indica se a resposta foi recuperada do cache Redis")
    latency_ms: float = Field(..., description="Tempo total de processamento da requisição em milissegundos")
