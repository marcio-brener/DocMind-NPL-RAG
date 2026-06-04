from fastapi import APIRouter, status
from app.core.logging import BaseLogger
from app.schemas.rag import RAGRequest, RAGResponse
from app.services.rag_service import rag_service

router = APIRouter()


@router.post(
    "/ask",
    response_model=RAGResponse,
    status_code=status.HTTP_200_OK,
    summary="Fazer uma pergunta ao sistema RAG",
    description=(
        "Recebe uma pergunta em linguagem natural, busca os fragmentos de documentos mais "
        "relevantes no banco vetorial, monta um prompt estruturado com o contexto recuperado "
        "e gera uma resposta utilizando o LLM configurado. "
        "Caso nenhum contexto seja encontrado ou a chave da API do LLM não esteja configurada, "
        "o sistema ativa um fallback inteligente sem interromper o serviço."
    ),
)
async def ask_question(payload: RAGRequest) -> RAGResponse:
    BaseLogger.info(f"Endpoint RAG /ask requisitado. Pergunta: '{payload.question[:60]}'")
    return await rag_service.answer(payload)
