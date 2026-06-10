from fastapi import APIRouter, status
from app.core.logging import BaseLogger
from app.schemas.search import SearchRequest, SearchResponse, SearchResultItem
from app.services.embedding_service import embedding_service
from app.services.vector_store import vector_store

router = APIRouter()


@router.post(
    "/search",
    response_model=SearchResponse,
    status_code=status.HTTP_200_OK,
    summary="Busca semântica no banco vetorial",
    description=(
        "Recebe uma query textual, gera seu vetor de embedding e executa uma busca de similaridade "
        "de cosseno no ChromaDB. Retorna os fragmentos (chunks) mais semanticamente próximos "
        "à pergunta, ordenados por relevância decrescente."
    ),
)
async def semantic_search(payload: SearchRequest) -> SearchResponse:
    BaseLogger.info(f"Busca semântica recebida: '{payload.query[:80]}...' (limit={payload.limit})")

    # 1. Gerar o embedding da query do usuário
    query_vector = embedding_service.embed_query(payload.query)

    # 2. Executar a busca de similaridade no ChromaDB
    where_filter = None
    if payload.filter_document_id:
        where_filter = {"source_doc_id": payload.filter_document_id}

    raw_results = vector_store.similarity_search(
        query_vector=query_vector,
        limit=payload.limit,
        where_filter=where_filter
    )

    # 3. Converter resultados brutos para o schema de resposta
    results = [
        SearchResultItem(
            chunk_id=item["chunk_id"],
            text=item["text"],
            similarity=item["similarity"],
            metadata=item["metadata"]
        )
        for item in raw_results
    ]

    BaseLogger.info(f"Busca semântica concluída. {len(results)} resultado(s) retornado(s).")
    return SearchResponse(
        query=payload.query,
        total_results=len(results),
        results=results
    )
