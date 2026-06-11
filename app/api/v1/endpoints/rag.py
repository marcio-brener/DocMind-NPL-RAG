import uuid
from datetime import datetime
from fastapi import APIRouter, status, HTTPException
from app.core.logging import BaseLogger
from app.schemas.rag import RAGRequest, RAGAskResponse, RAGResultResponse
from app.services.rabbitmq_service import rabbitmq_service
from app.services.cache_service import cache_service

router = APIRouter()


@router.post(
    "/ask",
    response_model=RAGAskResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Fazer uma pergunta ao sistema RAG",
    description=(
        "Recebe uma pergunta em linguagem natural, gera um request_id único, "
        "publica na fila de mensageria e retorna o ID para consulta posterior."
    ),
)
async def ask_question(payload: RAGRequest) -> RAGAskResponse:
    task_id = str(uuid.uuid4())
    request_id = task_id
    timestamp = datetime.utcnow().isoformat()
    
    BaseLogger.info(f"Endpoint RAG /ask requisitado. Pergunta: '{payload.question[:60]}' | task_id={task_id} | request_id={request_id}")
    BaseLogger.info(
        f"[REQUEST_AUDIT] Payload recebido: {payload.model_dump()}"
    )

    
    # Criar a tarefa inicial no TaskService usando task_id
    from app.schemas.task import TaskCreate, TaskStatus
    from app.services.task_service import task_service
    
    task_input = TaskCreate(
        task_id=task_id,
        document_id=None,
        filename=None
    )
    task_service.create_task(task_input)
    
    # Publicar no RabbitMQ (informa ambos os IDs)
    published = await rabbitmq_service.publish_rag_request(
        task_id=task_id,
        request_id=request_id,
        question=payload.question,
        limit=payload.limit,
        filter_document_id=payload.filter_document_id
    )
    
    if not published:
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.FAILED,
            progress=0,
            message="Falha ao enfileirar pergunta RAG no broker de mensageria."
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Falha ao enfileirar pergunta RAG no broker de mensageria."
        )
        
    return RAGAskResponse(
        task_id=task_id,
        request_id=request_id,
        status="PROCESSING",
        timestamp=timestamp
    )


@router.get(
    "/result/{request_id}",
    response_model=RAGResultResponse,
    status_code=status.HTTP_200_OK,
    summary="Consultar resultado de uma pergunta RAG",
    description="Consulta o Redis para verificar se a resposta da pergunta já foi processada."
)
async def get_rag_result(request_id: str) -> RAGResultResponse:
    redis_key = f"rag_response:{request_id}"
    cached_data = await cache_service.get(redis_key)
    
    if cached_data is None:
        return RAGResultResponse(status="PROCESSING")
        
    return RAGResultResponse(
        status="COMPLETED",
        request_id=cached_data.get("request_id"),
        answer=cached_data.get("answer"),
        sources=cached_data.get("sources", [])
    )
