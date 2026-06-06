import uuid
from datetime import datetime
import os
from fastapi import APIRouter, File, UploadFile, status, HTTPException
from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.document import DocumentUploadResponse, DocumentMetadata
from app.schemas.semantic import SemanticProcessResponse
from app.schemas.task import TaskCreate, TaskStatus
from app.services.document_processor import document_processor
from app.services.message_queue_service import rabbitmq_service
from app.services.semantic_processor import semantic_processor
from app.services.task_service import task_service
from app.services.vector_store import vector_store

router = APIRouter()


@router.post(
    "/upload",
    response_model=DocumentUploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Fazer upload de documento PDF ou Markdown (processamento assíncrono)",
    description=(
        "Recebe um arquivo (PDF ou Markdown), valida as restrições de tipo e tamanho, "
        "salva em disco local, extrai o conteúdo textual e enfileira o processamento "
        "semântico assíncrono via RabbitMQ. Retorna imediatamente com um task_id "
        "para rastreamento do processamento em background."
    ),
)
async def upload_document(
    file: UploadFile = File(..., description="Arquivo de documento a ser ingerido (.pdf ou .md)")
) -> DocumentUploadResponse:
    BaseLogger.info(
        f"[Upload] Requisição recebida: arquivo={file.filename} | tipo={file.content_type}"
    )

    # 1. Ler bytes do arquivo de forma assíncrona
    file_content = await file.read()

    # 2. Processar documento (validação, salvamento em disco, extração de texto)
    doc_id, cleaned_text, metadata = document_processor.process_document(
        file_content=file_content,
        filename=file.filename or "documento_desconhecido",
        content_type=file.content_type or "application/octet-stream"
    )

    # 3. Gerar task_id e criar tarefa de rastreamento
    task_id = str(uuid.uuid4())
    task_input = TaskCreate(
        task_id=task_id,
        document_id=doc_id,
        filename=file.filename or "documento_desconhecido",
    )
    task_service.create_task(task_input)

    BaseLogger.info(
        f"[Upload] Tarefa criada: task_id={task_id} | doc_id={doc_id}"
    )

    # 4. Publicar mensagem na fila RabbitMQ para processamento assíncrono
    published = rabbitmq_service.publish_document_processing(
        document_id=doc_id,
        task_id=task_id,
        filename=file.filename or "documento_desconhecido",
    )

    if not published:
        # Broker indisponível: atualiza task para FAILED e ainda retorna os metadados
        # para o cliente não perder o document_id (pode ser reprocessado manualmente)
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.FAILED,
            progress=0,
            message="Falha ao publicar na fila. Broker RabbitMQ indisponível.",
            error_detail="RabbitMQ não pôde ser alcançado no momento do upload.",
        )
        BaseLogger.error(
            f"[Upload] ✗ Falha ao publicar na fila RabbitMQ: task_id={task_id}"
        )
        queue_status = "queue_failed"
        queue_message = (
            "Arquivo salvo mas falha ao enfileirar processamento. "
            "Verifique o RabbitMQ e use o endpoint /{document_id}/process manualmente."
        )
    else:
        queue_status = "queued"
        queue_message = (
            f"Documento enfileirado com sucesso. "
            f"Acompanhe o processamento em: GET /api/v1/tasks/{task_id}"
        )
        BaseLogger.info(
            f"[Upload] ✓ Documento enfileirado: task_id={task_id} | "
            f"doc_id={doc_id} | fila={settings.RABBITMQ_DOCUMENT_QUEUE}"
        )

    # 5. Gerar excerpt para preview
    excerpt = cleaned_text[:200] + "..." if len(cleaned_text) > 200 else cleaned_text

    return DocumentUploadResponse(
        document_id=doc_id,
        filename=metadata.filename,
        status=queue_status,
        message=queue_message,
        metadata=metadata,
        excerpt=excerpt,
        task_id=task_id,
    )


@router.post(
    "/{document_id}/process",
    response_model=SemanticProcessResponse,
    status_code=status.HTTP_200_OK,
    summary="Processar semanticamente um documento (síncrono — fallback manual)",
    description=(
        "Localiza o arquivo anteriormente salvo pelo ID fornecido, extrai o texto, "
        "realiza segmentação (chunking) semântica inteligente de acordo com as configurações, "
        "gera vetores de embeddings em lote para cada fragmento e retorna os chunks com metadados e vetores. "
        "Este endpoint é mantido como fallback manual para reprocessamento, pois o fluxo principal "
        "é assíncrono via RabbitMQ (endpoint /upload)."
    ),
)
async def process_document_semantically(document_id: str) -> SemanticProcessResponse:
    BaseLogger.info(f"[Process] Iniciando processamento semântico manual: doc_id={document_id}")

    # 1. Localizar arquivo em UPLOAD_DIR que inicia com o ID fornecido
    target_filename = None
    if os.path.exists(settings.UPLOAD_DIR):
        for filename in os.listdir(settings.UPLOAD_DIR):
            if filename.startswith(f"{document_id}_"):
                target_filename = filename
                break

    if not target_filename:
        BaseLogger.warning(f"[Process] Tentativa de processamento de ID inexistente: {document_id}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Documento não encontrado. Certifique-se de que o upload foi concluído com sucesso."
        )

    file_path = os.path.join(settings.UPLOAD_DIR, target_filename)
    original_filename = target_filename[len(document_id) + 1:]
    file_ext = os.path.splitext(original_filename)[1].lower()

    # 2. Extração de texto dependendo da extensão
    raw_text = ""
    page_count = None

    if file_ext == ".pdf":
        raw_text, page_count = document_processor.extract_text_from_pdf(file_path)
    elif file_ext == ".md":
        raw_text = document_processor.extract_text_from_markdown(file_path)

    # 3. Limpeza de texto
    cleaned_text = document_processor.clean_text(raw_text)
    file_size = os.path.getsize(file_path)

    # 4. Recriar metadados básicos do documento original
    content_type = "application/pdf" if file_ext == ".pdf" else "text/markdown"
    metadata = DocumentMetadata(
        filename=original_filename,
        file_size_bytes=file_size,
        content_type=content_type,
        page_count=page_count,
        char_count=len(cleaned_text),
        uploaded_at=datetime.utcnow()
    )

    # 5. Processamento semântico: segmentação e geração de embeddings
    response = semantic_processor.process_text_into_chunks(
        document_id=document_id,
        text=cleaned_text,
        doc_metadata=metadata
    )

    # 6. Persistência no banco vetorial ChromaDB
    if response.chunks:
        persisted = vector_store.upsert_chunks(response.chunks)
        if not persisted:
            BaseLogger.warning(
                f"[Process] Processamento semântico concluído mas falha ao persistir "
                f"{len(response.chunks)} chunks do documento {document_id} no ChromaDB."
            )
        else:
            BaseLogger.info(
                f"[Process] {len(response.chunks)} chunks do documento {document_id} "
                f"persistidos no ChromaDB."
            )

    return response
