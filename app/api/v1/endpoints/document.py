from datetime import datetime
import os
from fastapi import APIRouter, File, UploadFile, status, HTTPException
from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.document import DocumentUploadResponse, DocumentMetadata
from app.schemas.semantic import SemanticProcessResponse
from app.services.document_processor import document_processor
from app.services.semantic_processor import semantic_processor
from app.services.vector_store import vector_store

router = APIRouter()


@router.post(
    "/upload",
    response_model=DocumentUploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Fazer upload de documento PDF ou Markdown",
    description=(
        "Recebe um arquivo (PDF ou Markdown), valida as restrições de tipo e tamanho, "
        "salva em disco local de auditoria, extrai e sanitiza todo o conteúdo textual e "
        "retorna os metadados gerados junto com uma prévia (excerpt) do texto legível."
    ),
)
async def upload_document(
    file: UploadFile = File(..., description="Arquivo de documento a ser ingerido (.pdf ou .md)")
) -> DocumentUploadResponse:
    BaseLogger.info(f"Requisição de upload recebida. Arquivo: {file.filename}, Tipo: {file.content_type}")

    # Ler os bytes do arquivo de forma assíncrona
    file_content = await file.read()

    # Processar o documento por meio do serviço
    doc_id, cleaned_text, metadata = document_processor.process_document(
        file_content=file_content,
        filename=file.filename or "documento_desconhecido",
        content_type=file.content_type or "application/octet-stream"
    )

    # Gerar trecho inicial como prévia (excerpt)
    excerpt = cleaned_text[:200] + "..." if len(cleaned_text) > 200 else cleaned_text

    return DocumentUploadResponse(
        document_id=doc_id,
        filename=metadata.filename,
        status="processed",
        message="Documento ingerido e texto extraído com sucesso.",
        metadata=metadata,
        excerpt=excerpt,
    )


@router.post(
    "/{document_id}/process",
    response_model=SemanticProcessResponse,
    status_code=status.HTTP_200_OK,
    summary="Processar semanticamente um documento",
    description=(
        "Localiza o arquivo anteriormente salvo pelo ID fornecido, extrai o texto, "
        "realiza segmentação (chunking) semântica inteligente de acordo com as configurações, "
        "gera vetores de embeddings em lote para cada fragmento e retorna os chunks com metadados e vetores."
    ),
)
async def process_document_semantically(document_id: str) -> SemanticProcessResponse:
    BaseLogger.info(f"Iniciando processamento semântico para o documento ID: {document_id}")

    # 1. Localizar arquivo em UPLOAD_DIR que inicia com o ID fornecido
    target_filename = None
    if os.path.exists(settings.UPLOAD_DIR):
        for filename in os.listdir(settings.UPLOAD_DIR):
            if filename.startswith(f"{document_id}_"):
                target_filename = filename
                break

    if not target_filename:
        BaseLogger.warning(f"Tentativa de processamento de ID inexistente: {document_id}")
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
                f"Processamento semântico concluído mas falha ao persistir {len(response.chunks)} "
                f"chunks do documento {document_id} no ChromaDB."
            )
        else:
            BaseLogger.info(
                f"{len(response.chunks)} chunks do documento {document_id} persistidos no ChromaDB."
            )

    return response

