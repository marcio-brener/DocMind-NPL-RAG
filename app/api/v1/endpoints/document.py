import uuid
from datetime import datetime
import os
from fastapi import APIRouter, File, UploadFile, status, HTTPException
from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.document import DocumentUploadResponse, DocumentMetadata, DocumentReprocessResponse, DocumentDeleteResponse
from app.schemas.semantic import SemanticProcessResponse
from app.schemas.task import TaskCreate, TaskStatus
from app.services.cache_service import cache_service
from app.services.document_processor import document_processor
from app.services.rabbitmq_service import rabbitmq_service
from app.services.semantic_processor import semantic_processor
from app.services.task_service import task_service
from app.services.vector_store import vector_store

router = APIRouter()


@router.post(
    "/upload",
    response_model=DocumentUploadResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Fazer upload de documento PDF ou Markdown (processamento assíncrono)",
    description=(
        "Recebe um arquivo (PDF ou Markdown), valida as restrições de tipo e tamanho, "
        "salva em disco local, registra uma tarefa e enfileira o processamento "
        "semântico assíncrono via RabbitMQ. Retorna imediatamente com um document_id "
        "para rastreamento do processamento em background."
    ),
)
async def upload_document(
    file: UploadFile = File(..., description="Arquivo de documento a ser ingerido (.pdf ou .md)")
) -> DocumentUploadResponse:
    filename = file.filename or "documento_desconhecido"
    BaseLogger.info(
        f"[Upload] Requisição recebida: arquivo={filename} | tipo={file.content_type}"
    )

    # 1. Validação de extensão
    file_ext = os.path.splitext(filename)[1].lower()
    if file_ext not in [".pdf", ".md"]:
        BaseLogger.warning(f"[Upload] Tentativa de upload de extensão não suportada: {filename}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tipo de arquivo inválido. Apenas arquivos PDF (.pdf) e Markdown (.md) são suportados."
        )

    # 2. Ler bytes do arquivo de forma assíncrona para validação de tamanho
    file_content = await file.read()
    file_size_bytes = len(file_content)
    max_size_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024
    if file_size_bytes > max_size_bytes:
        BaseLogger.warning(f"[Upload] Arquivo excede tamanho máximo de {settings.MAX_FILE_SIZE_MB}MB: {filename}")
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Arquivo muito grande. O limite máximo permitido é de {settings.MAX_FILE_SIZE_MB} MB."
        )

    # 3. Gerar document_id (UUID) e salvar o arquivo em disco
    document_id = str(uuid.uuid4())
    safe_filename = f"{document_id}_{filename}"
    filepath = os.path.join(settings.UPLOAD_DIR, safe_filename)

    BaseLogger.info(f"[Upload] Salvando arquivo em: {filepath}")
    try:
        os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
        with open(filepath, "wb") as f:
            f.write(file_content)
    except Exception as e:
        BaseLogger.error(f"[Upload] Erro ao salvar arquivo em disco: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro interno no servidor ao persistir o arquivo enviado."
        )

    uploaded_at = datetime.utcnow().isoformat()

    # 4. Criar tarefa de rastreamento com status inicial QUEUED
    # Acoplamos o task_id diretamente ao document_id
    task_input = TaskCreate(
        task_id=document_id,
        document_id=document_id,
        filename=filename,
    )
    task_service.create_task(task_input)
    BaseLogger.info(f"[Upload] Tarefa criada para rastreamento: task_id={document_id}")

    # 5. Publicar mensagem na fila RabbitMQ para processamento assíncrono
    published = await rabbitmq_service.publish_document_processing(
        document_id=document_id,
        filename=filename,
        filepath=os.path.abspath(filepath),
        uploaded_at=uploaded_at
    )

    if not published:
        # Broker indisponível: atualiza task para FAILED
        task_service.update_task(
            task_id=document_id,
            status=TaskStatus.FAILED,
            progress=0,
            message="Falha ao publicar na fila. Broker RabbitMQ indisponível.",
            error_detail="RabbitMQ não pôde ser alcançado no momento do upload.",
        )
        BaseLogger.error(f"[Upload] ✗ Falha ao publicar na fila RabbitMQ para doc_id={document_id}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao enfileirar o documento para processamento (RabbitMQ indisponível)."
        )

    BaseLogger.info(
        f"[Upload] ✓ Documento enfileirado com sucesso: doc_id={document_id} | "
        f"fila={settings.RABBITMQ_QUEUE}"
    )

    return DocumentUploadResponse(
        document_id=document_id,
        status="queued",
        message="Documento enviado para processamento"
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
        vector_store.delete_document_chunks(document_id)
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


@router.post(
    "/reprocess",
    response_model=DocumentReprocessResponse,
    status_code=status.HTTP_200_OK,
    summary="Reprocessar e reindexar todos os documentos existentes",
    description=(
        "Varre o diretório de uploads local para identificar todos os arquivos persistidos. "
        "Para cada documento, realiza novamente a extração de texto, limpeza, "
        "divisão em chunks com o novo CHUNK_SIZE e CHUNK_OVERLAP e a geração de embeddings, "
        "atualizando a base vetorial do ChromaDB."
    ),
)
async def reprocess_all_documents() -> DocumentReprocessResponse:
    BaseLogger.info("[Reprocess] Iniciando rotina de reprocessamento em lote...")
    
    if not os.path.exists(settings.UPLOAD_DIR):
        BaseLogger.warning(f"[Reprocess] Diretório de uploads não existe: {settings.UPLOAD_DIR}")
        return DocumentReprocessResponse(
            message="Diretório de uploads vazio ou inexistente.",
            total_processed=0,
            details=[]
        )
        
    # Identificar documentos a partir do padrão: UPLOAD_DIR/<document_id>_<filename>
    files = os.listdir(settings.UPLOAD_DIR)
    documents_to_process = []
    
    for filename in files:
        # Padrão: UUID (36 chars) + '_' + filename
        if len(filename) > 37 and filename[36] == '_':
            doc_id = filename[:36]
            # Verificar se é UUID válido
            try:
                uuid.UUID(doc_id)
                original_filename = filename[37:]
                filepath = os.path.join(settings.UPLOAD_DIR, filename)
                documents_to_process.append({
                    "document_id": doc_id,
                    "filename": original_filename,
                    "filepath": filepath
                })
            except ValueError:
                continue

    if not documents_to_process:
        BaseLogger.info("[Reprocess] Nenhum documento elegível encontrado para reprocessamento.")
        return DocumentReprocessResponse(
            message="Nenhum documento encontrado para reprocessar.",
            total_processed=0,
            details=[]
        )

    processed_count = 0
    details = []
    
    for doc in documents_to_process:
        doc_id = doc["document_id"]
        orig_name = doc["filename"]
        path = doc["filepath"]
        
        BaseLogger.info(f"[Reprocess] Reprocessando doc_id={doc_id} | arquivo={orig_name}...")
        try:
            file_ext = os.path.splitext(orig_name)[1].lower()
            raw_text = ""
            page_count = None

            if file_ext == ".pdf":
                raw_text, page_count = document_processor.extract_text_from_pdf(path)
            elif file_ext == ".md":
                raw_text = document_processor.extract_text_from_markdown(path)
            else:
                raise ValueError(f"Extensão não suportada: {file_ext}")

            cleaned_text = document_processor.clean_text(raw_text)
            file_size = os.path.getsize(path)

            metadata = DocumentMetadata(
                filename=orig_name,
                file_size_bytes=file_size,
                content_type="application/pdf" if file_ext == ".pdf" else "text/markdown",
                page_count=page_count,
                char_count=len(cleaned_text),
                uploaded_at=datetime.utcnow()
            )

            # Garantir que o splitter use os valores atualizados
            from langchain_text_splitters import RecursiveCharacterTextSplitter
            semantic_processor.text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=settings.CHUNK_SIZE,
                chunk_overlap=settings.CHUNK_OVERLAP,
                length_function=len,
                separators=["\n\n", "\n", " ", ""]
            )

            response = semantic_processor.process_text_into_chunks(
                document_id=doc_id,
                text=cleaned_text,
                doc_metadata=metadata
            )

            if response.chunks:
                vector_store.delete_document_chunks(doc_id)
                persisted = vector_store.upsert_chunks(response.chunks)
                if persisted:
                    processed_count += 1
                    details.append({
                        "document_id": doc_id,
                        "filename": orig_name,
                        "chunks_count": response.total_chunks,
                        "status": "success"
                    })
                    BaseLogger.info(f"[Reprocess] Sucesso para doc_id={doc_id}. {response.total_chunks} chunks indexados.")
                else:
                    raise RuntimeError("Falha ao persistir no ChromaDB.")
            else:
                raise ValueError("Nenhum chunk gerado para o arquivo.")

        except Exception as exc:
            BaseLogger.error(f"[Reprocess] Falha ao reprocessar {orig_name}: {str(exc)}")
            details.append({
                "document_id": doc_id,
                "filename": orig_name,
                "status": "failed",
                "error": str(exc)
            })

    # Limpar todo o cache do Redis para invalidar respostas RAG antigas
    try:
        await cache_service.clear()
        BaseLogger.info("[Reprocess] Cache do Redis limpo com sucesso para invalidar resultados RAG antigos.")
    except Exception as e:
        BaseLogger.error(f"[Reprocess] Falha ao limpar cache Redis: {str(e)}")

    return DocumentReprocessResponse(
        message=f"Reprocessamento concluído. {processed_count} documentos processados com sucesso.",
        total_processed=processed_count,
        details=details
    )


@router.delete(
    "/{document_id}",
    response_model=DocumentDeleteResponse,
    status_code=status.HTTP_200_OK,
    summary="Excluir documento do sistema",
    description=(
        "Remove permanentemente um documento do sistema, incluindo: "
        "todos os chunks vetoriais no ChromaDB, o arquivo físico em disco e a invalidação do cache Redis."
    ),
)
async def delete_document(
    document_id: str,
) -> DocumentDeleteResponse:
    """
    DELETE /api/v1/document/{document_id}

    Fluxo de remoção:
    1. Localiza o arquivo físico em data/uploads/ pelo prefixo document_id.
    2. Remove todos os chunks vetoriais do ChromaDB (retorna contagem).
    3. Remove o arquivo físico em disco.
    4. Invalida o cache Redis (flush completo).
    5. Retorna estatísticas da remoção.

    Status HTTP:
    - 200: Remoção concluída (com ou sem chunks/arquivo existentes no momento).
    - 404: Nenhum registro encontrado para o document_id informado.
    - 500: Falha interna durante a remoção.
    """
    BaseLogger.info(f"[DELETE] Recebida solicitação de exclusão para document_id={document_id}")

    # ── 1. Localizar arquivo físico pelo prefixo document_id ─────────────────
    physical_file_path: str | None = None
    if os.path.isdir(settings.UPLOAD_DIR):
        for filename in os.listdir(settings.UPLOAD_DIR):
            if filename.startswith(f"{document_id}_"):
                physical_file_path = os.path.join(settings.UPLOAD_DIR, filename)
                break

    # ── 2. Contar chunks existentes antes de qualquer remoção ─────────────────
    try:
        existing_check = vector_store.collection.get(
            where={"source_doc_id": document_id},
            include=["documents"]
        )
        has_chunks = len(existing_check.get("ids", [])) > 0
    except Exception as chk_exc:
        BaseLogger.error(f"[DELETE] Falha ao verificar chunks no ChromaDB: {str(chk_exc)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao verificar o documento no banco vetorial: {str(chk_exc)}",
        )

    # ── 3. 404 se não há nenhum rastro do documento ───────────────────────────
    if not has_chunks and physical_file_path is None:
        BaseLogger.warning(f"[DELETE] document_id={document_id} não encontrado (sem chunks e sem arquivo físico).")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Documento '{document_id}' não encontrado no sistema.",
        )

    # ── 4. Remover chunks do ChromaDB ─────────────────────────────────────────
    try:
        chunks_removed = vector_store.delete_document(document_id)
        BaseLogger.info(f"[DELETE] {chunks_removed} chunk(s) removido(s) do ChromaDB para document_id={document_id}")
    except Exception as vs_exc:
        BaseLogger.error(f"[DELETE] Falha ao remover chunks do ChromaDB: {str(vs_exc)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao remover chunks do banco vetorial: {str(vs_exc)}",
        )

    # ── 5. Remover arquivo físico ─────────────────────────────────────────────
    file_removed = False
    if physical_file_path and os.path.exists(physical_file_path):
        try:
            os.remove(physical_file_path)
            file_removed = True
            BaseLogger.info(f"[DELETE] Arquivo físico removido: {physical_file_path}")
        except OSError as rm_exc:
            BaseLogger.error(f"[DELETE] Falha ao remover arquivo físico '{physical_file_path}': {str(rm_exc)}")
            # Não abortamos — o ChromaDB já foi limpo; apenas reportamos no response.

    # ── 6. Invalidar cache Redis ──────────────────────────────────────────────
    try:
        await cache_service.clear()
        BaseLogger.info("[DELETE] Cache Redis invalidado com sucesso.")
    except Exception as cache_exc:
        BaseLogger.warning(f"[DELETE] Falha ao invalidar cache Redis (não-crítico): {str(cache_exc)}")

    BaseLogger.info(
        f"[DELETE] Exclusão concluída: document_id={document_id} | "
        f"chunks_removed={chunks_removed} | file_removed={file_removed}"
    )

    return DocumentDeleteResponse(
        success=True,
        document_id=document_id,
        chunks_removed=chunks_removed,
        file_removed=file_removed,
        message="Documento removido com sucesso.",
    )
