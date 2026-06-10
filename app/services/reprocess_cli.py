"""
Script CLI para reprocessar e reindexar todos os documentos existentes.
Execução:
    python -m app.services.reprocess_cli
"""
import asyncio
import os
import sys
import uuid
from datetime import datetime

# Garantir que o módulo raiz do projeto esteja no PATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from app.core.config import settings
from app.core.logging import BaseLogger, setup_logging
from app.schemas.document import DocumentMetadata
from app.services.document_processor import document_processor
from app.services.semantic_processor import semantic_processor
from app.services.vector_store import vector_store
from app.services.cache_service import cache_service


async def main():
    setup_logging()
    BaseLogger.info("=" * 60)
    BaseLogger.info("INICIANDO ROTINA CLI DE REPROCESSAMENTO E REINDEXAÇÃO RAG")
    BaseLogger.info(f"Chunk Size: {settings.CHUNK_SIZE} | Chunk Overlap: {settings.CHUNK_OVERLAP}")
    BaseLogger.info("=" * 60)

    if not os.path.exists(settings.UPLOAD_DIR):
        BaseLogger.error(f"Diretório de uploads não encontrado: {settings.UPLOAD_DIR}")
        return

    files = os.listdir(settings.UPLOAD_DIR)
    documents_to_process = []

    for filename in files:
        if len(filename) > 37 and filename[36] == '_':
            doc_id = filename[:36]
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
        BaseLogger.info("Nenhum documento encontrado para reprocessamento.")
        return

    BaseLogger.info(f"Encontrados {len(documents_to_process)} documentos para reprocessar.")

    processed_count = 0

    for doc in documents_to_process:
        doc_id = doc["document_id"]
        orig_name = doc["filename"]
        path = doc["filepath"]

        BaseLogger.info(f"-> Processando doc_id={doc_id} | arquivo={orig_name}")
        try:
            file_ext = os.path.splitext(orig_name)[1].lower()
            raw_text = ""
            page_count = None

            if file_ext == ".pdf":
                raw_text, page_count = document_processor.extract_text_from_pdf(path)
            elif file_ext == ".md":
                raw_text = document_processor.extract_text_from_markdown(path)
            else:
                BaseLogger.warning(f"Extensão não suportada ignorada: {file_ext}")
                continue

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

            # Forçar atualização do text_splitter com os novos parâmetros
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
                    BaseLogger.info(f"   ✓ Sucesso: {response.total_chunks} chunks indexados.")
                else:
                    BaseLogger.error("   ✗ Falha ao persistir no ChromaDB.")
            else:
                BaseLogger.warning("   ⚠ Nenhum chunk gerado para este arquivo.")

        except Exception as exc:
            BaseLogger.error(f"   ✗ Falha geral: {str(exc)}")

    # Limpar cache do Redis
    try:
        await cache_service.clear()
        BaseLogger.info("✓ Cache do Redis limpo com sucesso para invalidar resultados RAG antigos.")
    except Exception as e:
        BaseLogger.error(f"✗ Falha ao limpar cache Redis: {str(e)}")

    BaseLogger.info("=" * 60)
    BaseLogger.info(f"FIM: {processed_count}/{len(documents_to_process)} documentos reindexados com sucesso.")
    BaseLogger.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
