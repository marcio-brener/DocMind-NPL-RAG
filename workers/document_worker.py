"""
Worker de Processamento de Documentos Assíncrono — DocMind
=========================================================
Processo standalone que consome a fila RabbitMQ 'document_processing_queue'
e executa o pipeline completo de ingestão assíncrona usando aio-pika e asyncio.

Execução:
    python -m workers.document_worker
"""

import asyncio
import json
import os
import sys
import signal
from datetime import datetime
from typing import Optional

# Garante que o módulo raiz do projeto esteja no PATH para imports relativos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.core.config import settings
from app.core.logging import BaseLogger, setup_logging
from app.schemas.document import DocumentMetadata
from app.schemas.task import TaskCreate, TaskStatus
from app.services.document_processor import document_processor
from app.services.rabbitmq_service import rabbitmq_service, process_rag_request
from app.services.semantic_processor import semantic_processor
from app.services.task_service import task_service
from app.services.vector_store import vector_store


class DocumentWorker:
    """
    Worker consumer assíncrono responsável por escutar e processar mensagens
    da fila RabbitMQ de ingestão de documentos.
    """

    def __init__(self) -> None:
        self._running = False
        self._loop = asyncio.get_event_loop()

    def _setup_signal_handlers(self) -> None:
        """Registra handlers de sinal para encerramento gracioso."""
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                self._loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            except NotImplementedError:
                # add_signal_handler não é suportado no Windows em loop padrão (SelectorEventLoop)
                pass

    async def shutdown(self) -> None:
        """Encerra graciosamente o consumo da fila e as conexões."""
        BaseLogger.info("[WORKER] Sinal de encerramento recebido. Fechando conexões...")
        self._running = False
        await rabbitmq_service.close()
        BaseLogger.info("[WORKER] Conexões encerradas graciosamente.")
        # Encerrar loop
        asyncio.get_event_loop().stop()

    async def _execute_pipeline(self, document_id: str, task_id: str, filename: str, filepath: str) -> None:
        """
        Executa o pipeline completo de ingestão do documento em thread pools do asyncio
        para evitar o bloqueio do event loop.
        """
        # ── Etapa 1: PROCESSING — Localizar/Abrir arquivo ───────────────────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.PROCESSING,
            progress=10,
            message="Abrindo e localizando o arquivo no sistema...",
        )

        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Arquivo não encontrado no caminho fornecido: '{filepath}'")

        file_ext = os.path.splitext(filename)[1].lower()
        file_size = os.path.getsize(filepath)

        BaseLogger.info(f"[WORKER] Arquivo localizado com sucesso: {filepath} | Tamanho: {file_size} bytes")

        # ── Etapa 2: Extrair texto ────────────────────────────────────────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.PROCESSING,
            progress=30,
            message="Extraindo e limpando conteúdo textual do documento...",
        )

        def extract_text():
            if file_ext == ".pdf":
                return document_processor.extract_text_from_pdf(filepath)
            elif file_ext == ".md":
                return document_processor.extract_text_from_markdown(filepath), None
            else:
                raise ValueError(f"Extensão não suportada pelo worker: '{file_ext}'")

        raw_text, page_count = await asyncio.to_thread(extract_text)
        cleaned_text = await asyncio.to_thread(document_processor.clean_text, raw_text)

        if not cleaned_text.strip():
            raise ValueError("A extração de texto resultou em conteúdo em branco ou ilegível.")

        BaseLogger.info(f"[WORKER] Extração de texto concluída. Total de caracteres: {len(cleaned_text)}")

        # ── Etapa 3: Preparar metadados ─────────────────────────────────────────
        content_type = "application/pdf" if file_ext == ".pdf" else "text/markdown"
        doc_metadata = DocumentMetadata(
            filename=filename,
            file_size_bytes=file_size,
            content_type=content_type,
            page_count=page_count,
            char_count=len(cleaned_text),
            uploaded_at=datetime.utcnow(),
        )

        # ── Etapa 4: Chunking semântico ───────────────────────────────────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.PROCESSING,
            progress=60,
            message="Executando segmentação semântica (chunking)...",
        )

        semantic_response = await asyncio.to_thread(
            semantic_processor.process_text_into_chunks,
            document_id=document_id,
            text=cleaned_text,
            doc_metadata=doc_metadata,
        )

        if not semantic_response.chunks:
            raise ValueError("O processamento semântico resultou em zero chunks.")

        BaseLogger.info(f"[WORKER] Segmentação semântica concluída. Total de chunks gerados: {len(semantic_response.chunks)}")

        # ── Etapa 5: Persistência no ChromaDB (gera embeddings e salva) ─────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.PROCESSING,
            progress=80,
            message=f"Persistindo {semantic_response.total_chunks} chunks no banco vetorial ChromaDB...",
        )

        await asyncio.to_thread(vector_store.delete_document_chunks, document_id)
        persisted = await asyncio.to_thread(vector_store.upsert_chunks, semantic_response.chunks)
        if not persisted:
            raise RuntimeError("Falha ao persistir chunks indexados no ChromaDB.")

        # ── Etapa 6: COMPLETED ────────────────────────────────────────────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            message=(
                f"Processamento concluído com sucesso. "
                f"{semantic_response.total_chunks} chunks indexados no ChromaDB."
            ),
        )

        BaseLogger.info(
            f"[WORKER] Documento processado com sucesso: task_id={task_id} | "
            f"doc_id={document_id} | chunks={semantic_response.total_chunks}"
        )

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """
        Callback que consome mensagens da fila e realiza processamento assíncrono.
        Implementa controle de timeouts, tratamento de erros e fila DLQ.
        """
        body_str = message.body.decode("utf-8")
        BaseLogger.info(f"[CONSUMER] Documento recebido da fila: {body_str}")

        # 1. Parsing da mensagem
        try:
            payload = json.loads(body_str)
        except Exception as exc:
            BaseLogger.error(f"[WORKER] Falha no processamento (Mensagem inválida no JSON): {body_str}. Enviando para DLQ.")
            # Rejeitar sem requeue envia diretamente para a DLQ declarada
            await message.reject(requeue=False)
            return

        document_id = payload.get("document_id")
        filename = payload.get("filename")
        filepath = payload.get("filepath")

        # Validação simples de chaves obrigatórias
        if not document_id or not filename or not filepath:
            BaseLogger.error(f"[WORKER] Falha no processamento (Campos obrigatórios ausentes): {payload}. Enviando para DLQ.")
            await message.reject(requeue=False)
            return

        task_id = document_id  # Mantemos o task_id acoplado diretamente ao document_id

        # 2. Inicializar tarefa se não existir localmente no JSON
        if not task_service.get_task(task_id):
            task_service.create_task(
                TaskCreate(
                    task_id=task_id,
                    document_id=document_id,
                    filename=filename
                )
            )

        headers = dict(message.headers or {})
        retry_count = headers.get("x-retry-count", 0)
        max_retries = 3

        try:
            # Limita a execução do processamento do documento a um timeout de 300 segundos
            await asyncio.wait_for(
                self._execute_pipeline(
                    document_id=document_id,
                    task_id=task_id,
                    filename=filename,
                    filepath=filepath
                ),
                timeout=300.0
            )
            # Confirma o recebimento e processamento
            await message.ack()

        except asyncio.TimeoutError:
            error_msg = "Timeout de 300 segundos atingido durante o processamento do documento."
            BaseLogger.error(f"[WORKER] Falha no processamento por timeout para o documento {filename}.")
            await self._handle_failure(message, payload, retry_count, max_retries, error_msg)

        except Exception as exc:
            error_msg = str(exc)
            BaseLogger.error(f"[WORKER] Falha no processamento do documento {filename}: {error_msg}")
            await self._handle_failure(message, payload, retry_count, max_retries, error_msg)

    async def _handle_failure(
        self,
        message: AbstractIncomingMessage,
        payload: dict,
        retry_count: int,
        max_retries: int,
        error_msg: str
    ) -> None:
        """
        Trata falhas no processamento publicando novamente para retentativas ou direcionando para DLQ.
        """
        document_id = payload["document_id"]
        filename = payload["filename"]
        task_id = document_id

        if retry_count < max_retries:
            next_retry = retry_count + 1
            BaseLogger.warning(
                f"[RABBITMQ] Tentativa de reconexão/reprocessamento: Redirecionando para retry {next_retry}/{max_retries} para o documento {filename}."
            )
            
            task_service.update_task(
                task_id=task_id,
                status=TaskStatus.PROCESSING,
                progress=10,
                message=f"Falha no processamento. Retentando ({next_retry}/{max_retries})...",
                error_detail=error_msg
            )

            # Publicar cópia da mensagem com cabeçalho de retentativa incrementado
            headers = dict(message.headers or {})
            headers["x-retry-count"] = next_retry

            retry_message = aio_pika.Message(
                body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
                headers=headers
            )

            # Re-publicar diretamente na fila principal usando default_exchange
            await message.channel.default_exchange.publish(
                retry_message,
                routing_key=settings.RABBITMQ_QUEUE
            )
            
            # Realiza ack da mensagem antiga para que ela saia da fila e dê lugar à nova
            await message.ack()

        else:
            BaseLogger.error(
                f"[WORKER] Falha no processamento: limite de retentativas ({max_retries}) atingido para o documento {filename}."
            )
            
            task_service.update_task(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=0,
                message="Falha permanente no processamento. Documento enviado para DLQ.",
                error_detail=error_msg
            )
            
            # Rejeitar sem requeue envia para a Dead Letter Queue correspondente
            await message.reject(requeue=False)

    async def start(self) -> None:
        """Inicia o consumo contínuo de mensagens das filas principal e RAG."""
        setup_logging()
        self._setup_signal_handlers()
        self._running = True

        BaseLogger.info("=" * 60)
        BaseLogger.info("[Worker] DocMind Asynchronous Document Worker Iniciado.")
        BaseLogger.info(f"[Worker] Fila Principal: '{settings.RABBITMQ_QUEUE}'")
        BaseLogger.info(f"[Worker] Fila RAG: '{settings.RABBITMQ_RAG_QUEUE}'")
        BaseLogger.info(f"[Worker] Fila DLQ: 'document_dlq'")
        BaseLogger.info("=" * 60)

        # Laço para lidar com a inicialização / conexão ao RabbitMQ
        while self._running:
            try:
                connected = await rabbitmq_service.connect()
                if not connected:
                    BaseLogger.warning("[Worker] RabbitMQ indisponível. Retentando conexão em 5 segundos...")
                    await asyncio.sleep(5)
                    continue

                # Registra consumidores para ambas as filas
                await rabbitmq_service.consume_messages(
                    queue_name=settings.RABBITMQ_QUEUE,
                    callback=self._on_message
                )
                BaseLogger.info(f"[WORKER] Consumidor ativado para fila: {settings.RABBITMQ_QUEUE}")

                await rabbitmq_service.consume_messages(
                    queue_name=settings.RABBITMQ_RAG_QUEUE,
                    callback=process_rag_request
                )
                BaseLogger.info(f"[WORKER] Consumidor ativado para fila: {settings.RABBITMQ_RAG_QUEUE}")

                # Mantém o worker rodando indefinidamente
                while self._running and rabbitmq_service.is_connected:
                    await asyncio.sleep(1)

            except (aio_pika.exceptions.AMQPConnectionError, aio_pika.exceptions.ChannelClosed) as exc:
                BaseLogger.error(f"[WORKER] Erro de conexão com o RabbitMQ: {str(exc)}. Tentando reconectar em 5 segundos...")
                await rabbitmq_service.close()
                await asyncio.sleep(5)

            except Exception as exc:
                BaseLogger.error(f"[WORKER] Erro inesperado no loop principal: {str(exc)}. Tentando reconectar em 5 segundos...")
                await rabbitmq_service.close()
                await asyncio.sleep(5)


if __name__ == "__main__":
    worker = DocumentWorker()
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        BaseLogger.info("[Worker] Encerrando pelo usuário.")
