"""
Worker de Processamento de Documentos — DocMind
================================================
Processo standalone que consome a fila RabbitMQ 'document_processing'
e executa o pipeline completo de ingestão assíncrona:

    1. Recebe document_id + task_id da fila
    2. Localiza o arquivo em UPLOAD_DIR
    3. Extrai e limpa o texto
    4. Executa segmentação semântica (chunking)
    5. Gera embeddings vetoriais
    6. Persiste no ChromaDB
    7. Atualiza status da tarefa (QUEUED → PROCESSING → COMPLETED | FAILED)

Execução:
    python -m workers.document_worker

O worker opera em loop contínuo com prefetch_count=1 (fair dispatch),
garantindo que cada mensagem seja processada antes de receber outra.
"""

import json
import os
import sys
import signal
import time
from datetime import datetime
from typing import Optional

import pika
import pika.exceptions

# Garante que o módulo raiz do projeto esteja no PATH para imports relativos
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.core.config import settings
from app.core.logging import BaseLogger, setup_logging
from app.schemas.document import DocumentMetadata
from app.schemas.task import TaskStatus
from app.services.document_processor import document_processor
from app.services.embedding_service import embedding_service
from app.services.message_queue_service import RabbitMQService
from app.services.semantic_processor import semantic_processor
from app.services.task_service import task_service
from app.services.vector_store import vector_store


class DocumentWorker:
    """
    Worker consumer responsável por processar mensagens da fila document_processing.

    Cada mensagem corresponde a um documento previamente salvo no UPLOAD_DIR,
    aguardando processamento semântico completo (chunking + embeddings + ChromaDB).
    """

    def __init__(self) -> None:
        self._running = False
        self._rabbitmq = RabbitMQService()
        self._setup_signal_handlers()

    # ── Configuração ──────────────────────────────────────────────────────────

    def _setup_signal_handlers(self) -> None:
        """Registra handlers para SIGINT e SIGTERM para shutdown gracioso."""
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame) -> None:
        """Callback de shutdown gracioso ao receber sinal de encerramento."""
        BaseLogger.info(
            f"[Worker] Sinal de encerramento recebido (signal={signum}). "
            "Aguardando mensagem atual e encerrando..."
        )
        self._running = False
        self._rabbitmq.close()
        sys.exit(0)

    # ── Pipeline de Processamento ─────────────────────────────────────────────

    def _locate_file(self, document_id: str) -> Optional[str]:
        """
        Localiza o arquivo no UPLOAD_DIR pelo prefixo document_id.

        Args:
            document_id: UUID do documento salvo no upload.

        Returns:
            Caminho absoluto do arquivo ou None se não encontrado.
        """
        if not os.path.exists(settings.UPLOAD_DIR):
            BaseLogger.error(
                f"[Worker] Diretório de uploads não encontrado: {settings.UPLOAD_DIR}"
            )
            return None

        for filename in os.listdir(settings.UPLOAD_DIR):
            if filename.startswith(f"{document_id}_"):
                return os.path.join(settings.UPLOAD_DIR, filename)

        BaseLogger.warning(
            f"[Worker] Arquivo não encontrado para document_id={document_id} "
            f"em {settings.UPLOAD_DIR}"
        )
        return None

    def _process_document(self, document_id: str, task_id: str, filename: str) -> None:
        """
        Executa o pipeline completo de processamento semântico para um documento.

        Atualiza o TaskService em cada etapa do progresso:
            0%   → QUEUED     (estado inicial ao receber da fila)
            10%  → PROCESSING (arquivo localizado)
            30%  → PROCESSING (texto extraído e limpo)
            60%  → PROCESSING (chunking concluído)
            80%  → PROCESSING (embeddings gerados)
            100% → COMPLETED  (persistido no ChromaDB)

        Args:
            document_id: UUID do documento.
            task_id: UUID da tarefa para atualização de status.
            filename: Nome original do arquivo para extração de metadados.
        """
        BaseLogger.info(
            f"[Worker] ▶ Iniciando processamento: task_id={task_id} | "
            f"doc_id={document_id} | arquivo={filename}"
        )

        # ── Etapa 1: PROCESSING — Localizar arquivo ───────────────────────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.PROCESSING,
            progress=10,
            message="Localizando arquivo no sistema de armazenamento...",
        )

        file_path = self._locate_file(document_id)
        if not file_path:
            task_service.update_task(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=0,
                message="Arquivo não encontrado no sistema de armazenamento.",
                error_detail=f"Nenhum arquivo com prefixo '{document_id}' em {settings.UPLOAD_DIR}",
            )
            BaseLogger.error(
                f"[Worker] ✗ Falha: arquivo não localizado para doc_id={document_id}"
            )
            return

        original_filename = os.path.basename(file_path)[len(document_id) + 1:]
        file_ext = os.path.splitext(original_filename)[1].lower()
        file_size = os.path.getsize(file_path)

        BaseLogger.info(
            f"[Worker] Arquivo localizado: {file_path} | extensão={file_ext}"
        )

        # ── Etapa 2: Extrair texto ────────────────────────────────────────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.PROCESSING,
            progress=30,
            message="Extraindo e limpando conteúdo textual do documento...",
        )

        raw_text = ""
        page_count: Optional[int] = None

        try:
            if file_ext == ".pdf":
                raw_text, page_count = document_processor.extract_text_from_pdf(file_path)
            elif file_ext == ".md":
                raw_text = document_processor.extract_text_from_markdown(file_path)
            else:
                raise ValueError(f"Extensão não suportada pelo worker: '{file_ext}'")

            cleaned_text = document_processor.clean_text(raw_text)

            if not cleaned_text.strip():
                raise ValueError("Nenhum texto extraído do documento.")

            BaseLogger.info(
                f"[Worker] Texto extraído: {len(cleaned_text)} caracteres | "
                f"páginas={page_count}"
            )

        except Exception as exc:
            task_service.update_task(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=30,
                message="Falha na extração de texto do documento.",
                error_detail=str(exc),
            )
            BaseLogger.error(
                f"[Worker] ✗ Erro na extração de texto: {str(exc)}"
            )
            return

        # ── Etapa 3: Reconstruir metadados ────────────────────────────────────
        content_type = "application/pdf" if file_ext == ".pdf" else "text/markdown"
        doc_metadata = DocumentMetadata(
            filename=original_filename,
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

        try:
            semantic_response = semantic_processor.process_text_into_chunks(
                document_id=document_id,
                text=cleaned_text,
                doc_metadata=doc_metadata,
            )

            if not semantic_response.chunks:
                raise ValueError("O processamento semântico resultou em zero chunks.")

            BaseLogger.info(
                f"[Worker] Chunking concluído: {semantic_response.total_chunks} chunks gerados."
            )

        except Exception as exc:
            task_service.update_task(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=60,
                message="Falha no processamento semântico (chunking).",
                error_detail=str(exc),
            )
            BaseLogger.error(
                f"[Worker] ✗ Erro no chunking: {str(exc)}"
            )
            return

        # ── Etapa 5: Persistência no ChromaDB ─────────────────────────────────
        task_service.update_task(
            task_id=task_id,
            status=TaskStatus.PROCESSING,
            progress=80,
            message=f"Persistindo {semantic_response.total_chunks} chunks no ChromaDB...",
        )

        try:
            persisted = vector_store.upsert_chunks(semantic_response.chunks)

            if not persisted:
                raise RuntimeError("Falha ao persistir chunks no ChromaDB.")

            BaseLogger.info(
                f"[Worker] {semantic_response.total_chunks} chunks persistidos no ChromaDB."
            )

        except Exception as exc:
            task_service.update_task(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=80,
                message="Falha ao persistir vetores no ChromaDB.",
                error_detail=str(exc),
            )
            BaseLogger.error(
                f"[Worker] ✗ Erro na persistência ChromaDB: {str(exc)}"
            )
            return

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
            f"[Worker] ✓ Processamento finalizado: task_id={task_id} | "
            f"doc_id={document_id} | chunks={semantic_response.total_chunks}"
        )

    # ── Callback do Consumer ──────────────────────────────────────────────────

    def _on_message(
        self,
        channel,
        method,
        properties,
        body: bytes,
    ) -> None:
        """
        Callback invocado pelo pika a cada mensagem recebida da fila.

        Realiza ack manual após processamento bem-sucedido ou falha registrada.
        Garante que mensagens com payload inválido não fiquem presas na fila.
        """
        try:
            payload = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            BaseLogger.error(
                f"[Worker] Mensagem com payload inválido descartada: {str(exc)}"
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        document_id = payload.get("document_id")
        task_id = payload.get("task_id")
        filename = payload.get("filename", "desconhecido")

        BaseLogger.info(
            f"[Worker] ← Mensagem consumida: task_id={task_id} | "
            f"doc_id={document_id} | arquivo={filename}"
        )

        if not document_id or not task_id:
            BaseLogger.error(
                f"[Worker] Mensagem incompleta descartada: {payload}"
            )
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            self._process_document(
                document_id=document_id,
                task_id=task_id,
                filename=filename,
            )
        except Exception as exc:
            BaseLogger.error(
                f"[Worker] ✗ Erro não tratado no processamento "
                f"task_id={task_id}: {str(exc)}"
            )
            task_service.update_task(
                task_id=task_id,
                status=TaskStatus.FAILED,
                progress=0,
                message="Erro interno inesperado no worker.",
                error_detail=str(exc),
            )
        finally:
            # ACK sempre — evita reprocessamento infinito de mensagens com erros
            channel.basic_ack(delivery_tag=method.delivery_tag)

    # ── Loop Principal ────────────────────────────────────────────────────────

    def start(self) -> None:
        """
        Inicia o loop de consumo da fila document_processing.

        Tenta reconectar automaticamente em caso de falha de conexão,
        aguardando 5 segundos entre as tentativas.
        """
        setup_logging()
        self._running = True

        BaseLogger.info(
            "=" * 60
        )
        BaseLogger.info(
            f"[Worker] DocMind Document Worker iniciado."
        )
        BaseLogger.info(
            f"[Worker] Aguardando mensagens na fila: "
            f"'{settings.RABBITMQ_DOCUMENT_QUEUE}'"
        )
        BaseLogger.info(
            "[Worker] Pressione CTRL+C para encerrar graciosamente."
        )
        BaseLogger.info(
            "=" * 60
        )

        while self._running:
            try:
                connected = self._rabbitmq.connect()
                if not connected:
                    BaseLogger.warning(
                        "[Worker] Broker indisponível. Retentando em 5 segundos..."
                    )
                    time.sleep(5)
                    continue

                channel = self._rabbitmq._channel

                # Fair dispatch: processa 1 mensagem por vez
                channel.basic_qos(prefetch_count=1)

                channel.basic_consume(
                    queue=settings.RABBITMQ_DOCUMENT_QUEUE,
                    on_message_callback=self._on_message,
                    auto_ack=False,
                )

                BaseLogger.info(
                    f"[Worker] Consumer registrado na fila "
                    f"'{settings.RABBITMQ_DOCUMENT_QUEUE}'. Aguardando..."
                )
                channel.start_consuming()

            except pika.exceptions.AMQPConnectionError as exc:
                BaseLogger.error(
                    f"[Worker] Conexão perdida: {str(exc)}. "
                    "Reconectando em 5 segundos..."
                )
                self._rabbitmq.close()
                time.sleep(5)

            except pika.exceptions.ChannelClosedByBroker as exc:
                BaseLogger.error(
                    f"[Worker] Canal fechado pelo broker: {str(exc)}. "
                    "Reconectando em 5 segundos..."
                )
                self._rabbitmq.close()
                time.sleep(5)

            except KeyboardInterrupt:
                BaseLogger.info("[Worker] Interrupção recebida. Encerrando...")
                break

            except Exception as exc:
                BaseLogger.error(
                    f"[Worker] Erro inesperado no loop principal: {str(exc)}. "
                    "Reconectando em 5 segundos..."
                )
                self._rabbitmq.close()
                time.sleep(5)

        self._rabbitmq.close()
        BaseLogger.info("[Worker] Worker encerrado.")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    worker = DocumentWorker()
    worker.start()
