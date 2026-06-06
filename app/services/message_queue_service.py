import json
from typing import Any, Dict, Optional

import pika
import pika.exceptions

from app.core.config import settings
from app.core.logging import BaseLogger


class RabbitMQService:
    """
    Serviço corporativo de mensageria baseado em RabbitMQ.

    Responsabilidades:
        - Estabelecer e gerenciar conexão com o broker via pika.
        - Declarar filas duráveis de forma idempotente.
        - Publicar mensagens nas filas de processamento de documentos e RAG.
        - Fechar conexões de forma segura ao encerrar.

    Filas gerenciadas:
        - document_processing: fila principal de ingestão assíncrona.
        - rag_requests: fila para requisições RAG enfileiradas (futura expansão).

    Todas as mensagens são publicadas com delivery_mode=2 (persistentes),
    garantindo durabilidade mesmo em caso de restart do broker.
    """

    def __init__(self) -> None:
        self._connection: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self._connected: bool = False

    # ── Conexão ───────────────────────────────────────────────────────────────

    def connect(self) -> bool:
        """
        Estabelece conexão com o broker RabbitMQ e declara as filas configuradas.

        Lê RABBITMQ_URL das configurações (ex: amqp://guest:guest@localhost:5672/).
        As filas são declaradas como durable=True para sobreviver a reinicializações.

        Returns:
            True se a conexão for bem-sucedida, False caso contrário.
        """
        try:
            BaseLogger.info(
                f"[RabbitMQ] Conectando ao broker: {settings.RABBITMQ_URL}"
            )
            params = pika.URLParameters(settings.RABBITMQ_URL)
            params.heartbeat = 600
            params.blocked_connection_timeout = 300

            self._connection = pika.BlockingConnection(params)
            self._channel = self._connection.channel()

            # Declarar fila de processamento de documentos
            self._channel.queue_declare(
                queue=settings.RABBITMQ_DOCUMENT_QUEUE,
                durable=True,
            )
            BaseLogger.info(
                f"[RabbitMQ] Fila declarada: '{settings.RABBITMQ_DOCUMENT_QUEUE}' "
                f"(durable=True)"
            )

            # Declarar fila de requisições RAG
            self._channel.queue_declare(
                queue=settings.RABBITMQ_RAG_QUEUE,
                durable=True,
            )
            BaseLogger.info(
                f"[RabbitMQ] Fila declarada: '{settings.RABBITMQ_RAG_QUEUE}' "
                f"(durable=True)"
            )

            self._connected = True
            BaseLogger.info("[RabbitMQ] Conexão estabelecida com sucesso.")
            return True

        except pika.exceptions.AMQPConnectionError as exc:
            BaseLogger.error(
                f"[RabbitMQ] Falha ao conectar ao broker: {str(exc)}"
            )
            self._connected = False
            return False
        except Exception as exc:
            BaseLogger.error(
                f"[RabbitMQ] Erro inesperado durante a conexão: {str(exc)}"
            )
            self._connected = False
            return False

    def _ensure_connected(self) -> bool:
        """
        Verifica se a conexão está ativa; tenta reconectar se necessário.

        Returns:
            True se conectado (ou reconexão bem-sucedida), False caso contrário.
        """
        if self._connected and self._connection and not self._connection.is_closed:
            return True

        BaseLogger.warning(
            "[RabbitMQ] Conexão inativa. Tentando reconexão automática..."
        )
        return self.connect()

    # ── Publicação ────────────────────────────────────────────────────────────

    def publish_document_processing(
        self,
        document_id: str,
        task_id: str,
        filename: str,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publica uma mensagem na fila de processamento de documentos.

        A mensagem contém todos os dados necessários para o worker localizar
        o arquivo, executar o pipeline de chunking/embeddings e atualizar a tarefa.

        Args:
            document_id: UUID do documento salvo em disco.
            task_id: UUID da tarefa para rastreamento de status.
            filename: Nome original do arquivo para localização no UPLOAD_DIR.
            extra_metadata: Dicionário opcional com metadados adicionais.

        Returns:
            True se publicado com sucesso, False em caso de falha.
        """
        if not self._ensure_connected():
            BaseLogger.error(
                "[RabbitMQ] Não foi possível publicar: broker indisponível."
            )
            return False

        payload: Dict[str, Any] = {
            "document_id": document_id,
            "task_id": task_id,
            "filename": filename,
            "metadata": extra_metadata or {},
        }

        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=settings.RABBITMQ_DOCUMENT_QUEUE,
                body=json.dumps(payload, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type="application/json",
                ),
            )
            BaseLogger.info(
                f"[RabbitMQ] Mensagem publicada na fila "
                f"'{settings.RABBITMQ_DOCUMENT_QUEUE}': "
                f"task_id={task_id} | doc_id={document_id} | arquivo={filename}"
            )
            return True

        except pika.exceptions.AMQPError as exc:
            BaseLogger.error(
                f"[RabbitMQ] Falha ao publicar mensagem de documento: {str(exc)}"
            )
            self._connected = False
            return False
        except Exception as exc:
            BaseLogger.error(
                f"[RabbitMQ] Erro inesperado ao publicar: {str(exc)}"
            )
            return False

    def publish_rag_request(
        self,
        question: str,
        request_id: str,
        limit: int = 4,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Publica uma requisição RAG na fila de processamento assíncrono.

        Args:
            question: Pergunta do usuário a ser respondida pelo pipeline RAG.
            request_id: UUID de rastreamento da requisição.
            limit: Número máximo de chunks de contexto a usar.
            extra_metadata: Metadados adicionais opcionais.

        Returns:
            True se publicado com sucesso, False em caso de falha.
        """
        if not self._ensure_connected():
            BaseLogger.error(
                "[RabbitMQ] Não foi possível publicar requisição RAG: broker indisponível."
            )
            return False

        payload: Dict[str, Any] = {
            "request_id": request_id,
            "question": question,
            "limit": limit,
            "metadata": extra_metadata or {},
        }

        try:
            self._channel.basic_publish(
                exchange="",
                routing_key=settings.RABBITMQ_RAG_QUEUE,
                body=json.dumps(payload, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type="application/json",
                ),
            )
            BaseLogger.info(
                f"[RabbitMQ] Requisição RAG publicada na fila "
                f"'{settings.RABBITMQ_RAG_QUEUE}': request_id={request_id}"
            )
            return True

        except pika.exceptions.AMQPError as exc:
            BaseLogger.error(
                f"[RabbitMQ] Falha ao publicar requisição RAG: {str(exc)}"
            )
            self._connected = False
            return False
        except Exception as exc:
            BaseLogger.error(
                f"[RabbitMQ] Erro inesperado ao publicar requisição RAG: {str(exc)}"
            )
            return False

    # ── Encerramento ──────────────────────────────────────────────────────────

    def close(self) -> None:
        """
        Fecha o canal e a conexão com o broker RabbitMQ de forma segura.
        Deve ser chamado no shutdown da aplicação ou do worker.
        """
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
                BaseLogger.debug("[RabbitMQ] Canal fechado com sucesso.")

            if self._connection and not self._connection.is_closed:
                self._connection.close()
                BaseLogger.info("[RabbitMQ] Conexão encerrada com sucesso.")

        except Exception as exc:
            BaseLogger.warning(
                f"[RabbitMQ] Aviso ao fechar conexão: {str(exc)}"
            )
        finally:
            self._connected = False
            self._channel = None
            self._connection = None

    @property
    def is_connected(self) -> bool:
        """Retorna True se a conexão com o broker está ativa."""
        return (
            self._connected
            and self._connection is not None
            and not self._connection.is_closed
        )


# Instanciação Singleton do serviço de mensageria
rabbitmq_service = RabbitMQService()
