import json
from typing import Any, Callable, Dict, Optional
import aio_pika
from aio_pika.abc import AbstractIncomingMessage

from app.core.config import settings
from app.core.logging import BaseLogger


class RabbitMQService:
    """
    Serviço assíncrono corporativo de mensageria baseado em RabbitMQ e aio-pika.

    Responsabilidades:
        - Conectar ao RabbitMQ de forma robusta e assíncrona.
        - Declarar exchanges e filas de forma idempotente (Topologia RAG).
        - Implementar reconexão automática com log corporativo de reconexão.
        - Publicar mensagens na fila de processamento.
        - Consumir mensagens da fila.
        - Declarar fila Dead Letter Queue (DLQ) para tratamento de erros.
    """

    def __init__(self) -> None:
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self._connected: bool = False

    async def connect(self) -> bool:
        """
        Estabelece a conexão assíncrona robusta com o broker RabbitMQ
        e configura a topologia das filas e exchanges (incluindo DLQ).
        """
        try:
            BaseLogger.info(f"[RABBITMQ] Conectando ao broker em: {settings.RABBITMQ_URL}")
            
            # connect_robust do aio-pika fornece reconexão automática por padrão
            self.connection = await aio_pika.connect_robust(
                settings.RABBITMQ_URL,
                timeout=10
            )
            
            # Registrar callback de reconexão para fins de log corporativo
            self.connection.reconnect_callbacks.add(self._on_reconnect)
            
            self.channel = await self.connection.channel()
            
            # Configurar QoS - Fair Dispatch (Prefetch = 1)
            await self.channel.set_qos(prefetch_count=1)

            # Declarar topologia de filas (Main e DLQ)
            await self._declare_topology()

            self._connected = True
            BaseLogger.info("[RABBITMQ] Conexão estabelecida com sucesso.")
            return True

        except Exception as exc:
            BaseLogger.error(f"[RABBITMQ] Falha ao conectar ao broker: {str(exc)}")
            self._connected = False
            return False

    def _on_reconnect(self, sender: Any) -> None:
        """
        Callback acionado quando o aio-pika restabelece a conexão perdida.
        """
        BaseLogger.warning("[RABBITMQ] Tentativa de reconexão restabelecida com sucesso com o broker.")

    async def _declare_topology(self) -> None:
        """
        Declara a estrutura de exchanges e filas, incluindo a Dead Letter Queue (DLQ).
        """
        if not self.channel:
            raise RuntimeError("O canal RabbitMQ não está inicializado.")

        # 1. Configurar Dead Letter Exchange (DLX) e Dead Letter Queue (DLQ)
        dlx_name = f"{settings.RABBITMQ_EXCHANGE}.dlx"
        dlq_name = "document_dlq"

        # Declarar DLX
        dlx = await self.channel.declare_exchange(
            dlx_name,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        # Declarar DLQ
        dlq = await self.channel.declare_queue(
            dlq_name,
            durable=True
        )
        # Vincular DLQ à DLX usando o nome da fila como routing key
        await dlq.bind(dlx, routing_key=dlq_name)
        BaseLogger.info(f"[QUEUE] Fila DLQ declarada: '{dlq_name}' vinculada à Exchange '{dlx_name}'")

        # 2. Configurar Exchange e Fila Principal
        exchange_name = settings.RABBITMQ_EXCHANGE
        queue_name = settings.RABBITMQ_QUEUE
        routing_key = settings.RABBITMQ_ROUTING_KEY

        # Declarar Exchange Principal
        main_exchange = await self.channel.declare_exchange(
            exchange_name,
            type=aio_pika.ExchangeType.DIRECT,
            durable=True
        )

        # Declarar Fila Principal configurando a DLX e DLQ routing key
        main_queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dlq_name
            }
        )

        # Vincular Fila Principal à Exchange Principal
        await main_queue.bind(main_exchange, routing_key=routing_key)
        BaseLogger.info(
            f"[QUEUE] Fila principal declarada: '{queue_name}' vinculada à Exchange '{exchange_name}' "
            f"com routing_key='{routing_key}' e DLX='{dlx_name}'"
        )

        # 3. Configurar Fila RAG Requests vinculando à DLX
        rag_queue_name = settings.RABBITMQ_RAG_QUEUE
        rag_queue = await self.channel.declare_queue(
            rag_queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dlq_name
            }
        )
        BaseLogger.info(f"[QUEUE] Fila RAG declarada: '{rag_queue_name}' com DLX='{dlx_name}' e routing_key='{dlq_name}' para DLQ")


    async def publish_document_processing(
        self,
        document_id: str,
        filename: str,
        filepath: str,
        uploaded_at: str
    ) -> bool:
        """
        Publica uma mensagem na exchange correspondente à fila de processamento.
        """
        if not self.is_connected or not self.channel:
            BaseLogger.error("[PRODUCER] Falha de conexão: broker indisponível para publicação.")
            return False

        payload = {
            "document_id": document_id,
            "filename": filename,
            "filepath": filepath,
            "uploaded_at": uploaded_at
        }

        try:
            exchange_name = settings.RABBITMQ_EXCHANGE
            routing_key = settings.RABBITMQ_ROUTING_KEY

            exchange = await self.channel.get_exchange(exchange_name, ensure=False)
            
            message = aio_pika.Message(
                body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json"
            )

            await exchange.publish(message, routing_key=routing_key)
            BaseLogger.info(
                f"[PRODUCER] Documento enviado para fila '{settings.RABBITMQ_QUEUE}': "
                f"doc_id={document_id} | arquivo={filename} | rota={routing_key}"
            )
            return True

        except Exception as exc:
            BaseLogger.error(f"[PRODUCER] Erro ao publicar mensagem para doc_id={document_id}: {str(exc)}")
            return False

    async def publish_rag_request(
        self,
        task_id: str,
        request_id: str,
        question: str,
        limit: int = 4,
        filter_document_id: Optional[str] = None
    ) -> bool:
        """
        Publica uma mensagem contendo a pergunta do usuário na fila 'rag_requests'.
        """
        if not self.is_connected or not self.channel:
            BaseLogger.error("[PRODUCER] Falha de conexão: broker indisponível para publicação de pergunta RAG.")
            return False

        from datetime import datetime
        payload = {
            "task_id": task_id,
            "request_id": request_id,
            "question": question,
            "limit": limit,
            "filter_document_id": filter_document_id,
            "timestamp": datetime.utcnow().isoformat()
        }

        try:
            BaseLogger.info(f"[PRODUCER] Publicando pergunta RAG | task_id={task_id} | filter_document_id={filter_document_id}")
            
            message = aio_pika.Message(
                body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json"
            )

            # Usar default_exchange com o nome da fila como routing_key para enviar diretamente à fila
            await self.channel.default_exchange.publish(
                message,
                routing_key=settings.RABBITMQ_RAG_QUEUE
            )
            BaseLogger.info(f"[RABBITMQ] Mensagem enviada para fila {settings.RABBITMQ_RAG_QUEUE}")
            return True

        except Exception as exc:
            BaseLogger.error(f"[PRODUCER] Erro ao publicar pergunta RAG para task_id={task_id}: {str(exc)}")
            return False


    async def consume_messages(self, queue_name: str, callback: Callable[[AbstractIncomingMessage], Any]) -> None:
        """
        Inicia o consumo contínuo de mensagens da fila fornecida, executando o callback assíncrono.
        """
        if not self.channel:
            raise RuntimeError("O canal do RabbitMQ não está disponível.")

        queue = await self.channel.get_queue(queue_name, ensure=False)
        await queue.consume(callback)
        BaseLogger.info(f"[CONSUMER] Escutando continuamente mensagens da fila: '{queue_name}'")

    async def close(self) -> None:
        """
        Fecha a conexão e os canais de forma graciosa.
        """
        try:
            if self.channel and not self.channel.is_closed:
                await self.channel.close()
                BaseLogger.debug("[RABBITMQ] Canal fechado com sucesso.")
            if self.connection and not self.connection.is_closed:
                await self.connection.close()
                BaseLogger.info("[RABBITMQ] Conexão encerrada com sucesso.")
        except Exception as exc:
            BaseLogger.warning(f"[RABBITMQ] Aviso ao fechar conexões: {str(exc)}")
        finally:
            self.channel = None
            self.connection = None
            self._connected = False

    @property
    def is_connected(self) -> bool:
        """Retorna True se a conexão está estabelecida e ativa."""
        return (
            self.connection is not None
            and not self.connection.is_closed
        )


# Instanciação Singleton
rabbitmq_service = RabbitMQService()


async def process_rag_request(message: AbstractIncomingMessage) -> None:
    """
    Callback assíncrono para processar mensagens da fila 'rag_requests'.
    """
    import json
    from app.core.logging import BaseLogger
    from app.schemas.rag import RAGRequest
    from app.schemas.task import TaskStatus
    from app.services.rag_service import rag_service
    from app.services.cache_service import cache_service
    from app.services.task_service import task_service

    body_str = message.body.decode("utf-8")
    BaseLogger.info("[RABBITMQ] Mensagem recebida")

    try:
        payload = json.loads(body_str)
        task_id = payload.get("task_id")
        request_id = payload.get("request_id")
        question = payload.get("question")
        limit = payload.get("limit", 4)
        filter_document_id = payload.get("filter_document_id")

        target_id = task_id or request_id

        if not target_id or not question:
            raise ValueError("Campos obrigatórios ausentes na mensagem ('task_id'/'request_id' ou 'question')")

        # Atualizar tarefa para PROCESSING (progresso 50%)
        task_service.update_task(
            task_id=target_id,
            status=TaskStatus.PROCESSING,
            progress=50,
            message="Executando pipeline RAG"
        )
        BaseLogger.info(f"[CONSUMER] Executando pipeline RAG | task_id={target_id} | filter_document_id={filter_document_id}")
        
        # Executar RAG pipeline
        rag_request = RAGRequest(question=question, limit=limit, filter_document_id=filter_document_id)
        rag_response = await rag_service.answer(rag_request, request_id=request_id)

        # Integrar com Redis: salvar chave f"rag_response:{request_id}" com TTL de 3600
        redis_key = f"rag_response:{request_id}"
        result_payload = {
            "request_id": request_id,
            "answer": rag_response.answer,
            "sources": [
                {
                    "chunk_id": s.chunk_id,
                    "filename": s.filename,
                    "excerpt": s.excerpt,
                    "similarity": s.similarity
                }
                for s in rag_response.sources
            ]
        }
        await cache_service.set(redis_key, result_payload, ttl=3600)
        
        # Atualizar tarefa para COMPLETED com o resultado final (progresso 100%)
        task_service.update_task(
            task_id=target_id,
            status=TaskStatus.COMPLETED,
            progress=100,
            message="Resposta gerada com sucesso",
            result=result_payload
        )
        
        await message.ack()
        BaseLogger.info("[RABBITMQ] Resposta gerada com sucesso")

    except Exception as exc:
        BaseLogger.error(f"[RABBITMQ] Falha ao processar mensagem: {str(exc)}")
        BaseLogger.error("[RABBITMQ] Enviando para DLQ")
        
        # Atualizar status da tarefa para FAILED em caso de erro
        try:
            payload = json.loads(body_str)
            task_id = payload.get("task_id")
            request_id = payload.get("request_id")
            target_id = task_id or request_id
            if target_id:
                task_service.update_task(
                    task_id=target_id,
                    status=TaskStatus.FAILED,
                    progress=0,
                    message="Falha ao processar RAG",
                    error_detail=str(exc)
                )
        except Exception as update_exc:
            BaseLogger.error(f"Erro ao atualizar status da tarefa para FAILED: {str(update_exc)}")

        try:
            # Rejeita sem recolocar na fila principal, enviando para a DLQ via DLX configurado
            await message.reject(requeue=False)
        except Exception as reject_exc:
            BaseLogger.error(f"Erro ao rejeitar mensagem: {str(reject_exc)}")

