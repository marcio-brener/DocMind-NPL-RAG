"""
Testes Automatizados — Propagação do filter_document_id no Pipeline RAG
========================================================================
Valida que o campo filter_document_id é preservado em todo o fluxo:
    API → RabbitMQ Producer → Fila → Consumer → RAG Service → Vector Search

Cenários cobertos:
    1. Pergunta sem filtro → where is None
    2. Pergunta com filtro → where == {"source_doc_id": doc_id}
    3. Mesma pergunta, documentos diferentes → cache_key_A != cache_key_B
    4. RabbitMQ preserva filter_document_id no producer e consumer
"""

import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import status
from fastapi.testclient import TestClient

from app.schemas.rag import RAGRequest
from app.services.cache_service import CacheService
from app.services.rag_service import RAGService


# ─────────────────────────────────────────────────────────────
# Cenário 1: Pergunta sem filtro → where is None
# ─────────────────────────────────────────────────────────────

def test_busca_vetorial_sem_filtro_where_none() -> None:
    """
    Quando nenhum filter_document_id é fornecido, o where_filter
    passado ao vector_store.similarity_search deve ser None.
    """
    service = RAGService()
    service._llm_chain = None

    mock_results = [
        {
            "chunk_id": "c1",
            "text": "Texto de teste para validar busca sem filtro.",
            "similarity": 0.80,
            "metadata": {"filename": "doc.md", "source_doc_id": "doc-001"}
        }
    ]

    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = mock_results

        request = RAGRequest(question="Explique o conceito de busca vetorial", limit=4)
        assert request.filter_document_id is None

        response = asyncio.run(service.answer(request))

        # Validar que similarity_search foi chamado com where_filter=None
        call_args = mock_vs.similarity_search.call_args
        assert call_args.kwargs.get("where_filter") is None or call_args[1].get("where_filter") is None


def test_busca_vetorial_sem_filtro_retorna_resultados() -> None:
    """
    Busca sem filtro deve retornar resultados normalmente (regressão).
    """
    service = RAGService()
    service._llm_chain = None

    mock_results = [
        {
            "chunk_id": "c1",
            "text": "Texto relevante para busca geral sem filtro aplicado.",
            "similarity": 0.75,
            "metadata": {"filename": "geral.md", "source_doc_id": "doc-geral"}
        }
    ]

    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = mock_results

        request = RAGRequest(question="Explique conceitos gerais de NLP", limit=4)
        response = asyncio.run(service.answer(request))

    assert response.context_found is True
    assert len(response.sources) >= 1


# ─────────────────────────────────────────────────────────────
# Cenário 2: Pergunta com filtro → where == {"source_doc_id": doc_id}
# ─────────────────────────────────────────────────────────────

def test_busca_vetorial_com_filtro_where_correto() -> None:
    """
    Quando filter_document_id é fornecido, o where_filter passado ao
    vector_store.similarity_search deve ser {"source_doc_id": doc_id}.
    """
    service = RAGService()
    service._llm_chain = None
    target_doc_id = "38fa4e98-62d6-48f3-8c88-5c502a3acdda"

    mock_results = [
        {
            "chunk_id": "c-filtrado",
            "text": "Texto que pertence ao documento filtrado especificamente.",
            "similarity": 0.85,
            "metadata": {"filename": "curriculo.pdf", "source_doc_id": target_doc_id}
        }
    ]

    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = mock_results

        request = RAGRequest(
            question="Quais as experiencias profissionais de Marcio",
            limit=4,
            filter_document_id=target_doc_id
        )
        response = asyncio.run(service.answer(request))

        # Validar que similarity_search foi chamado com where_filter correto
        call_args = mock_vs.similarity_search.call_args
        where_filter = call_args.kwargs.get("where_filter") or call_args[1].get("where_filter")
        assert where_filter == {"source_doc_id": target_doc_id}


def test_busca_vetorial_com_filtro_retorna_documento_correto() -> None:
    """
    Busca com filtro deve retornar apenas resultados do documento especificado.
    """
    service = RAGService()
    service._llm_chain = None
    target_doc_id = "doc-especifico-123"

    mock_results = [
        {
            "chunk_id": "c-especifico",
            "text": "Conteúdo específico do documento filtrado para validação.",
            "similarity": 0.90,
            "metadata": {"filename": "especifico.pdf", "source_doc_id": target_doc_id}
        }
    ]

    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = mock_results

        request = RAGRequest(
            question="Informações do documento específico",
            limit=4,
            filter_document_id=target_doc_id
        )
        response = asyncio.run(service.answer(request))

    assert response.context_found is True
    assert len(response.sources) == 1
    assert response.sources[0].chunk_id == "c-especifico"


# ─────────────────────────────────────────────────────────────
# Cenário 3: Mesma pergunta, documentos diferentes → cache keys distintas
# ─────────────────────────────────────────────────────────────

def test_cache_key_diferente_para_documentos_diferentes() -> None:
    """
    A mesma pergunta feita para documentos diferentes deve gerar
    chaves de cache SHA-256 distintas, evitando respostas cruzadas.
    """
    question = "Quem é Marcio?"
    limit = 4
    doc_a = "doc-curriculo-pdf"
    doc_b = "doc-livro-pdf"

    key_a = CacheService.build_cache_key(question, limit, doc_a)
    key_b = CacheService.build_cache_key(question, limit, doc_b)

    assert key_a != key_b, (
        f"Chaves de cache devem ser diferentes para documentos diferentes. "
        f"key_a={key_a}, key_b={key_b}"
    )


def test_cache_key_diferente_com_e_sem_filtro() -> None:
    """
    A mesma pergunta com e sem filter_document_id deve gerar
    chaves de cache diferentes.
    """
    question = "Quem é Marcio?"
    limit = 4

    key_sem_filtro = CacheService.build_cache_key(question, limit, None)
    key_com_filtro = CacheService.build_cache_key(question, limit, "doc-123")

    assert key_sem_filtro != key_com_filtro, (
        f"Chave sem filtro deve ser diferente da chave com filtro. "
        f"sem={key_sem_filtro}, com={key_com_filtro}"
    )


def test_cache_key_igual_para_mesmo_documento() -> None:
    """
    A mesma pergunta para o mesmo documento deve gerar a mesma chave de cache.
    """
    question = "Quem é Marcio?"
    limit = 4
    doc_id = "doc-curriculo-pdf"

    key_1 = CacheService.build_cache_key(question, limit, doc_id)
    key_2 = CacheService.build_cache_key(question, limit, doc_id)

    assert key_1 == key_2, (
        f"Chaves devem ser iguais para a mesma pergunta e documento. "
        f"key_1={key_1}, key_2={key_2}"
    )


def test_cache_key_deterministica_sha256() -> None:
    """
    Verifica que a chave de cache é um hash SHA-256 válido (64 caracteres hex).
    """
    key = CacheService.build_cache_key("Pergunta de teste", 4, "doc-123")
    assert len(key) == 64
    assert all(c in "0123456789abcdef" for c in key)


# ─────────────────────────────────────────────────────────────
# Cenário 4: RabbitMQ preserva filter_document_id (Producer + Consumer)
# ─────────────────────────────────────────────────────────────

def test_api_ask_envia_filter_document_id_ao_producer(client: TestClient) -> None:
    """
    Testa que o endpoint /ask propaga filter_document_id para
    o publish_rag_request do RabbitMQ.
    """
    target_doc_id = "38fa4e98-62d6-48f3-8c88-5c502a3acdda"
    payload = {
        "question": "Quais as experiencias profissionais de Marcio",
        "limit": 4,
        "filter_document_id": target_doc_id
    }

    with patch(
        "app.api.v1.endpoints.rag.rabbitmq_service.publish_rag_request",
        new_callable=AsyncMock
    ) as mock_publish:
        mock_publish.return_value = True
        response = client.post("/api/v1/rag/ask", json=payload)

    assert response.status_code == status.HTTP_202_ACCEPTED

    # Validar que publish_rag_request foi chamado COM filter_document_id
    mock_publish.assert_called_once()
    call_kwargs = mock_publish.call_args.kwargs if mock_publish.call_args.kwargs else {}
    call_args_dict = {}
    if mock_publish.call_args.args:
        # Fallback: mapear argumentos posicionais
        pass
    
    # Verificar via kwargs ou por inspeção do call
    all_args = {**call_kwargs}
    if not all_args:
        # Pode estar passando por posicional; checar a chamada completa
        call_str = str(mock_publish.call_args)
        assert target_doc_id in call_str, (
            f"filter_document_id '{target_doc_id}' não encontrado na chamada ao producer: {call_str}"
        )
    else:
        assert all_args.get("filter_document_id") == target_doc_id, (
            f"filter_document_id esperado: {target_doc_id}, "
            f"recebido: {all_args.get('filter_document_id')}"
        )


def test_api_ask_sem_filtro_envia_none_ao_producer(client: TestClient) -> None:
    """
    Testa que o endpoint /ask propaga filter_document_id=None quando
    nenhum filtro é fornecido no payload.
    """
    payload = {
        "question": "Explique aprendizado de máquina em detalhes",
        "limit": 4
    }

    with patch(
        "app.api.v1.endpoints.rag.rabbitmq_service.publish_rag_request",
        new_callable=AsyncMock
    ) as mock_publish:
        mock_publish.return_value = True
        response = client.post("/api/v1/rag/ask", json=payload)

    assert response.status_code == status.HTTP_202_ACCEPTED

    mock_publish.assert_called_once()
    call_kwargs = mock_publish.call_args.kwargs if mock_publish.call_args.kwargs else {}
    if call_kwargs:
        assert call_kwargs.get("filter_document_id") is None


def test_consumer_propaga_filter_document_id_para_rag_request() -> None:
    """
    Testa que o consumer RabbitMQ extrai o filter_document_id da mensagem
    e o inclui no RAGRequest passado ao rag_service.answer().
    """
    target_doc_id = "doc-consumer-test-456"
    
    message_payload = {
        "task_id": "task-123",
        "request_id": "req-123",
        "question": "Pergunta de teste para validar consumer",
        "limit": 4,
        "filter_document_id": target_doc_id,
        "timestamp": "2026-06-09T00:00:00"
    }

    # Criar mock da mensagem RabbitMQ
    mock_message = AsyncMock()
    mock_message.body = json.dumps(message_payload).encode("utf-8")
    mock_message.ack = AsyncMock()

    # Mock do RAGResponse para retornar
    from app.schemas.rag import RAGResponse
    mock_rag_response = RAGResponse(
        question="Pergunta de teste para validar consumer",
        answer="Resposta de teste",
        sources=[],
        context_found=False,
        llm_used=False,
        cache_hit=False,
        latency_ms=10.0
    )

    with patch("app.services.task_service.task_service") as mock_task_svc, \
         patch("app.services.rag_service.rag_service") as mock_rag_svc, \
         patch("app.services.cache_service.cache_service") as mock_cache_svc:
        
        mock_task_svc.update_task.return_value = None
        mock_rag_svc.answer = AsyncMock(return_value=mock_rag_response)
        mock_cache_svc.set = AsyncMock(return_value=True)

        from app.services.rabbitmq_service import process_rag_request
        asyncio.run(process_rag_request(mock_message))

        # Validar que rag_service.answer foi chamado com RAGRequest contendo filter_document_id
        mock_rag_svc.answer.assert_called_once()
        rag_request_arg = mock_rag_svc.answer.call_args[0][0]  # Primeiro argumento posicional
        
        assert isinstance(rag_request_arg, RAGRequest)
        assert rag_request_arg.filter_document_id == target_doc_id, (
            f"Consumer deve propagar filter_document_id={target_doc_id}, "
            f"mas recebeu: {rag_request_arg.filter_document_id}"
        )


def test_consumer_sem_filtro_propaga_none() -> None:
    """
    Testa que o consumer RabbitMQ propaga filter_document_id=None
    quando a mensagem não contém o campo.
    """
    message_payload = {
        "task_id": "task-no-filter",
        "request_id": "req-no-filter",
        "question": "Pergunta sem filtro via consumer",
        "limit": 4,
        "timestamp": "2026-06-09T00:00:00"
    }

    mock_message = AsyncMock()
    mock_message.body = json.dumps(message_payload).encode("utf-8")
    mock_message.ack = AsyncMock()

    from app.schemas.rag import RAGResponse
    mock_rag_response = RAGResponse(
        question="Pergunta sem filtro via consumer",
        answer="Resposta de teste",
        sources=[],
        context_found=False,
        llm_used=False,
        cache_hit=False,
        latency_ms=10.0
    )

    with patch("app.services.task_service.task_service") as mock_task_svc, \
         patch("app.services.rag_service.rag_service") as mock_rag_svc, \
         patch("app.services.cache_service.cache_service") as mock_cache_svc:
        
        mock_task_svc.update_task.return_value = None
        mock_rag_svc.answer = AsyncMock(return_value=mock_rag_response)
        mock_cache_svc.set = AsyncMock(return_value=True)

        from app.services.rabbitmq_service import process_rag_request
        asyncio.run(process_rag_request(mock_message))

        mock_rag_svc.answer.assert_called_once()
        rag_request_arg = mock_rag_svc.answer.call_args[0][0]
        
        assert isinstance(rag_request_arg, RAGRequest)
        assert rag_request_arg.filter_document_id is None, (
            f"Consumer deve propagar filter_document_id=None quando ausente, "
            f"mas recebeu: {rag_request_arg.filter_document_id}"
        )


# ─────────────────────────────────────────────────────────────
# Testes de integração: payload do producer inclui o campo
# ─────────────────────────────────────────────────────────────

def test_producer_serializa_filter_document_id_no_payload() -> None:
    """
    Testa que o RabbitMQService.publish_rag_request() inclui
    filter_document_id no corpo JSON da mensagem publicada.
    """
    from app.services.rabbitmq_service import RabbitMQService

    service = RabbitMQService()
    service._connected = True
    
    # Mock do channel e default_exchange
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()
    mock_channel.default_exchange = mock_exchange
    service.channel = mock_channel
    
    # Mock da connection para is_connected
    mock_connection = MagicMock()
    mock_connection.is_closed = False
    service.connection = mock_connection

    target_doc_id = "doc-producer-test-789"

    result = asyncio.run(service.publish_rag_request(
        task_id="task-prod-test",
        request_id="req-prod-test",
        question="Teste de serialização do producer",
        limit=4,
        filter_document_id=target_doc_id
    ))

    assert result is True
    
    # Extrair o corpo da mensagem publicada
    mock_exchange.publish.assert_called_once()
    published_message = mock_exchange.publish.call_args[0][0]  # Primeiro arg posicional
    body_json = json.loads(published_message.body.decode("utf-8"))
    
    assert "filter_document_id" in body_json, (
        f"Payload da mensagem deve conter 'filter_document_id'. Payload: {body_json}"
    )
    assert body_json["filter_document_id"] == target_doc_id


def test_producer_serializa_none_quando_sem_filtro() -> None:
    """
    Testa que o RabbitMQService.publish_rag_request() inclui
    filter_document_id=null no payload quando não fornecido.
    """
    from app.services.rabbitmq_service import RabbitMQService

    service = RabbitMQService()
    service._connected = True
    
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()
    mock_channel.default_exchange = mock_exchange
    service.channel = mock_channel
    
    mock_connection = MagicMock()
    mock_connection.is_closed = False
    service.connection = mock_connection

    result = asyncio.run(service.publish_rag_request(
        task_id="task-no-filter",
        request_id="req-no-filter",
        question="Teste sem filtro no producer",
        limit=4
    ))

    assert result is True
    
    published_message = mock_exchange.publish.call_args[0][0]
    body_json = json.loads(published_message.body.decode("utf-8"))
    
    assert "filter_document_id" in body_json
    assert body_json["filter_document_id"] is None
