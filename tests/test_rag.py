import io
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import status
from fastapi.testclient import TestClient

from app.schemas.rag import RAGRequest
from app.services.rag_service import RAGService


# ─────────────────────────────────────────────────────────────
# Testes Unitários do RAGService (isolados, sem API externa)
# ─────────────────────────────────────────────────────────────

def test_rag_service_fallback_sem_contexto() -> None:
    """
    Testa que o RAGService retorna fallback correto quando não há contexto disponível.
    """
    service = RAGService()

    # Mockar o vector_store para retornar lista vazia
    with patch("app.services.rag_service.vector_store") as mock_vs:
        mock_vs.similarity_search.return_value = []

        import asyncio
        request = RAGRequest(question="O que é inteligência artificial?", limit=4)
        response = asyncio.get_event_loop().run_until_complete(service.answer(request))

    assert response.context_found is False
    assert response.llm_used is False
    assert len(response.sources) == 0
    assert "não foram encontrados" in response.answer.lower()
    assert response.latency_ms > 0


def test_rag_service_fallback_sem_llm_com_contexto() -> None:
    """
    Testa que o RAGService retorna o contexto recuperado quando não há LLM disponível
    mas existem fragmentos relevantes no banco vetorial.
    """
    from app.services.embedding_service import embedding_service

    service = RAGService()
    service._llm_chain = None  # Simular ausência de LLM

    mock_result = [{
        "chunk_id": "chunk-abc-0",
        "text": "O sistema RAG permite combinar recuperação com geração de respostas.",
        "similarity": 0.87,
        "metadata": {"filename": "manual_rag.md", "chunk_index": 0}
    }]

    with patch("app.services.rag_service.vector_store") as mock_vs:
        mock_vs.similarity_search.return_value = mock_result

        import asyncio
        request = RAGRequest(question="Como funciona o pipeline RAG?", limit=3)
        response = asyncio.get_event_loop().run_until_complete(service.answer(request))

    assert response.context_found is True
    assert response.llm_used is False
    assert len(response.sources) == 1
    assert response.sources[0].filename == "manual_rag.md"
    assert response.sources[0].similarity == 0.87
    assert "fallback" in response.answer.lower() or "fragmento" in response.answer.lower()


def test_rag_service_filtra_chunks_por_similaridade_minima() -> None:
    """
    Testa que chunks com similaridade abaixo do limiar mínimo são filtrados e não aparecem nas fontes.
    """
    service = RAGService()
    service._llm_chain = None

    mock_results = [
        {"chunk_id": "c1", "text": "Texto relevante.", "similarity": 0.75, "metadata": {"filename": "a.md"}},
        {"chunk_id": "c2", "text": "Texto irrelevante.", "similarity": 0.10, "metadata": {"filename": "b.md"}},
    ]

    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.core.config.settings.RAG_MIN_SIMILARITY", 0.50):
        mock_vs.similarity_search.return_value = mock_results

        import asyncio
        request = RAGRequest(question="Explique o conceito de relevância semântica.", limit=4)
        response = asyncio.get_event_loop().run_until_complete(service.answer(request))

    # Apenas c1 tem similarity >= 0.50
    assert len(response.sources) == 1
    assert response.sources[0].chunk_id == "c1"


def test_rag_service_com_llm_mockado() -> None:
    """
    Testa o pipeline completo com LLM mockado, verificando que a resposta
    gerada pelo LLM é passada corretamente para a resposta final.
    """
    service = RAGService()

    mock_llm_chain = AsyncMock()
    mock_llm_chain.ainvoke = AsyncMock(
        return_value="O sistema RAG é uma arquitetura que combina recuperação de informações com geração de linguagem natural."
    )
    service._llm_chain = mock_llm_chain

    mock_result = [{
        "chunk_id": "chunk-xyz-1",
        "text": "O RAG (Retrieval-Augmented Generation) é uma técnica avançada de NLP.",
        "similarity": 0.92,
        "metadata": {"filename": "conceitos_nlp.md", "chunk_index": 1}
    }]

    with patch("app.services.rag_service.vector_store") as mock_vs:
        mock_vs.similarity_search.return_value = mock_result

        import asyncio
        request = RAGRequest(question="O que é RAG?", limit=4)
        response = asyncio.get_event_loop().run_until_complete(service.answer(request))

    assert response.context_found is True
    assert response.llm_used is True
    assert "RAG" in response.answer
    assert response.latency_ms > 0
    assert len(response.sources) == 1
    mock_llm_chain.ainvoke.assert_called_once()


# ─────────────────────────────────────────────────────────────
# Testes de Integração via API REST
# ─────────────────────────────────────────────────────────────

def test_api_rag_ask_sem_documentos_retorna_fallback(client: TestClient) -> None:
    """
    Testa que a API retorna uma resposta válida de fallback quando não há documentos ingeridos.
    """
    payload = {"question": "Explique o conceito de aprendizado de máquina.", "limit": 4}
    response = client.post("/api/v1/rag/ask", json=payload)

    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    assert "question" in data
    assert "answer" in data
    assert "sources" in data
    assert "context_found" in data
    assert "llm_used" in data
    assert "latency_ms" in data
    assert isinstance(data["sources"], list)
    assert data["latency_ms"] > 0


def test_api_rag_pipeline_completo(client: TestClient) -> None:
    """
    Testa o pipeline RAG completo via API:
    1. Upload de documento
    2. Processamento semântico (chunking + embedding + ChromaDB)
    3. Pergunta ao sistema RAG
    4. Validação da resposta com referências de fontes
    """
    # 1. Upload
    content = "# Fundamentos de Machine Learning\n\n" + "\n\n".join([
        f"Parágrafo {i}: Redes neurais são modelos inspirados no cérebro humano usados para reconhecimento de padrões complexos."
        for i in range(12)
    ])
    files = {"file": ("fundamentos_ml.md", io.BytesIO(content.encode("utf-8")), "text/markdown")}
    upload = client.post("/api/v1/document/upload", files=files)
    assert upload.status_code == status.HTTP_201_CREATED
    doc_id = upload.json()["document_id"]

    # 2. Processar e persistir no ChromaDB
    process = client.post(f"/api/v1/document/{doc_id}/process")
    assert process.status_code == status.HTTP_200_OK
    assert process.json()["total_chunks"] >= 1

    # 3. Pergunta ao RAG (mock do LLM para controlar o output)
    with patch("app.services.rag_service.rag_service._llm_chain") as mock_chain:
        mock_chain.ainvoke = AsyncMock(
            return_value="Redes neurais são modelos computacionais inspirados no cérebro humano."
        )

        payload = {"question": "O que são redes neurais?", "limit": 3}
        rag_resp = client.post("/api/v1/rag/ask", json=payload)

    assert rag_resp.status_code == status.HTTP_200_OK
    data = rag_resp.json()

    assert data["context_found"] is True
    assert len(data["sources"]) >= 1
    assert data["latency_ms"] > 0

    # Validar estrutura de cada source
    for source in data["sources"]:
        assert "chunk_id" in source
        assert "filename" in source
        assert "excerpt" in source
        assert "similarity" in source
        assert 0.0 <= source["similarity"] <= 1.0


def test_api_rag_validacao_payload_incompleto(client: TestClient) -> None:
    """
    Testa que a API retorna 422 quando a pergunta não é fornecida.
    """
    response = client.post("/api/v1/rag/ask", json={"limit": 4})
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


def test_api_rag_validacao_pergunta_curta(client: TestClient) -> None:
    """
    Testa que a API rejeita perguntas muito curtas (< 5 chars).
    """
    response = client.post("/api/v1/rag/ask", json={"question": "oi"})
    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
