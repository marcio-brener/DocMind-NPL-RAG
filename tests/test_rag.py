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
    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = []

        import asyncio
        request = RAGRequest(question="O que é inteligência artificial?", limit=4)
        response = asyncio.run(service.answer(request))

    assert response.context_found is False
    assert response.llm_used is False
    assert len(response.sources) == 0
    assert "não encontrei informações suficientes" in response.answer.lower()
    assert response.latency_ms >= 0


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

    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = mock_result

        import asyncio
        request = RAGRequest(question="Como funciona o pipeline RAG?", limit=3)
        response = asyncio.run(service.answer(request))

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
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True), \
         patch("app.core.config.settings.RAG_MIN_SIMILARITY", 0.50):
        mock_vs.similarity_search.return_value = mock_results

        import asyncio
        request = RAGRequest(question="Explique o conceito de relevância semântica.", limit=4)
        response = asyncio.run(service.answer(request))

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

    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = mock_result

        import asyncio
        request = RAGRequest(question="O que é RAG?", limit=4)
        response = asyncio.run(service.answer(request))

    assert response.context_found is True
    assert response.llm_used is True
    assert "RAG" in response.answer
    assert response.latency_ms >= 0
    assert len(response.sources) == 1
    mock_llm_chain.ainvoke.assert_called_once()


# ─────────────────────────────────────────────────────────────
# Testes de Integração via API REST
# ─────────────────────────────────────────────────────────────

def test_api_rag_ask_sem_documentos_retorna_fallback(client: TestClient) -> None:
    """
    Testa que a API do /ask aceita a pergunta de forma assíncrona, enfileira,
    e retorna o resultado correto no endpoint de consulta /tasks/{task_id}.
    """
    payload = {"question": "Explique o conceito de aprendizado de máquina.", "limit": 4}
    
    with patch("app.api.v1.endpoints.rag.rabbitmq_service.publish_rag_request", new_callable=AsyncMock) as mock_publish:
        mock_publish.return_value = True
        response = client.post("/api/v1/rag/ask", json=payload)

    assert response.status_code == status.HTTP_202_ACCEPTED
    data = response.json()

    assert "task_id" in data
    assert "request_id" in data
    assert data["status"] == "PROCESSING"
    assert "timestamp" in data
    task_id = data["task_id"]
    request_id = data["request_id"]

    # 1. Consultar resultado quando ainda não processado
    with patch("app.api.v1.endpoints.tasks.task_service.get_task") as mock_get_task:
        from app.schemas.task import TaskResponse, TaskStatus
        from datetime import datetime
        mock_get_task.return_value = TaskResponse(
            task_id=task_id,
            document_id=None,
            filename=None,
            status=TaskStatus.PROCESSING,
            progress=50,
            message="Executando pipeline RAG",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        result_response = client.get(f"/api/v1/tasks/{task_id}")
        assert result_response.status_code == status.HTTP_200_OK
        assert result_response.json()["status"] == "PROCESSING"

    # 2. Consultar resultado quando já processado (mockando retorno)
    mock_cached_result = {
        "request_id": request_id,
        "answer": "Não foram encontrados fragmentos de documentos relevantes para esta pergunta...",
        "sources": []
    }
    with patch("app.api.v1.endpoints.tasks.task_service.get_task") as mock_get_task:
        from app.schemas.task import TaskResponse, TaskStatus
        from datetime import datetime
        mock_get_task.return_value = TaskResponse(
            task_id=task_id,
            document_id=None,
            filename=None,
            status=TaskStatus.COMPLETED,
            progress=100,
            message="Resposta gerada com sucesso",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            result=mock_cached_result
        )
        result_response = client.get(f"/api/v1/tasks/{task_id}")
        assert result_response.status_code == status.HTTP_200_OK
        result_data = result_response.json()
        assert result_data["status"] == "COMPLETED"
        assert result_data["task_id"] == task_id
        assert "não foram encontrados" in result_data["result"]["answer"].lower()
        assert isinstance(result_data["result"]["sources"], list)
        assert len(result_data["result"]["sources"]) == 0


def test_api_rag_pipeline_completo(client: TestClient) -> None:
    """
    Testa o pipeline RAG completo via API:
    1. Upload de documento
    2. Processamento semântico (chunking + embedding + ChromaDB)
    3. Pergunta assíncrona ao sistema RAG (/ask)
    4. Consulta do resultado da tarefa (/tasks/{task_id}) retornando referências
    """
    # 1. Upload
    content = "# Fundamentos de Machine Learning\n\n" + "\n\n".join([
        f"Parágrafo {i}: Redes neurais são modelos inspirados no cérebro humano usados para reconhecimento de padrões complexos."
        for i in range(12)
    ])
    files = {"file": ("fundamentos_ml.md", io.BytesIO(content.encode("utf-8")), "text/markdown")}
    
    with patch("app.api.v1.endpoints.document.rabbitmq_service.publish_document_processing", new_callable=AsyncMock) as mock_pub_doc:
        mock_pub_doc.return_value = True
        upload = client.post("/api/v1/document/upload", files=files)
        
    assert upload.status_code == status.HTTP_202_ACCEPTED
    doc_id = upload.json()["document_id"]

    # 2. Processar e persistir no ChromaDB
    process = client.post(f"/api/v1/document/{doc_id}/process")
    assert process.status_code == status.HTTP_200_OK
    assert process.json()["total_chunks"] >= 1

    # 3. Pergunta ao RAG (mock do enfileiramento e do Redis)
    payload = {"question": "O que são redes neurais?", "limit": 3}
    with patch("app.api.v1.endpoints.rag.rabbitmq_service.publish_rag_request", new_callable=AsyncMock) as mock_publish:
        mock_publish.return_value = True
        rag_resp = client.post("/api/v1/rag/ask", json=payload)

    assert rag_resp.status_code == status.HTTP_202_ACCEPTED
    task_id = rag_resp.json()["task_id"]
    request_id = rag_resp.json()["request_id"]

    # Mockar a resposta que o consumer salvaria no campo result da tarefa
    mock_result_payload = {
        "request_id": request_id,
        "answer": "Redes neurais são modelos computacionais inspirados no cérebro humano.",
        "sources": [
            {
                "chunk_id": "chunk-xyz-123",
                "filename": "fundamentos_ml.md",
                "excerpt": "Redes neurais são modelos inspirados no cérebro humano...",
                "similarity": 0.92
            }
        ]
    }

    # 4. Consultar resultado do processamento da tarefa
    with patch("app.api.v1.endpoints.tasks.task_service.get_task") as mock_get_task:
        from app.schemas.task import TaskResponse, TaskStatus
        from datetime import datetime
        mock_get_task.return_value = TaskResponse(
            task_id=task_id,
            document_id=None,
            filename=None,
            status=TaskStatus.COMPLETED,
            progress=100,
            message="Resposta gerada com sucesso",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            result=mock_result_payload
        )
        result_resp = client.get(f"/api/v1/tasks/{task_id}")

    assert result_resp.status_code == status.HTTP_200_OK
    data = result_resp.json()

    assert data["status"] == "COMPLETED"
    assert data["task_id"] == task_id
    assert "redes neurais" in data["result"]["answer"].lower()
    assert len(data["result"]["sources"]) >= 1

    # Validar estrutura de cada source no resultado
    for source in data["result"]["sources"]:
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


def test_rag_service_dynamic_threshold_and_filter() -> None:
    """
    Testa se o threshold dinâmico e o filtro de documento são aplicados.
    """
    service = RAGService()
    
    mock_results = [
        {"chunk_id": "c1", "text": "Texto RideBuddy de TI.", "similarity": 0.48, "metadata": {"filename": "a.md", "source_doc_id": "doc-123"}},
        {"chunk_id": "c2", "text": "Outro texto irrelevante.", "similarity": 0.20, "metadata": {"filename": "b.md", "source_doc_id": "doc-456"}},
    ]
    
    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        mock_vs.similarity_search.return_value = mock_results
        
        import asyncio
        # Caso 1: Busca geral sem filtro de doc.
        # Max similarity é 0.48. O threshold dinâmico será max(0.48 * 0.82, 0.35) = 0.3936.
        # Chunks com similarity >= 0.3936 passarão (neste caso, c1 passa, c2 é descartado).
        request = RAGRequest(question="RideBuddy", limit=4)
        response = asyncio.run(service.answer(request))
        
        assert response.context_found is True
        assert len(response.sources) == 1
        assert response.sources[0].chunk_id == "c1"
        
        # Caso 2: Busca filtrada por document_id que não retorna nenhum resultado do banco vetorial.
        request_filtered = RAGRequest(question="RideBuddy", limit=4, filter_document_id="doc-456")
        mock_vs.similarity_search.return_value = []  # Banco retorna vazio para este filtro
        response_filtered = asyncio.run(service.answer(request_filtered))
        
        # Como nenhum chunk foi retornado pelo banco vetorial, não há contexto.
        assert response_filtered.context_found is False
        assert len(response_filtered.sources) == 0


def test_rag_service_new_features() -> None:
    """
    Testa as novas funcionalidades do RAGService:
    1. Similaridade baixa porém válida.
    2. Threshold adaptativo.
    3. Fallback Top-K.
    4. Busca com filter_document_id.
    5. Busca sem filter_document_id.
    6. Cenário onde o melhor score é inferior ao baseline.
    7. Garantia de que pelo menos um chunk é enviado ao LLM.
    """
    service = RAGService()
    
    mock_results = [
        {"chunk_id": "c1", "text": "Python e Docker no DocMind", "similarity": 0.25, "metadata": {"filename": "doc_a.md", "source_doc_id": "doc-a"}},
        {"chunk_id": "c2", "text": "Outro texto aleatório e irrelevante", "similarity": 0.15, "metadata": {"filename": "doc_b.md", "source_doc_id": "doc-b"}},
        {"chunk_id": "c3", "text": "Mais um fragmento genérico sem correspondência", "similarity": 0.10, "metadata": {"filename": "doc_c.md", "source_doc_id": "doc-c"}},
        {"chunk_id": "c4", "text": "ChromaDB vetorial em nuvem", "similarity": 0.05, "metadata": {"filename": "doc_d.md", "source_doc_id": "doc-d"}},
    ]
    
    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        
        import asyncio
        
        # --- Caso 1, 5 e 6: Busca sem filter_document_id, melhor score (0.25) < baseline (0.35) ---
        # A pergunta "DocMind" tem overlap com "Python e Docker no DocMind".
        # Overlap de "DocMind" com c1 é 1.0 (1/1 token).
        # Hybrid score de c1 = max(0.25, 0.7 * 0.25 + 0.3 * 1.0) = max(0.25, 0.175 + 0.30) = 0.475.
        # Outros chunks têm overlap = 0.0.
        # Hybrid score de c2 = max(0.15, 0.7 * 0.15 + 0) = 0.15.
        # Hybrid score de c3 = 0.10, c4 = 0.05.
        # Max hybrid score = 0.475, que é >= 0.35 baseline.
        # Portanto, threshold = max(0.475 * 0.82, 0.35) = max(0.3895, 0.35) = 0.3895.
        # Apenas c1 (0.475 >= 0.3895) passa. c2, c3, c4 são rejeitados.
        mock_vs.similarity_search.return_value = mock_results
        request = RAGRequest(question="DocMind", limit=4)
        response = asyncio.run(service.answer(request))
        
        assert response.context_found is True
        assert len(response.sources) == 1
        assert response.sources[0].chunk_id == "c1"
        
        # --- Caso 2: Threshold Adaptativo menor que 0.35 ---
        # Se todos os chunks têm zero overlap com a pergunta "XyzUnicorn",
        # seus hybrid scores são iguais às suas similaridades (c1=0.25, c2=0.15, c3=0.10, c4=0.05).
        # Max hybrid score = 0.25, que é < 0.35 baseline.
        # Limiar dinâmico adaptativo = 0.25 * 0.70 = 0.175.
        # Chunks com score >= 0.175 devem passar (neste caso, c1=0.25 passa, c2=0.15 rejeitado, etc.).
        request_adapt = RAGRequest(question="XyzUnicorn", limit=4)
        response_adapt = asyncio.run(service.answer(request_adapt))
        
        assert response_adapt.context_found is True
        assert len(response_adapt.sources) == 1
        assert response_adapt.sources[0].chunk_id == "c1"
        
        # --- Caso 4: Busca com filter_document_id ---
        # Filtramos por "doc-b", então mock_vs deve retornar apenas c2.
        # Se re-enviarmos a mesma pergunta "DocMind" com filtro "doc-b", apenas c2 deve ser retornado pelo mock vector store.
        mock_vs.similarity_search.return_value = [mock_results[1]]  # Apenas c2
        request_filtered = RAGRequest(question="DocMind", limit=4, filter_document_id="doc-b")
        response_filtered = asyncio.run(service.answer(request_filtered))
        
        # c2 tem similaridade 0.15. Com overlap 0.0, hybrid_score = 0.15.
        # Max hybrid score = 0.15 < 0.35 baseline.
        # Limiar adaptativo = 0.15 * 0.70 = 0.105.
        # c2 (0.15 >= 0.105) passa.
        assert response_filtered.context_found is True
        assert len(response_filtered.sources) == 1
        assert response_filtered.sources[0].chunk_id == "c2"
        
        # --- Caso 3 e 7: Fallback Top-K e Garantia de envio ---
        # Se os scores forem todos 0.0, a condição score > 0.0 fará todos serem rejeitados.
        # Isso ativará o fallback Top-K, que trará os 3 primeiros chunks.
        zero_results = [
            {"chunk_id": "c1", "text": "Texto A", "similarity": 0.0, "metadata": {"filename": "a.md", "source_doc_id": "doc-a"}},
            {"chunk_id": "c2", "text": "Texto B", "similarity": 0.0, "metadata": {"filename": "b.md", "source_doc_id": "doc-b"}},
            {"chunk_id": "c3", "text": "Texto C", "similarity": 0.0, "metadata": {"filename": "c.md", "source_doc_id": "doc-c"}},
            {"chunk_id": "c4", "text": "Texto D", "similarity": 0.0, "metadata": {"filename": "d.md", "source_doc_id": "doc-d"}},
        ]
        mock_vs.similarity_search.return_value = zero_results
        
        request_fallback = RAGRequest(question="PerguntaSemNexo", limit=4)
        response_fallback = asyncio.run(service.answer(request_fallback))
        
        assert response_fallback.context_found is True
        assert len(response_fallback.sources) == 3
        assert response_fallback.sources[0].chunk_id == "c1"
        assert response_fallback.sources[1].chunk_id == "c2"
        assert response_fallback.sources[2].chunk_id == "c3"
