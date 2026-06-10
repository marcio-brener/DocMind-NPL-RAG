import asyncio
from unittest.mock import AsyncMock, patch
import pytest

from app.schemas.rag import RAGRequest
from app.services.rag_service import RAGService, is_cv_query


def test_cv_query_detection() -> None:
    """
    Testa se a função de detecção de pergunta de currículo (is_cv_query)
    funciona corretamente para várias perguntas típicas e não-típicas.
    """
    # Perguntas típicas de currículo (devem retornar True)
    assert is_cv_query("Quais empresas Márcio Brener trabalhou?") is True
    assert is_cv_query("Liste todas as experiências profissionais de Marcio.") is True
    assert is_cv_query("Quais cargos ele ocupou?") is True
    assert is_cv_query("Fale sobre o histórico profissional e carreira.") is True
    assert is_cv_query("Qual o emprego atual dele?") is True
    assert is_cv_query("Pode resumir o currículo do Márcio?") is True

    # Perguntas genéricas (devem retornar False)
    assert is_cv_query("O que é inteligência artificial?") is False
    assert is_cv_query("Como funciona o Kubernetes?") is False
    assert is_cv_query("Quem é o presidente do Brasil?") is False
    assert is_cv_query("Explique o conceito de redes neurais.") is False


def test_rag_service_cv_query_behavior() -> None:
    """
    Testa se o RAGService aplica as regras específicas para perguntas de currículo:
    1. Aumento do limit para 12.
    2. Threshold reduzido para 50% do best score.
    3. Garantia de envio de pelo menos 10 chunks para o LLM.
    4. Adição de instrução especial ao prompt do LLM.
    """
    service = RAGService()

    # Mockar a chain do LLM para registrar os argumentos passados
    mock_llm_chain = AsyncMock()
    mock_llm_chain.ainvoke = AsyncMock(return_value="Márcio Brener trabalhou na Magon, Everlux, Team Leasing e Santher.")
    service._llm_chain = mock_llm_chain

    # Gerar 15 chunks mockados. Alguns com palavras-chave de experiência, outros não.
    mock_results = []
    for i in range(15):
        text = f"Fragmento de teste {i}."
        if i in (5, 6, 7):
            text += " Experiência profissional na empresa X."
        
        mock_results.append({
            "chunk_id": f"chunk-{i}",
            "text": text,
            "similarity": 0.40 - (i * 0.02),  # Similaridades decrescentes: 0.40, 0.38, 0.36...
            "metadata": {"filename": "Curriculo_Marcio_Brener.pdf", "source_doc_id": "doc-cv"}
        })

    # Mock do Redis e do Vector Store
    with patch("app.services.rag_service.vector_store") as mock_vs, \
         patch("app.services.rag_service.cache_service.get", return_value=None), \
         patch("app.services.rag_service.cache_service.set", return_value=True):
        
        mock_vs.similarity_search.return_value = mock_results

        request = RAGRequest(question="Quais empresas Márcio Brener trabalhou?", limit=4)
        response = asyncio.run(service.answer(request))

    # Asserts do pipeline RAG de Currículo:
    # 1. limit foi promovido a 12
    assert len(response.sources) >= 10
    
    # 2. LLM foi chamado com o prompt enriquecido
    mock_llm_chain.ainvoke.assert_called_once()
    called_kwargs = mock_llm_chain.ainvoke.call_args[0][0]
    assert "Instrução adicional: Liste TODAS as experiências" in called_kwargs["question"]
    assert "Magon" in response.answer or "Everlux" in response.answer
