import io
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch
from fastapi import status
from fastapi.testclient import TestClient

from app.schemas.semantic import Chunk
from app.services.vector_store import VectorStoreService


def _make_chunk(doc_id: str = "doc-test", idx: int = 0) -> Chunk:
    """Cria um chunk de teste com embedding determinístico de 384 dimensões."""
    from app.services.embedding_service import embedding_service
    text = f"Texto do fragmento número {idx} gerado para testes de persistência vetorial."
    return Chunk(
        id=f"chunk-{doc_id}-{idx}",
        text=text,
        embedding=embedding_service.embed_query(text),
        metadata={
            "source_doc_id": doc_id,
            "filename": "manual_teste.md",
            "chunk_index": idx,
            "total_chunks": 3,
            "char_count": len(text),
            "uploaded_at": "2026-06-01T00:00:00"
        }
    )


def test_vector_store_upsert_and_search() -> None:
    """
    Testa o ciclo completo de inserção e busca no ChromaDB usando um diretório temporário isolado.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch("app.core.config.settings.CHROMADB_PATH", tmp_dir):
            store = VectorStoreService(collection_name="test_collection")
            
            # 1. Criar e inserir chunks de teste
            chunks = [_make_chunk("doc-A", i) for i in range(3)]
            result = store.upsert_chunks(chunks)
            
            assert result is True

            # 2. Buscar por similaridade usando o vetor do chunk 0
            query_vector = chunks[0].embedding
            results = store.similarity_search(query_vector=query_vector, limit=3)

            assert isinstance(results, list)
            assert len(results) > 0
            
            first = results[0]
            assert "chunk_id" in first
            assert "text" in first
            assert "similarity" in first
            assert "metadata" in first
            assert isinstance(first["similarity"], float)
            assert 0.0 <= first["similarity"] <= 1.0
            
            # O chunk mais similar deve ser o próprio chunk[0] (similaridade ~1.0)
            assert first["similarity"] > 0.95

            # Clean up ChromaDB client to release file lock on Windows
            if store.client and hasattr(store.client, "_system") and hasattr(store.client._system, "stop"):
                store.client._system.stop()


def test_vector_store_upsert_empty_list() -> None:
    """
    Testa que o upsert com lista vazia retorna False sem lançar exceções.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch("app.core.config.settings.CHROMADB_PATH", tmp_dir):
            store = VectorStoreService(collection_name="test_empty")
            result = store.upsert_chunks([])
            assert result is False

            # Clean up ChromaDB client to release file lock on Windows
            if store.client and hasattr(store.client, "_system") and hasattr(store.client._system, "stop"):
                store.client._system.stop()


def test_vector_store_delete_document() -> None:
    """
    Testa se a exclusão de chunks por document_id funciona corretamente.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        with patch("app.core.config.settings.CHROMADB_PATH", tmp_dir):
            store = VectorStoreService(collection_name="test_delete")
            
            chunks = [_make_chunk("doc-delete", i) for i in range(2)]
            store.upsert_chunks(chunks)
            
            # Verificar que existem resultados antes da exclusão
            query_vector = chunks[0].embedding
            before = store.similarity_search(query_vector=query_vector, limit=5)
            assert len(before) > 0

            # Deletar e confirmar
            deleted = store.delete_document_chunks("doc-delete")
            assert deleted is True

            # Clean up ChromaDB client to release file lock on Windows
            if store.client and hasattr(store.client, "_system") and hasattr(store.client._system, "stop"):
                store.client._system.stop()


def test_api_full_pipeline_with_search(client: TestClient) -> None:
    """
    Testa o pipeline integrado completo via API:
    1. Upload do documento Markdown
    2. Processamento semântico + persistência automática no ChromaDB
    3. Busca semântica retornando fragmentos do documento inserido
    """
    # 1. Upload do documento
    file_content = "# Guia de Arquitetura\n\n" + "\n\n".join(
        [f"Seção {i}: Este capítulo discute {i * 10} padrões de design de sistemas distribuídos." for i in range(10)]
    )
    files = {"file": ("arquitetura.md", io.BytesIO(file_content.encode("utf-8")), "text/markdown")}
    upload_resp = client.post("/api/v1/document/upload", files=files)
    assert upload_resp.status_code == status.HTTP_202_ACCEPTED
    doc_id = upload_resp.json()["document_id"]

    # 2. Processamento semântico e persistência no ChromaDB
    process_resp = client.post(f"/api/v1/document/{doc_id}/process")
    assert process_resp.status_code == status.HTTP_200_OK
    assert process_resp.json()["total_chunks"] >= 1

    # 3. Busca semântica
    search_payload = {
        "query": "padrões de design de sistemas distribuídos",
        "limit": 3
    }
    search_resp = client.post("/api/v1/query/search", json=search_payload)
    assert search_resp.status_code == status.HTTP_200_OK

    data = search_resp.json()
    assert data["total_results"] >= 1
    assert data["query"] == search_payload["query"]
    
    first_result = data["results"][0]
    assert "text" in first_result
    assert "similarity" in first_result
    assert "metadata" in first_result
    assert "chunk_id" in first_result
    assert isinstance(first_result["similarity"], float)


def test_api_search_returns_empty_gracefully(client: TestClient) -> None:
    """
    Testa que uma busca semântica em uma coleção com resultados retorna resposta válida.
    """
    search_payload = {
        "query": "consulta totalmente aleatória de topico incomum xyz987",
        "limit": 2
    }
    response = client.post("/api/v1/query/search", json=search_payload)
    assert response.status_code == status.HTTP_200_OK
    
    data = response.json()
    assert "results" in data
    assert "total_results" in data
    assert isinstance(data["results"], list)
