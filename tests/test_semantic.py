import io
from fastapi import status
from fastapi.testclient import TestClient

from app.services.embedding_service import embedding_service
from app.services.semantic_processor import semantic_processor
from app.schemas.document import DocumentMetadata


def test_embedding_service_dimensions() -> None:
    """
    Testa se o serviço de embeddings gera vetores na dimensão padrão de 384.
    """
    text = "Sentença de teste para validação de dimensão vetorial."
    vector = embedding_service.embed_query(text)
    
    assert isinstance(vector, list)
    assert len(vector) == 384
    assert all(isinstance(x, float) for x in vector)


def test_embedding_service_batch() -> None:
    """
    Testa se o serviço de embeddings em lote retorna a quantidade correta de vetores.
    """
    texts = ["Texto um", "Texto dois", "Texto três"]
    vectors = embedding_service.embed_documents(texts)
    
    assert isinstance(vectors, list)
    assert len(vectors) == 3
    assert all(len(v) == 384 for v in vectors)


def test_semantic_processor_segmentation() -> None:
    """
    Testa a lógica de chunking do SemanticProcessorService diretamente.
    """
    document_id = "doc-123"
    # Um texto longo o suficiente para gerar múltiplos chunks com tamanho limite de 500
    long_text = " ".join([f"Este é o parágrafo número {i} contendo texto semanticamente rico." for i in range(30)])
    
    doc_metadata = DocumentMetadata(
        filename="corporativo_longo.md",
        file_size_bytes=len(long_text),
        content_type="text/markdown",
        char_count=len(long_text)
    )
    
    response = semantic_processor.process_text_into_chunks(
        document_id=document_id,
        text=long_text,
        doc_metadata=doc_metadata
    )
    
    assert response.document_id == document_id
    assert response.total_chunks > 1
    assert len(response.chunks) == response.total_chunks
    
    # Validar metadados do primeiro chunk
    first_chunk = response.chunks[0]
    assert first_chunk.metadata["source_doc_id"] == document_id
    assert first_chunk.metadata["chunk_index"] == 0
    assert len(first_chunk.embedding) == 384


def test_api_semantic_process_flow_success(client: TestClient) -> None:
    """
    Testa o fluxo completo integrado da API:
    1. Upload de documento Markdown
    2. Processamento semântico deste mesmo documento através do ID retornado
    """
    # 1. Upload do arquivo
    file_content = "# Relatorio Financeiro\n\n" + "\n\n".join(
        [f"Linha de relatorio corporativo de auditoria numero {i} para fins semânticos." for i in range(15)]
    )
    files = {
        "file": ("auditoria_anual.md", io.BytesIO(file_content.encode("utf-8")), "text/markdown")
    }
    
    upload_response = client.post("/api/v1/document/upload", files=files)
    assert upload_response.status_code == status.HTTP_201_CREATED
    upload_data = upload_response.json()
    doc_id = upload_data["document_id"]
    
    # 2. Processamento semântico
    process_response = client.post(f"/api/v1/document/{doc_id}/process")
    assert process_response.status_code == status.HTTP_200_OK
    
    process_data = process_response.json()
    assert process_data["document_id"] == doc_id
    assert process_data["total_chunks"] >= 1
    
    # Verificar estrutura de um chunk na resposta HTTP
    chunks = process_data["chunks"]
    first_chunk = chunks[0]
    assert "id" in first_chunk
    assert "text" in first_chunk
    assert "embedding" in first_chunk
    assert "metadata" in first_chunk
    
    assert len(first_chunk["embedding"]) == 384
    assert first_chunk["metadata"]["filename"] == "auditoria_anual.md"
    assert first_chunk["metadata"]["chunk_index"] == 0


def test_api_semantic_process_not_found(client: TestClient) -> None:
    """
    Testa se o endpoint retorna 404 caso seja requisitado o processamento de um ID inválido.
    """
    fake_id = "inexistente-12345"
    response = client.post(f"/api/v1/document/{fake_id}/process")
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "não encontrado" in response.json()["detail"]
