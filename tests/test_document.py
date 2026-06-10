import io
import os
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi import status
from fastapi.testclient import TestClient
from app.core.config import settings


def test_upload_markdown_file_success(client: TestClient) -> None:
    """
    Testa se um upload de arquivo Markdown (.md) válido é salvo e enfileirado com sucesso.
    """
    file_content = "# Título do Documento\n\nEste é um parágrafo de teste."
    file_name = "documento_teste.md"
    
    files = {
        "file": (file_name, io.BytesIO(file_content.encode("utf-8")), "text/markdown")
    }
    
    # Mock do envio para a fila do RabbitMQ
    with patch("app.api.v1.endpoints.document.rabbitmq_service.publish_document_processing", new_callable=AsyncMock) as mock_publish:
        mock_publish.return_value = True
        
        response = client.post("/api/v1/document/upload", files=files)
        
        assert response.status_code == status.HTTP_202_ACCEPTED
        data = response.json()
        
        assert data["status"] == "queued"
        assert data["message"] == "Documento enviado para processamento"
        assert "document_id" in data
        mock_publish.assert_called_once()


def test_upload_pdf_file_success(client: TestClient) -> None:
    """
    Testa o upload de um arquivo PDF válido, confirmando se é enfileirado corretamento.
    """
    file_content = b"%PDF-1.4 mock binary content"
    file_name = "manual_corporativo.pdf"
    
    files = {
        "file": (file_name, io.BytesIO(file_content), "application/pdf")
    }
    
    # Mock do envio para a fila do RabbitMQ
    with patch("app.api.v1.endpoints.document.rabbitmq_service.publish_document_processing", new_callable=AsyncMock) as mock_publish:
        mock_publish.return_value = True
        
        response = client.post("/api/v1/document/upload", files=files)
        
        assert response.status_code == status.HTTP_202_ACCEPTED
        data = response.json()
        
        assert data["status"] == "queued"
        assert data["message"] == "Documento enviado para processamento"
        assert "document_id" in data
        mock_publish.assert_called_once()


def test_upload_invalid_extension(client: TestClient) -> None:
    """
    Testa se o endpoint recusa extensões não suportadas como imagens ou planilhas.
    """
    file_content = b"fake image bytes"
    file_name = "foto_perfil.png"
    
    files = {
        "file": (file_name, io.BytesIO(file_content), "image/png")
    }
    
    response = client.post("/api/v1/document/upload", files=files)
    
    assert response.status_code == status.HTTP_400_BAD_REQUEST
    data = response.json()
    assert "Apenas arquivos PDF" in data["detail"]


def test_upload_file_too_large(client: TestClient) -> None:
    """
    Testa se o endpoint recusa arquivos que excedem o tamanho máximo.
    """
    file_content = b"conteudo curto de teste"
    file_name = "documento_grande.md"
    
    files = {
        "file": (file_name, io.BytesIO(file_content), "text/markdown")
    }
    
    with patch("app.core.config.settings.MAX_FILE_SIZE_MB", 0):
        response = client.post("/api/v1/document/upload", files=files)
        assert response.status_code == status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
        data = response.json()
        assert "Arquivo muito grande" in data["detail"]


def test_manual_process_success(client: TestClient) -> None:
    """
    Testa o processamento manual síncrono (fallback) de um arquivo existente.
    """
    document_id = "test-doc-id-123"
    file_name = "doc_manual.pdf"
    safe_filename = f"{document_id}_{file_name}"
    
    # Criar arquivo temporário fake no UPLOAD_DIR para o teste
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    temp_file_path = os.path.join(settings.UPLOAD_DIR, safe_filename)
    with open(temp_file_path, "wb") as f:
        f.write(b"%PDF-1.4 mock manual file")
        
    try:
        # Mock para a extração do texto e persistência
        with patch("app.services.document_processor.document_processor.extract_text_from_pdf") as mock_extract, \
             patch("app.api.v1.endpoints.document.vector_store.upsert_chunks") as mock_upsert:
             
            mock_extract.return_value = ("Este é o texto extraído do PDF manual.", 1)
            mock_upsert.return_value = True
            
            response = client.post(f"/api/v1/document/{document_id}/process")
            
            assert response.status_code == status.HTTP_200_OK
            data = response.json()
            assert data["document_id"] == document_id
            assert data["total_chunks"] > 0
            
            mock_extract.assert_called_once()
            mock_upsert.assert_called_once()
            
    finally:
        # Remover arquivo temporário de teste
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)


# ─────────────────────────────────────────────────────────────
# Testes do endpoint DELETE /api/v1/document/{document_id}
# ─────────────────────────────────────────────────────────────

def test_delete_document_not_found(client: TestClient) -> None:
    """
    Testa que o endpoint retorna 404 quando o document_id não possui
    nenhum registro — nem arquivo físico nem chunks no ChromaDB.
    """
    fake_id = "00000000-0000-0000-0000-000000000000"

    with patch("app.api.v1.endpoints.document.vector_store.collection") as mock_col:
        # ChromaDB retorna lista vazia — nenhum chunk para este ID
        mock_col.get.return_value = {"ids": [], "documents": []}

        response = client.delete(f"/api/v1/document/{fake_id}")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    data = response.json()
    assert "não encontrado" in data["detail"].lower()


def test_delete_document_success_with_file_and_chunks(client: TestClient) -> None:
    """
    Testa o fluxo completo de exclusão:
    - Arquivo físico existe em UPLOAD_DIR.
    - ChromaDB contém chunks associados.
    - Após DELETE: chunks e arquivo são removidos, cache é limpo.
    - Resposta JSON contém todos os campos esperados.
    """
    document_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    file_name = "relatorio_corporativo.md"
    safe_filename = f"{document_id}_{file_name}"

    # Criar arquivo físico temporário no UPLOAD_DIR
    os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
    temp_path = os.path.join(settings.UPLOAD_DIR, safe_filename)
    with open(temp_path, "w", encoding="utf-8") as f:
        f.write("# Conteúdo de teste para exclusão")

    try:
        with patch("app.api.v1.endpoints.document.vector_store.collection") as mock_col, \
             patch("app.api.v1.endpoints.document.vector_store.delete_document", return_value=12) as mock_del, \
             patch("app.api.v1.endpoints.document.cache_service.clear", new_callable=AsyncMock) as mock_cache:

            # Primeira chamada: verificação da existência de chunks
            mock_col.get.return_value = {"ids": [f"{document_id}_chunk_0"], "documents": ["texto"]}

            response = client.delete(f"/api/v1/document/{document_id}")

        assert response.status_code == status.HTTP_200_OK
        data = response.json()

        assert data["success"] is True
        assert data["document_id"] == document_id
        assert data["chunks_removed"] == 12
        assert data["file_removed"] is True
        assert data["message"] == "Documento removido com sucesso."

        # Confirmar que delete_document foi chamado com o document_id correto
        mock_del.assert_called_once_with(document_id)
        # Confirmar que o cache foi invalidado
        mock_cache.assert_called_once()

    finally:
        # Garantir limpeza mesmo em caso de falha
        if os.path.exists(temp_path):
            os.remove(temp_path)


def test_delete_document_success_chunks_only(client: TestClient) -> None:
    """
    Testa a exclusão quando apenas existem chunks no ChromaDB,
    sem arquivo físico correspondente (ex: upload via API deletado manualmente).
    Deve retornar 200 com file_removed=False e chunks_removed > 0.
    """
    document_id = "11111111-2222-3333-4444-555555555555"

    with patch("app.api.v1.endpoints.document.vector_store.collection") as mock_col, \
         patch("app.api.v1.endpoints.document.vector_store.delete_document", return_value=7) as mock_del, \
         patch("app.api.v1.endpoints.document.cache_service.clear", new_callable=AsyncMock):

        mock_col.get.return_value = {"ids": [f"{document_id}_chunk_{i}" for i in range(7)], "documents": ["x"] * 7}

        response = client.delete(f"/api/v1/document/{document_id}")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    assert data["success"] is True
    assert data["document_id"] == document_id
    assert data["chunks_removed"] == 7
    assert data["file_removed"] is False
    assert data["message"] == "Documento removido com sucesso."
    mock_del.assert_called_once_with(document_id)


def test_delete_document_response_schema(client: TestClient) -> None:
    """
    Valida que a resposta do endpoint DELETE possui exatamente os campos
    definidos no schema DocumentDeleteResponse.
    """
    document_id = "cccccccc-dddd-eeee-ffff-aaaaaaaaaaaa"

    with patch("app.api.v1.endpoints.document.vector_store.collection") as mock_col, \
         patch("app.api.v1.endpoints.document.vector_store.delete_document", return_value=3), \
         patch("app.api.v1.endpoints.document.cache_service.clear", new_callable=AsyncMock):

        mock_col.get.return_value = {"ids": ["c1", "c2", "c3"], "documents": ["a", "b", "c"]}

        response = client.delete(f"/api/v1/document/{document_id}")

    assert response.status_code == status.HTTP_200_OK
    data = response.json()

    required_fields = {"success", "document_id", "chunks_removed", "file_removed", "message"}
    assert required_fields == set(data.keys()), (
        f"Campos ausentes ou extras no response. Esperado: {required_fields}. Recebido: {set(data.keys())}"
    )
    assert isinstance(data["success"], bool)
    assert isinstance(data["document_id"], str)
    assert isinstance(data["chunks_removed"], int)
    assert isinstance(data["file_removed"], bool)
    assert isinstance(data["message"], str)
