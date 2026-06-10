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
