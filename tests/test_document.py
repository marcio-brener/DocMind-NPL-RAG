import io
from unittest.mock import patch
from fastapi import status
from fastapi.testclient import TestClient


def test_upload_markdown_file_success(client: TestClient) -> None:
    """
    Testa se um upload de arquivo Markdown (.md) válido é ingerido e limpo com sucesso.
    """
    file_content = "# Título do Documento\n\nEste é um parágrafo de teste com múltiplos   espaços."
    file_name = "documento_teste.md"
    
    files = {
        "file": (file_name, io.BytesIO(file_content.encode("utf-8")), "text/markdown")
    }
    
    response = client.post("/api/v1/document/upload", files=files)
    
    assert response.status_code == status.HTTP_201_CREATED
    data = response.json()
    
    assert data["status"] == "processed"
    assert data["filename"] == file_name
    assert "document_id" in data
    assert "metadata" in data
    
    metadata = data["metadata"]
    assert metadata["filename"] == file_name
    assert metadata["content_type"] == "text/markdown"
    assert metadata["page_count"] is None
    # Verifica se os espaços múltiplos foram limpos (ex: "múltiplos   espaços" -> "múltiplos espaços")
    assert "múltiplos espaços" in data["excerpt"]


def test_upload_pdf_file_success(client: TestClient) -> None:
    """
    Testa o upload de um arquivo PDF simulado, mockando a extração do PdfReader
    para evitar falhas com arquivos binários malformados.
    """
    file_content = b"%PDF-1.4 mock binary content"
    file_name = "manual_corporativo.pdf"
    
    files = {
        "file": (file_name, io.BytesIO(file_content), "application/pdf")
    }
    
    # Mock do método de extração do PDF do próprio serviço
    with patch("app.services.document_processor.document_processor.extract_text_from_pdf") as mock_extract:
        mock_extract.return_value = ("Este é o texto extraído de um PDF corporativo real contendo informações confidenciais.", 5)
        
        response = client.post("/api/v1/document/upload", files=files)
        
        assert response.status_code == status.HTTP_201_CREATED
        data = response.json()
        
        assert data["status"] == "processed"
        assert data["filename"] == file_name
        
        metadata = data["metadata"]
        assert metadata["content_type"] == "application/pdf"
        assert metadata["page_count"] == 5
        assert "PDF corporativo" in data["excerpt"]
        
        mock_extract.assert_called_once()


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
    # Criamos um arquivo com tamanho maior que 10MB em bytes para simular a falha
    # Para economizar RAM, vamos forçar uma configuração temporária de tamanho limite bem baixo (ex: 0 MB)
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
