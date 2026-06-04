# pyrefly: ignore [missing-import]
from fastapi.testclient import TestClient
# pyrefly: ignore [missing-import]
from fastapi import status


def test_health_check_endpoint(client: TestClient) -> None:
    """
    Testa se o endpoint de healthcheck retorna status 200 e a estrutura JSON correta.
    """
    response = client.get("/api/v1/health")
    assert response.status_code == status.HTTP_200_OK
    
    data = response.json()
    
    # Validar campos raiz
    assert "status" in data
    assert "environment" in data
    assert "version" in data
    assert "services" in data
    
    assert data["status"] == "ok"
    assert data["version"] == "1.0.0"
    
    # Validar sub-serviços
    services = data["services"]
    assert "chromadb" in services
    assert "redis" in services
    assert "rabbitmq" in services
    
    # Validar estrutura de um serviço específico
    assert services["chromadb"]["status"] == "healthy (mock)"
