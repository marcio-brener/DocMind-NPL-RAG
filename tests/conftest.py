import pytest
# pyrefly: ignore [missing-import]
from fastapi.testclient import TestClient
from app.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    """
    Fixture que fornece uma instância de TestClient para simular
    requisições HTTP nos testes.
    """
    with TestClient(app) as c:
        yield c
