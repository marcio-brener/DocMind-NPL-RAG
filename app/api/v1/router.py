# pyrefly: ignore [missing-import]
from fastapi import APIRouter
from app.api.v1.endpoints import health, document, query, rag

api_router = APIRouter()

# Inclusão dos endpoints individuais da v1
api_router.include_router(health.router, tags=["Health"])
api_router.include_router(document.router, prefix="/document", tags=["Documents"])
api_router.include_router(query.router, prefix="/query", tags=["Search"])
api_router.include_router(rag.router, prefix="/rag", tags=["RAG"])
