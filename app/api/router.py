# pyrefly: ignore [missing-import]
from fastapi import APIRouter
from app.api.v1.router import api_router as v1_router
from app.core.config import settings

api_router = APIRouter()

# Inclui as rotas do v1 sob o prefixo configurado (/api/v1)
api_router.include_router(v1_router, prefix=settings.API_V1_STR)
