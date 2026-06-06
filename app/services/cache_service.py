import hashlib
import json
from typing import Any, Optional

from app.core.config import settings
from app.core.logging import BaseLogger

# Importação opcional do cliente Redis
try:
    import redis.asyncio as aioredis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class CacheService:
    """
    Serviço corporativo de cache Redis desacoplado.

    Responsabilidades:
        - Conectar ao Redis utilizando REDIS_URL das configurações.
        - Armazenar e recuperar respostas do pipeline RAG por chave determinística.
        - Operar de forma transparente como singleton na aplicação.
        - Degradar graciosamente (no-op) quando o Redis não estiver disponível.

    Métodos públicos:
        get(key)            → Recupera valor do cache ou None.
        set(key, value, ttl)→ Persiste valor no cache com TTL em segundos.
        delete(key)         → Remove uma entrada do cache.
        exists(key)         → Verifica se uma chave existe no cache.
        clear()             → Limpa todas as chaves do banco Redis atual.

    Chave determinística:
        cache_key = sha256(question + "|" + str(limit))
    """

    def __init__(self) -> None:
        self._client: Optional[Any] = None
        self._available: bool = False
        self._connect()

    # ── Inicialização ────────────────────────────────────────────────────────

    def _connect(self) -> None:
        """
        Inicializa o cliente Redis assíncrono a partir de REDIS_URL.
        Em caso de falha, o serviço continua operacional em modo degradado (no-op).
        """
        if not REDIS_AVAILABLE:
            BaseLogger.warning(
                "[REDIS] Biblioteca 'redis[asyncio]' não instalada. "
                "Cache desativado. Instale com: pip install redis[asyncio]"
            )
            return

        try:
            self._client = aioredis.from_url(
                settings.REDIS_URL,
                encoding="utf-8",
                decode_responses=True,
            )
            self._available = True
            BaseLogger.info(
                f"[REDIS] Cliente Redis inicializado com sucesso. URL: {settings.REDIS_URL}"
            )
        except Exception as exc:
            BaseLogger.error(
                f"[REDIS] Erro de conexão ao inicializar cliente Redis: {exc}. "
                "Cache desativado para esta sessão."
            )
            self._available = False

    # ── Geração de chave determinística ─────────────────────────────────────

    @staticmethod
    def build_cache_key(question: str, limit: int) -> str:
        """
        Gera uma chave de cache determinística via SHA-256.

        Args:
            question: Pergunta do usuário.
            limit:    Número máximo de chunks solicitados.

        Returns:
            String hexadecimal de 64 caracteres (SHA-256).

        Exemplo:
            key = CacheService.build_cache_key("O que é RAG?", 4)
            # → "a3f2c1..."
        """
        raw = f"{question.strip()}|{limit}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    # ── Métodos de cache ─────────────────────────────────────────────────────

    async def get(self, key: str) -> Optional[Any]:
        """
        Recupera um valor do cache Redis.

        Args:
            key: Chave de cache (gerada por build_cache_key).

        Returns:
            O objeto deserializado ou None se não encontrado / Redis indisponível.
        """
        if not self._available or self._client is None:
            return None

        try:
            raw = await self._client.get(key)
            if raw is not None:
                BaseLogger.info(f"[REDIS] Cache HIT → chave: {key[:16]}...")
                return json.loads(raw)
            BaseLogger.debug(f"[REDIS] Cache MISS → chave: {key[:16]}...")
            return None
        except Exception as exc:
            BaseLogger.error(f"[REDIS] Erro de conexão ao executar GET: {exc}")
            return None

    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """
        Persiste um valor no cache Redis com expiração automática.

        Args:
            key:   Chave de cache.
            value: Objeto serializável em JSON.
            ttl:   Tempo de vida em segundos (padrão: 3600 = 1 hora).

        Returns:
            True se gravado com sucesso, False caso contrário.
        """
        if not self._available or self._client is None:
            return False

        try:
            serialized = json.dumps(value, ensure_ascii=False)
            await self._client.setex(key, ttl, serialized)
            BaseLogger.info(
                f"[REDIS] Salvando resposta → chave: {key[:16]}... | TTL: {ttl}s"
            )
            return True
        except Exception as exc:
            BaseLogger.error(f"[REDIS] Erro de conexão ao executar SET: {exc}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Remove uma entrada específica do cache Redis.

        Args:
            key: Chave de cache a ser removida.

        Returns:
            True se a chave foi removida, False caso contrário.
        """
        if not self._available or self._client is None:
            return False

        try:
            result = await self._client.delete(key)
            deleted = result > 0
            if deleted:
                BaseLogger.info(f"[REDIS] Chave removida: {key[:16]}...")
            return deleted
        except Exception as exc:
            BaseLogger.error(f"[REDIS] Erro de conexão ao executar DELETE: {exc}")
            return False

    async def exists(self, key: str) -> bool:
        """
        Verifica se uma chave existe no cache Redis.

        Args:
            key: Chave de cache a verificar.

        Returns:
            True se a chave existir, False caso contrário ou Redis indisponível.
        """
        if not self._available or self._client is None:
            return False

        try:
            result = await self._client.exists(key)
            return result > 0
        except Exception as exc:
            BaseLogger.error(f"[REDIS] Erro de conexão ao executar EXISTS: {exc}")
            return False

    async def clear(self) -> bool:
        """
        Apaga todas as chaves do banco Redis atual (FLUSHDB).

        ⚠️  Use com cautela em ambientes compartilhados.

        Returns:
            True se executado com sucesso, False caso contrário.
        """
        if not self._available or self._client is None:
            return False

        try:
            await self._client.flushdb()
            BaseLogger.warning("[REDIS] Cache limpo (FLUSHDB executado).")
            return True
        except Exception as exc:
            BaseLogger.error(f"[REDIS] Erro de conexão ao executar FLUSHDB: {exc}")
            return False

    # ── Propriedade de estado ────────────────────────────────────────────────

    @property
    def is_available(self) -> bool:
        """Indica se o Redis está disponível e configurado."""
        return self._available


# Instanciação Singleton do serviço de cache
cache_service = CacheService()
