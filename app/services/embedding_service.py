import hashlib
import random
from typing import List
from app.core.config import settings
from app.core.logging import BaseLogger

# Tentativa de importação das bibliotecas do LangChain para Embeddings locais
try:
    from langchain_community.embeddings import HuggingFaceEmbeddings
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


class EmbeddingService:
    """
    Serviço corporativo de geração de vetores (embeddings).
    Possui suporte integrado para carregar localmente modelos HuggingFace (sentence-transformers)
    e um fallback determinístico offline em caso de falha de conexão ou ausência de GPU/Internet.
    """

    def __init__(self) -> None:
        self.model = None
        self.is_fallback = False
        self.dimension = 384  # Dimensão padrão do modelo sentence-transformers/all-MiniLM-L6-v2

        if LANGCHAIN_AVAILABLE:
            try:
                BaseLogger.info(f"Carregando modelo de embeddings local: {settings.EMBEDDING_MODEL_NAME}...")
                self.model = HuggingFaceEmbeddings(
                    model_name=settings.EMBEDDING_MODEL_NAME,
                    encode_kwargs={"normalize_embeddings": True}
                )
                BaseLogger.info("Modelo de embeddings carregado com sucesso pelo HuggingFace.")
            except Exception as e:
                BaseLogger.warning(
                    f"Falha ao inicializar modelo HuggingFace: {str(e)}. "
                    f"Ativando fallback determinístico offline para evitar travamentos."
                )
                self.is_fallback = True
        else:
            BaseLogger.warning("LangChain Community não disponível. Ativando fallback determinístico offline.")
            self.is_fallback = True

    def _generate_mock_embedding(self, text: str) -> List[float]:
        """
        Gera um vetor de embedding determinístico baseado no hash do texto.
        Útil para testes, CI/CD ou desenvolvimento offline sem internet.
        Retorna um vetor normalizado de tamanho `dimension`.
        """
        # Cria um hash sha256 do texto para servir como semente determinística
        hash_digest = hashlib.sha256(text.encode("utf-8")).digest()
        
        # Semeia o gerador de números aleatórios de forma local (thread-safe)
        rng = random.Random(int.from_bytes(hash_digest, "big"))
        
        # Gera o vetor
        vector = [rng.uniform(-1.0, 1.0) for _ in range(self.dimension)]
        
        # Normaliza o vetor (L2 norm) para cosseno-similaridade consistente
        norm = sum(x * x for x in vector) ** 0.5
        if norm > 0:
            vector = [x / norm for x in vector]
            
        return vector

    def embed_query(self, text: str) -> List[float]:
        """
        Gera o vetor (embedding) para uma única sentença ou query.
        """
        if self.is_fallback or not self.model:
            return self._generate_mock_embedding(text)
        
        try:
            return self.model.embed_query(text)
        except Exception as e:
            BaseLogger.error(f"Erro ao gerar embedding de query com HuggingFace: {str(e)}. Usando fallback.")
            return self._generate_mock_embedding(text)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """
        Gera vetores (embeddings) em lote para uma lista de textos.
        """
        if not texts:
            return []

        if self.is_fallback or not self.model:
            return [self._generate_mock_embedding(t) for t in texts]
        
        try:
            return self.model.embed_documents(texts)
        except Exception as e:
            BaseLogger.error(f"Erro ao gerar embeddings de lote com HuggingFace: {str(e)}. Usando fallback.")
            return [self._generate_mock_embedding(t) for t in texts]


# Instanciação Singleton do serviço de embeddings
embedding_service = EmbeddingService()
