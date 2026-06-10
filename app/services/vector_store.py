import os
import chromadb
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.semantic import Chunk


class VectorStoreService:
    """
    Serviço responsável pela persistência vetorial utilizando ChromaDB.
    """

    def __init__(self, collection_name: str = "nlp_rag_collection") -> None:
        self.collection_name = collection_name
        self.client = None
        self.collection = None

        self._initialize_chroma()

    def _initialize_chroma(self) -> None:
        try:
            os.makedirs(settings.CHROMADB_PATH, exist_ok=True)

            BaseLogger.info(
                f"Conectando ao ChromaDB em: {settings.CHROMADB_PATH}"
            )

            self.client = chromadb.PersistentClient(
                path=settings.CHROMADB_PATH
            )

            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={
                    "hnsw:space": "cosine"
                }
            )

            BaseLogger.info(
                f"Coleção '{self.collection_name}' conectada com sucesso."
            )

        except Exception as e:
            BaseLogger.error(
                f"Falha ao conectar ao ChromaDB: {str(e)}"
            )
            raise

    def upsert_chunks(self, chunks: List[Chunk]) -> bool:

        if not chunks:
            BaseLogger.warning(
                "Nenhum chunk recebido para persistência."
            )
            return False

        try:
            ids = [chunk.id for chunk in chunks]
            embeddings = [chunk.embedding for chunk in chunks]
            documents = [chunk.text for chunk in chunks]
            metadatas = [chunk.metadata for chunk in chunks]

            BaseLogger.info(
                f"Persistindo {len(chunks)} chunks..."
            )

            self.collection.upsert(
                ids=ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas
            )

            BaseLogger.info(
                "Chunks persistidos com sucesso."
            )

            return True

        except Exception as e:
            BaseLogger.error(
                f"Erro ao persistir chunks: {str(e)}"
            )
            return False

    def similarity_search(
        self,
        query_vector: List[float],
        limit: int = 4,
        where_filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:

        try:

            BaseLogger.info(
                f"Executando busca vetorial (limit={limit}, where={where_filter})"
            )

            results = self.collection.query(
                query_embeddings=[query_vector],
                n_results=limit,
                where=where_filter,
                include=[
                    "documents",
                    "metadatas",
                    "distances"
                ]
            )

            formatted_results: List[Dict[str, Any]] = []

            ids = results.get("ids", [[]])[0]
            docs = results.get("documents", [[]])[0]
            metas = results.get("metadatas", [[]])[0]
            distances = results.get("distances", [[]])[0]

            if not ids:
                BaseLogger.warning(
                    "Nenhum resultado encontrado."
                )
                return []

            space = "cosine"
            if self.collection and self.collection.metadata:
                space = self.collection.metadata.get("hnsw:space", "cosine")

            # Log debug do Chroma
            BaseLogger.info(
                "===== CHROMA DEBUG =====\n"
                f"SPACE: {space}\n"
                f"DISTANCES: {distances}\n"
                f"IDS: {ids}\n"
                "=========="
            )

            for idx in range(len(ids)):
                distance = float(distances[idx])

                if space == "l2":
                    # Se o vetor estiver normalizado, a distância L2 quadrada é 2 * (1 - cos_sim)
                    if distance > 2.0:
                        similarity = 1.0 / (1.0 + distance)
                    else:
                        similarity = max(0.0, 1.0 - (distance / 2.0))
                elif space == "ip":
                    similarity = max(0.0, 1.0 - distance)
                else:  # cosine
                    similarity = max(0.0, 1.0 - distance)

                similarity = round(similarity, 4)

                formatted_results.append({
                    "chunk_id": ids[idx],
                    "text": docs[idx],
                    "similarity": similarity,
                    "distance": round(distance, 4),
                    "metadata": metas[idx] or {}
                })

            formatted_results.sort(
                key=lambda x: x["similarity"],
                reverse=True
            )

            BaseLogger.info(
                f"Resultados encontrados: {len(formatted_results)}"
            )

            BaseLogger.info(
                f"Busca retornou {len(formatted_results)} resultados."
            )

            return formatted_results

        except Exception as e:
            BaseLogger.error(
                f"Erro durante busca vetorial: {str(e)}"
            )
            return []

    def count_chunks(self) -> int:

        try:
            total = self.collection.count()

            BaseLogger.info(
                f"Total de chunks armazenados: {total}"
            )

            return total

        except Exception as e:
            BaseLogger.error(
                f"Erro ao contar chunks: {str(e)}"
            )
            return 0

    def get_all_chunks(
        self,
        limit: int = 100
    ) -> List[Dict[str, Any]]:

        try:

            data = self.collection.get(
                limit=limit
            )

            ids = data.get("ids", [])
            docs = data.get("documents", [])
            metas = data.get("metadatas", [])

            results = []

            for i in range(len(ids)):
                results.append({
                    "id": ids[i],
                    "text": docs[i],
                    "metadata": metas[i]
                })

            return results

        except Exception as e:
            BaseLogger.error(
                f"Erro ao listar chunks: {str(e)}"
            )
            return []

    def get_collection_stats(self) -> Dict[str, Any]:

        try:

            return {
                "collection_name": self.collection_name,
                "total_chunks": self.collection.count(),
                "storage_path": settings.CHROMADB_PATH
            }

        except Exception as e:
            BaseLogger.error(
                f"Erro ao obter estatísticas: {str(e)}"
            )

            return {}

    def delete_document_chunks(
        self,
        document_id: str
    ) -> bool:

        try:

            BaseLogger.info(
                f"Removendo chunks do documento {document_id}"
            )

            self.collection.delete(
                where={
                    "source_doc_id": document_id
                }
            )

            BaseLogger.info(
                "Chunks removidos com sucesso."
            )

            return True

        except Exception as e:
            BaseLogger.error(
                f"Erro ao remover chunks: {str(e)}"
            )
            return False

    def clear_collection(self) -> bool:

        try:

            BaseLogger.warning(
                f"Limpando coleção '{self.collection_name}'"
            )

            self.client.delete_collection(
                self.collection_name
            )

            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={
                    "hnsw:space": "cosine"
                }
            )

            return True

        except Exception as e:
            BaseLogger.error(
                f"Erro ao limpar coleção: {str(e)}"
            )
            return False


vector_store = VectorStoreService()