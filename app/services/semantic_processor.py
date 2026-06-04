import uuid
from typing import Dict, List, Tuple
from langchain_text_splitters import RecursiveCharacterTextSplitter

from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.document import DocumentMetadata
from app.schemas.semantic import Chunk, SemanticProcessResponse
from app.services.embedding_service import embedding_service


class SemanticProcessorService:
    """
    Serviço corporativo encarregado de segmentar textos longos (chunking)
    utilizando critérios semânticos inteligentes (LangChain) e gerar
    seus respectivos vetores de embedding no banco de forma otimizada.
    """

    def __init__(self) -> None:
        # Configurando o particionador de texto recursivo
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=settings.CHUNK_SIZE,
            chunk_overlap=settings.CHUNK_OVERLAP,
            length_function=len,
            separators=["\n\n", "\n", " ", ""]
        )
        BaseLogger.debug(
            f"Segmentador configurado. Chunk Size: {settings.CHUNK_SIZE}, Overlap: {settings.CHUNK_OVERLAP}"
        )

    def process_text_into_chunks(
        self, document_id: str, text: str, doc_metadata: DocumentMetadata
    ) -> SemanticProcessResponse:
        """
        Segmenta o texto extraído, gera embeddings de lote e anexa metadados contextuais
        aos chunks resultantes.
        """
        BaseLogger.info(f"Segmentando documento {document_id} ({doc_metadata.filename}) em chunks...")
        
        # 1. Segmentar o texto
        text_chunks = self.text_splitter.split_text(text)
        total_chunks = len(text_chunks)
        
        if total_chunks == 0:
            BaseLogger.warning(f"O documento {document_id} resultou em zero chunks.")
            return SemanticProcessResponse(
                document_id=document_id,
                total_chunks=0,
                chunks=[]
            )

        BaseLogger.info(f"Documento segmentado com sucesso. Total de chunks gerados: {total_chunks}")

        # 2. Gerar embeddings em lote para otimização de performance
        BaseLogger.info("Gerando embeddings em lote para os chunks...")
        embeddings = embedding_service.embed_documents(text_chunks)

        # 3. Construir objetos Chunk finais contendo os dados e metadados herdados e novos
        chunks_response: List[Chunk] = []
        
        for idx, (chunk_text, chunk_vector) in enumerate(zip(text_chunks, embeddings)):
            chunk_id = str(uuid.uuid4())
            
            # Montar metadados específicos deste fragmento
            chunk_metadata = {
                "source_doc_id": document_id,
                "filename": doc_metadata.filename,
                "chunk_index": idx,
                "char_count": len(chunk_text),
                "total_chunks": total_chunks,
                "uploaded_at": doc_metadata.uploaded_at.isoformat()
            }
            
            chunk_obj = Chunk(
                id=chunk_id,
                text=chunk_text,
                embedding=chunk_vector,
                metadata=chunk_metadata
            )
            chunks_response.append(chunk_obj)

        BaseLogger.info(f"Processamento semântico do documento {document_id} concluído.")
        return SemanticProcessResponse(
            document_id=document_id,
            total_chunks=total_chunks,
            chunks=chunks_response
        )


# Instanciação Singleton do processador semântico
semantic_processor = SemanticProcessorService()
