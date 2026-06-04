import os
import re
import uuid
from datetime import datetime
from typing import Tuple
from fastapi import HTTPException, status
from pypdf import PdfReader

from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.document import DocumentMetadata


class DocumentProcessorService:
    """
    Serviço corporativo responsável pela ingestão, salvamento, extração de texto,
    limpeza básica e geração de metadados para arquivos PDF e Markdown.
    """

    def __init__(self) -> None:
        # Garantir que o diretório de uploads exista localmente
        os.makedirs(settings.UPLOAD_DIR, exist_ok=True)
        BaseLogger.debug(f"Diretório de uploads configurado em: {settings.UPLOAD_DIR}")

    def clean_text(self, text: str) -> str:
        """
        Executa a limpeza básica de texto extraído:
        - Normaliza quebras de linha excessivas
        - Remove espaços em branco duplicados e laterais
        - Mantém estrutura legível para geração de embeddings
        """
        if not text:
            return ""
        
        # Substitui múltiplos espaços por um único espaço
        text = re.sub(r"[ \t]+", " ", text)
        
        # Substitui mais de duas quebras de linha por apenas duas
        text = re.sub(r"\n\s*\n+", "\n\n", text)
        
        return text.strip()

    def extract_text_from_pdf(self, file_path: str) -> Tuple[str, int]:
        """
        Extrai o texto contido em um arquivo PDF utilizando a biblioteca pypdf.
        Retorna uma tupla contendo (texto_extraido, total_de_paginas).
        """
        BaseLogger.info(f"Iniciando extração de PDF do arquivo: {file_path}")
        text_content = []
        page_count = 0

        try:
            reader = PdfReader(file_path)
            page_count = len(reader.pages)
            
            if page_count == 0:
                raise ValueError("O arquivo PDF está vazio ou não possui páginas legíveis.")

            for i, page in enumerate(reader.pages):
                page_text = page.extract_text()
                if page_text:
                    text_content.append(page_text)
                else:
                    BaseLogger.warning(f"Página {i+1} do PDF '{file_path}' não retornou texto.")

            full_text = "\n".join(text_content)
            return full_text, page_count

        except Exception as e:
            BaseLogger.error(f"Falha na extração de texto do PDF {file_path}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Não foi possível ler o arquivo PDF: {str(e)}"
            )

    def extract_text_from_markdown(self, file_path: str) -> str:
        """
        Lê e retorna o conteúdo puro de um arquivo Markdown (.md).
        """
        BaseLogger.info(f"Iniciando leitura de arquivo Markdown: {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            BaseLogger.error(f"Falha na leitura do arquivo Markdown {file_path}: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Não foi possível ler o arquivo Markdown: {str(e)}"
            )

    def process_document(
        self, file_content: bytes, filename: str, content_type: str
    ) -> Tuple[str, str, DocumentMetadata]:
        """
        Valida, salva o arquivo localmente, executa a extração do texto
        e constrói os metadados finais.
        Retorna uma tupla: (document_id, texto_limpo, metadados)
        """
        # 1. Validação de extensão
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in [".pdf", ".md"]:
            BaseLogger.warning(f"Tentativa de upload de extensão não suportada: {filename}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tipo de arquivo inválido. Apenas arquivos PDF (.pdf) e Markdown (.md) são suportados."
            )

        # 2. Validação do tamanho de arquivo
        file_size_bytes = len(file_content)
        max_size_bytes = settings.MAX_FILE_SIZE_MB * 1024 * 1024
        if file_size_bytes > max_size_bytes:
            BaseLogger.warning(f"Arquivo excede tamanho máximo de {settings.MAX_FILE_SIZE_MB}MB: {filename}")
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail=f"Arquivo muito grande. O limite máximo permitido é de {settings.MAX_FILE_SIZE_MB} MB."
            )

        # 3. Gerar ID Único do documento e salvar localmente para trilha de auditoria/processamento
        doc_id = str(uuid.uuid4())
        safe_filename = f"{doc_id}_{filename}"
        file_path = os.path.join(settings.UPLOAD_DIR, safe_filename)

        BaseLogger.info(f"Salvando cópia do arquivo temporário em: {file_path}")
        try:
            with open(file_path, "wb") as f:
                f.write(file_content)
        except Exception as e:
            BaseLogger.error(f"Erro ao salvar arquivo em disco: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Erro interno no servidor ao persistir o arquivo enviado."
            )

        # 4. Extração de texto dependendo do tipo do arquivo
        raw_text = ""
        page_count = None

        if file_ext == ".pdf":
            raw_text, page_count = self.extract_text_from_pdf(file_path)
        elif file_ext == ".md":
            raw_text = self.extract_text_from_markdown(file_path)

        # 5. Limpeza de texto
        cleaned_text = self.clean_text(raw_text)
        
        # Validar se o texto extraído não ficou completamente vazio
        if not cleaned_text or len(cleaned_text.strip()) == 0:
            # Remover arquivo temporário inválido antes de falhar
            if os.path.exists(file_path):
                os.remove(file_path)
            BaseLogger.warning(f"Nenhum texto pôde ser extraído do documento: {filename}")
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Extração falhou: nenhuma string de texto ou conteúdo legível foi encontrado no arquivo."
            )

        # 6. Criação de Metadados
        metadata = DocumentMetadata(
            filename=filename,
            file_size_bytes=file_size_bytes,
            content_type=content_type,
            page_count=page_count,
            char_count=len(cleaned_text),
            uploaded_at=datetime.utcnow(),
        )

        BaseLogger.info(f"Documento {filename} processado com sucesso. ID: {doc_id}")
        return doc_id, cleaned_text, metadata


# Instanciação Singleton do serviço de processamento
document_processor = DocumentProcessorService()
