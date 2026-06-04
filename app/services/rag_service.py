import time
from typing import List, Tuple

from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.rag import RAGRequest, RAGResponse, SourceReference
from app.services.embedding_service import embedding_service
from app.services.vector_store import vector_store

# Importação opcional do LangChain / Google Gemini
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_core.prompts import ChatPromptTemplate
    from langchain_core.output_parsers import StrOutputParser
    LANGCHAIN_LLM_AVAILABLE = True
except ImportError:
    LANGCHAIN_LLM_AVAILABLE = False

# Template do prompt RAG corporativo em Português
_RAG_PROMPT_TEMPLATE = ChatPromptTemplate.from_messages([
    ("system", (
        "Você é um assistente corporativo especializado em análise de documentos técnicos e empresariais. "
        "Responda à pergunta do usuário EXCLUSIVAMENTE com base nos fragmentos de contexto fornecidos abaixo.\n\n"
        "REGRAS OBRIGATÓRIAS:\n"
        "- Use apenas informações presentes nos fragmentos de contexto.\n"
        "- Se os fragmentos não contiverem informação suficiente, diga claramente que não encontrou "
        "informação relevante nos documentos disponíveis.\n"
        "- Seja objetivo, claro e profissional.\n"
        "- Não invente fatos ou informações que não estejam nos fragmentos.\n\n"
        "FRAGMENTOS DE CONTEXTO:\n{context}"
    )),
    ("human", "{question}")
]) if LANGCHAIN_LLM_AVAILABLE else None


class RAGService:
    """
    Serviço central do pipeline RAG (Retrieval-Augmented Generation).

    Fluxo:
        1. Gera embedding da pergunta do usuário.
        2. Recupera os chunks mais semanticamente similares do ChromaDB.
        3. Filtra chunks por score mínimo de similaridade.
        4. Constrói um prompt estruturado com o contexto recuperado.
        5. Chama o LLM (Google Gemini via LangChain) para gerar a resposta.
        6. Ativa fallback inteligente se: sem contexto, sem API key ou LLM indisponível.
        7. Retorna a resposta com referências rastreáveis de cada fonte utilizada.
    """

    def __init__(self) -> None:
        self._llm_chain = None
        self._setup_llm()

    def _setup_llm(self) -> None:
        BaseLogger.info(
    f"GOOGLE_API_KEY carregada: {settings.GOOGLE_API_KEY[:10]}"
)
        """
        Configura a chain LangChain (Prompt → LLM → OutputParser).
        Se a GOOGLE_API_KEY não for válida ou o LangChain não estiver disponível,
        a chain fica como None e o fallback será ativado automaticamente.
        """
        is_mock_key = not settings.GOOGLE_API_KEY or settings.GOOGLE_API_KEY in (
            "mock-key-for-now", "your-google-api-key-here", "sua-google-api-key-aqui", ""
        )

        if not LANGCHAIN_LLM_AVAILABLE:
            BaseLogger.warning("LangChain Google Gemini não disponível. Pipeline RAG usará fallback.")
            return

        if is_mock_key:
            BaseLogger.warning(
                "GOOGLE_API_KEY não configurada com uma chave real. "
                "Pipeline RAG usará fallback de contexto sem LLM."
            )
            return

        try:
            llm = ChatGoogleGenerativeAI(
                model=settings.GEMINI_MODEL,
                google_api_key=settings.GOOGLE_API_KEY,
                temperature=settings.LLM_TEMPERATURE,
            )
            self._llm_chain = _RAG_PROMPT_TEMPLATE | llm | StrOutputParser()
            BaseLogger.info(f"Pipeline RAG inicializado com modelo: {settings.GEMINI_MODEL}")
        except Exception as e:
            BaseLogger.error(f"Falha ao inicializar LLM chain: {str(e)}. Usando fallback.")

    def _build_context_string(self, results: List[dict]) -> str:
        """
        Formata os chunks recuperados em um bloco de contexto estruturado para o prompt.
        """
        lines = []
        for i, item in enumerate(results, start=1):
            filename = item.get("metadata", {}).get("filename", "desconhecido")
            lines.append(f"[Fragmento {i} - Fonte: {filename}]\n{item['text']}")
        return "\n\n---\n\n".join(lines)

    def _build_fallback_answer(self, results: List[dict]) -> str:
        """
        Gera uma resposta de fallback estruturada apresentando o contexto recuperado diretamente,
        sem passar pelo LLM. Usado quando a API key não está disponível.
        """
        if not results:
            return (
                "Não foram encontrados fragmentos de documentos relevantes para esta pergunta. "
                "Por favor, certifique-se de que os documentos relacionados foram ingeridos e processados."
            )
        context = self._build_context_string(results)
        return (
            "⚠️ Resposta gerada sem LLM (modo fallback — configure GOOGLE_API_KEY para respostas geradas por IA).\n\n"
            "Fragmentos de documentos mais relevantes encontrados:\n\n"
            f"{context}"
        )

    async def answer(self, request: RAGRequest) -> RAGResponse:
        """
        Ponto de entrada principal do pipeline RAG.
        """
        start_time = time.monotonic()
        BaseLogger.info(f"Pipeline RAG iniciado para a pergunta: '{request.question[:80]}'")

        # ── 1. Geração do embedding da pergunta ─────────────────────────────
        query_vector = embedding_service.embed_query(request.question)

        # ── 2. Recuperação de contexto do ChromaDB ───────────────────────────
        raw_results = vector_store.similarity_search(
            query_vector=query_vector,
            limit=request.limit
        )

        # ── 3. Filtrar por similaridade mínima ───────────────────────────────
        filtered = [r for r in raw_results if r.get("similarity", 0.0) >= settings.RAG_MIN_SIMILARITY]

        context_found = len(filtered) > 0
        BaseLogger.info(f"{len(filtered)}/{len(raw_results)} chunks passaram pelo filtro de similaridade mínima.")

        # ── 4. Construir referências de fontes ───────────────────────────────
        sources: List[SourceReference] = [
            SourceReference(
                chunk_id=item["chunk_id"],
                filename=item.get("metadata", {}).get("filename", "desconhecido"),
                excerpt=item["text"][:200] + "..." if len(item["text"]) > 200 else item["text"],
                similarity=item["similarity"]
            )
            for item in filtered
        ]

        # ── 5. Fallback: sem contexto relevante ──────────────────────────────
        if not context_found:
            BaseLogger.warning("Nenhum contexto relevante encontrado. Retornando fallback sem contexto.")
            elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
            return RAGResponse(
                question=request.question,
                answer=(
                    "Não foram encontrados fragmentos de documentos relevantes para esta pergunta. "
                    "Por favor, certifique-se de que os documentos relacionados foram ingeridos e processados."
                ),
                sources=[],
                context_found=False,
                llm_used=False,
                latency_ms=elapsed_ms
            )

        # ── 6. Geração da resposta via LLM ou fallback ───────────────────────
        context_str = self._build_context_string(filtered)
        llm_used = False
        answer_text = ""

        if self._llm_chain:
            try:
                BaseLogger.info("Chamando LLM para geração de resposta...")
                answer_text = await self._llm_chain.ainvoke({
                    "context": context_str,
                    "question": request.question
                })
                llm_used = True
                BaseLogger.info("Resposta gerada com sucesso pelo LLM.")
            except Exception as e:
                BaseLogger.error(f"Erro na chamada ao LLM: {str(e)}. Ativando fallback de contexto.")
                answer_text = self._build_fallback_answer(filtered)
        else:
            answer_text = self._build_fallback_answer(filtered)

        # ── 7. Retorno final ──────────────────────────────────────────────────
        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        BaseLogger.info(f"Pipeline RAG concluído em {elapsed_ms}ms. LLM usado: {llm_used}")

        return RAGResponse(
            question=request.question,
            answer=answer_text,
            sources=sources,
            context_found=context_found,
            llm_used=llm_used,
            latency_ms=elapsed_ms
        )


# Instanciação Singleton do serviço RAG
rag_service = RAGService()
