import time
from typing import List

from app.core.config import settings
from app.core.logging import BaseLogger
from app.schemas.rag import RAGRequest, RAGResponse, SourceReference
from app.services.cache_service import cache_service, CacheService
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

    Fluxo com cache Redis:
        1. Gera cache_key determinística via SHA-256 (pergunta + limit).
        2. Consulta o Redis — se houver HIT, retorna imediatamente sem
           acionar ChromaDB nem Gemini.
        3. Em caso de MISS, executa o pipeline RAG completo:
            a. Gera embedding da pergunta.
            b. Recupera os chunks mais semanticamente similares do ChromaDB.
            c. Filtra chunks por score mínimo de similaridade.
            d. Constrói um prompt estruturado com o contexto recuperado.
            e. Chama o LLM (Google Gemini via LangChain) para gerar a resposta.
            f. Ativa fallback inteligente se: sem contexto, sem API key ou LLM indisponível.
        4. Persiste a resposta final no Redis com TTL configurável.
        5. Retorna a resposta com referências rastreáveis de cada fonte utilizada
           e o campo cache_hit indicando a origem da resposta.
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

    # ── Helpers internos ─────────────────────────────────────────────────────

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

    def _response_to_dict(self, response: RAGResponse) -> dict:
        """
        Serializa um RAGResponse para dicionário JSON-compatível,
        adequado para persistência no Redis.
        """
        return {
            "question": response.question,
            "answer": response.answer,
            "sources": [
                {
                    "chunk_id": s.chunk_id,
                    "filename": s.filename,
                    "excerpt": s.excerpt,
                    "similarity": s.similarity,
                }
                for s in response.sources
            ],
            "context_found": response.context_found,
            "llm_used": response.llm_used,
            "cache_hit": response.cache_hit,
            "latency_ms": response.latency_ms,
        }

    def _response_from_dict(self, data: dict, cache_latency_ms: float) -> RAGResponse:
        """
        Desserializa um dicionário armazenado no Redis de volta a um RAGResponse,
        substituindo a latência original pela latência real da recuperação do cache.
        """
        return RAGResponse(
            question=data["question"],
            answer=data["answer"],
            sources=[
                SourceReference(
                    chunk_id=s["chunk_id"],
                    filename=s["filename"],
                    excerpt=s["excerpt"],
                    similarity=s["similarity"],
                )
                for s in data.get("sources", [])
            ],
            context_found=data["context_found"],
            llm_used=data["llm_used"],
            cache_hit=True,
            latency_ms=cache_latency_ms,
        )

    # ── Pipeline principal ───────────────────────────────────────────────────

    async def answer(self, request: RAGRequest) -> RAGResponse:
        """
        Ponto de entrada principal do pipeline RAG com suporte a cache Redis.
        """
        start_time = time.monotonic()
        BaseLogger.info(f"Pipeline RAG iniciado para a pergunta: '{request.question[:80]}'")

        # ── 1. Geração da chave de cache ─────────────────────────────────────
        cache_key = CacheService.build_cache_key(request.question, request.limit)
        BaseLogger.debug(f"[REDIS] Cache key gerada: {cache_key[:16]}...")

        # ── 2. Consulta ao Redis ──────────────────────────────────────────────
        cached_data = await cache_service.get(cache_key)

        if cached_data is not None:
            elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
            BaseLogger.info(
                f"[REDIS] Cache HIT → retornando resposta em {elapsed_ms}ms "
                f"(ChromaDB e Gemini não foram consultados)."
            )
            return self._response_from_dict(cached_data, elapsed_ms)

        BaseLogger.info("[REDIS] Cache MISS → executando pipeline RAG completo.")

        # ── 3. Geração do embedding da pergunta ──────────────────────────────
        query_vector = embedding_service.embed_query(request.question)

        # ── 4. Recuperação de contexto do ChromaDB ───────────────────────────
        raw_results = vector_store.similarity_search(
            query_vector=query_vector,
            limit=request.limit
        )

        # ── 5. Filtrar por similaridade mínima ───────────────────────────────
        filtered = [r for r in raw_results if r.get("similarity", 0.0) >= settings.RAG_MIN_SIMILARITY]

        context_found = len(filtered) > 0
        BaseLogger.info(f"{len(filtered)}/{len(raw_results)} chunks passaram pelo filtro de similaridade mínima.")

        # ── 6. Construir referências de fontes ───────────────────────────────
        sources: List[SourceReference] = [
            SourceReference(
                chunk_id=item["chunk_id"],
                filename=item.get("metadata", {}).get("filename", "desconhecido"),
                excerpt=item["text"][:200] + "..." if len(item["text"]) > 200 else item["text"],
                similarity=item["similarity"]
            )
            for item in filtered
        ]

        # ── 7. Fallback: sem contexto relevante ──────────────────────────────
        if not context_found:
            BaseLogger.warning("Nenhum contexto relevante encontrado. Retornando fallback sem contexto.")
            elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
            fallback_response = RAGResponse(
                question=request.question,
                answer=(
                    "Não foram encontrados fragmentos de documentos relevantes para esta pergunta. "
                    "Por favor, certifique-se de que os documentos relacionados foram ingeridos e processados."
                ),
                sources=[],
                context_found=False,
                llm_used=False,
                cache_hit=False,
                latency_ms=elapsed_ms
            )
            # Respostas de fallback (sem contexto) não são cacheadas para evitar
            # persistir resultados inconclusivos que mudarão após ingestão de documentos.
            return fallback_response

        # ── 8. Geração da resposta via LLM ou fallback ───────────────────────
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

        # ── 9. Montagem da resposta final ─────────────────────────────────────
        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        BaseLogger.info(f"Pipeline RAG concluído em {elapsed_ms}ms. LLM usado: {llm_used}")

        final_response = RAGResponse(
            question=request.question,
            answer=answer_text,
            sources=sources,
            context_found=context_found,
            llm_used=llm_used,
            cache_hit=False,
            latency_ms=elapsed_ms
        )

        # ── 10. Persistência no Redis ─────────────────────────────────────────
        BaseLogger.info(
            f"[REDIS] Salvando resposta no cache → TTL: {settings.CACHE_TTL_SECONDS}s"
        )
        await cache_service.set(
            key=cache_key,
            value=self._response_to_dict(final_response),
            ttl=settings.CACHE_TTL_SECONDS,
        )

        return final_response


# Instanciação Singleton do serviço RAG
rag_service = RAGService()
