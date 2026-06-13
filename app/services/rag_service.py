import time
from typing import List, Optional

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
        "- Utilize todos os fragmentos fornecidos para compor sua resposta.\n"
        "- Consolide informações de múltiplos fragmentos de maneira lógica, estruturada e concisa.\n"
        "- Quando a pergunta envolver:\n"
        "  * experiência profissional\n"
        "  * currículo\n"
        "  * histórico\n"
        "  * habilidades\n"
        "  * formação\n"
        "  * projetos\n"
        "  * documentos corporativos\n"
        "  combine todas as informações relevantes encontradas em todos os fragmentos.\n"
        "- Não responda usando apenas o primeiro fragmento. Se houver dados complementares em outros trechos, inclua-os na resposta final.\n"
        "- Use apenas informações presentes nos fragmentos de contexto.\n"
        "- Se os fragmentos não contiverem informação suficiente, diga claramente que não encontrou "
        "informação relevante nos documentos disponíveis.\n"
        "- Seja objetivo, claro e profissional.\n"
        "- Não invente fatos ou informações que não estejam nos fragmentos.\n\n"
        "FRAGMENTOS DE CONTEXTO:\n{context}"
    )),
    ("human", "{question}")
]) if LANGCHAIN_LLM_AVAILABLE else None
import re
import unicodedata


def strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")


def is_cv_query(question: str) -> bool:
    normalized = strip_accents(question.lower())
    keywords = [
        "experiencia", "trabalhou", "empresa", "empresas", "emprego",
        "historico profissional", "carreira", "curriculo", "trabalho", "cargo", "cargos"
    ]
    return any(kw in normalized for kw in keywords)


class RAGService:
    """
    Serviço central do pipeline RAG (Retrieval-Augmented Generation).
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
                max_output_tokens=settings.LLM_MAX_TOKENS,
            )
            self._llm_chain = _RAG_PROMPT_TEMPLATE | llm | StrOutputParser()
            BaseLogger.info(f"Pipeline RAG inicializado com modelo: {settings.GEMINI_MODEL} (max_output_tokens={settings.LLM_MAX_TOKENS})")
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

    async def answer(self, request: RAGRequest, request_id: Optional[str] = None) -> RAGResponse:
        """
        Ponto de entrada principal do pipeline RAG com suporte a cache Redis,
        deduplicação, re-ranking híbrido e logs detalhados de observabilidade.
        """
        start_time = time.monotonic()
        
        # 1. Resolver se é uma pergunta de currículo/experiência para boost e limite dinâmico
        is_cv = is_cv_query(request.question)
        if is_cv:
            limit = 12
        else:
            limit = request.limit or settings.RAG_CONTEXT_CHUNKS
        
        BaseLogger.info(
            f"Pipeline RAG iniciado para a pergunta: '{request.question[:80]}' | "
            f"doc_id={request.filter_document_id} | limit={limit} | is_cv={is_cv}"
        )

        # ── 2. Geração da chave de cache com pergunta normalizada e filtro de doc ──
        cache_key = CacheService.build_cache_key(request.question, limit, request.filter_document_id)
        BaseLogger.info(f"[REDIS] Verificando cache para a pergunta normalizada. Chave: {cache_key[:16]}...")

        # ── 3. Consulta ao Redis ──────────────────────────────────────────────
        cached_data = await cache_service.get(cache_key)

        if cached_data is not None:
            elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
            BaseLogger.info(f"[REDIS] Cache HIT → Resposta encontrada no cache Redis.")
            
            # Log de observabilidade completo para Cache Hit
            BaseLogger.info(
                f"[OBSERVABILIDADE] Resumo RAG Pipeline:\n"
                f"- Question: {request.question}\n"
                f"- Limit: {limit}\n"
                f"- Document ID: {request.filter_document_id}\n"
                f"- Chunks Recuperados (ChromaDB): 0 (Cache Hit)\n"
                f"- Chunks Filtrados (Threshold): 0\n"
                f"- Threshold Final: 0.0\n"
                f"- Scores: []\n"
                f"- Tempo Execucao (Total): {elapsed_ms:.2f}ms\n"
                f"- Cache: HIT"
            )
            return self._response_from_dict(cached_data, elapsed_ms)

        BaseLogger.info("[REDIS] Cache MISS → Pergunta não encontrada no cache. Executando pipeline completo.")

        # ── 4. Geração do embedding da pergunta e recuperação ────────────────
        candidate_limit = max(limit * 5, 50) if is_cv else max(limit * 5, 25)
        BaseLogger.info(f"[RETRIEVAL] Iniciando busca vetorial por embeddings. Limite de candidatos (Recall): {candidate_limit}")
        t_vector_start = time.monotonic()
        
        embed_query_text = request.question
        if is_cv:
            embed_query_text = f"{request.question} experiência profissional empresas trabalhou cargo emprego histórico profissional carreira currículo"
            
        query_vector = embedding_service.embed_query(embed_query_text)

        # Montar where filter se filter_document_id for fornecido
        where_filter = None
        if request.filter_document_id:
            where_filter = {"source_doc_id": request.filter_document_id}

        # ── 5. Recuperação de contexto do ChromaDB ───────────────────────────
        raw_results = vector_store.similarity_search(
            query_vector=query_vector,
            limit=candidate_limit,
            where_filter=where_filter
        )
        vector_search_ms = round((time.monotonic() - t_vector_start) * 1000, 2)
        BaseLogger.info(f"[RETRIEVAL] Busca vetorial concluída em {vector_search_ms}ms. Obtidos {len(raw_results)} candidatos.")

        # ── 5. Log de Chunks recuperados do ChromaDB ───────────────────────────
        BaseLogger.info("===== CHUNKS RECUPERADOS DO CHROMADB =====")
        for idx, r in enumerate(raw_results, 1):
            normalized_text = r['text'].strip().replace('\n', ' ')[:80]
            BaseLogger.info(
                f"[{idx}] Chunk ID: {r['chunk_id']} | "
                f"Cosine Similarity: {r['similarity']} | "
                f"Arquivo: {r.get('metadata', {}).get('filename', 'desconhecido')} | "
                f"Texto inicial: {normalized_text}..."
            )

        # ── 6. Remoção de Chunks Duplicados ──────────────────────────────────
        seen_texts = set()
        deduplicated = []
        for r in raw_results:
            norm_text = r["text"].strip().lower()
            if norm_text not in seen_texts:
                seen_texts.add(norm_text)
                deduplicated.append(r)
        
        BaseLogger.info(f"[RETRIEVAL] Remoção de duplicados concluída: {len(raw_results)} -> {len(deduplicated)} chunks únicos.")

        # ── 7. Re-ranking Híbrido (Cosine Similarity + Token Overlap) ────────
        STOP_WORDS = {
            # Português
            "o", "a", "os", "as", "um", "uma", "uns", "umas", "de", "do", "da", "dos", "das",
            "em", "no", "na", "nos", "nas", "para", "por", "com", "sem", "sob", "sobre", "atras",
            "que", "se", "como", "esta", "este", "isto", "aquilo", "ou", "e", "mas", "porem",
            "um", "ao", "aos", "pelos", "pelas", "num", "numa", "ele", "ela", "eles", "elas",
            # English
            "the", "a", "an", "and", "or", "but", "if", "then", "else", "of", "at", "by", "for",
            "with", "about", "against", "between", "into", "through", "during", "before", "after",
            "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under",
            "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",
            "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no",
            "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can",
            "will", "just", "don", "should", "now"
        }

        def _get_tokens(text: str) -> set:
            normalized = strip_accents(text.lower())
            normalized = re.sub(r"[^\w\s]", " ", normalized)
            words = normalized.split()
            return {w for w in words if len(w) > 1 and w not in STOP_WORDS}

        query_tokens = _get_tokens(request.question)
        
        re_ranked = []
        for item in deduplicated:
            similarity = item.get("similarity", 0.0)
            chunk_tokens = _get_tokens(item["text"])
            
            # Token Overlap Ratio (Proporção de interseção de termos)
            if query_tokens:
                overlap_ratio = len(query_tokens.intersection(chunk_tokens)) / len(query_tokens)
            else:
                overlap_ratio = 0.0
                
            # Score híbrido básico
            hybrid_score = max(similarity, (0.7 * similarity) + (0.3 * overlap_ratio))
            
            # Boost por palavras-chave para perguntas sobre currículo/carreira
            keyword_boost = 0.0
            if is_cv:
                boost_keywords = ["experiência", "trabalhou", "empresa", "empresas", "emprego", "histórico profissional", "carreira", "currículo", "trabalho", "cargo"]
                chunk_text_lower = strip_accents(item["text"].lower())
                matches = sum(1 for kw in boost_keywords if strip_accents(kw) in chunk_text_lower)
                if matches > 0:
                    keyword_boost = min(0.3, matches * 0.1)
            
            hybrid_score = min(1.0, hybrid_score + keyword_boost)
            
            item_copy = dict(item)
            item_copy["hybrid_score"] = hybrid_score
            item_copy["overlap_ratio"] = overlap_ratio
            item_copy["keyword_boost"] = keyword_boost
            re_ranked.append(item_copy)
            
        # Log detalhado ANTES do re-ranking (ordem original do ChromaDB)
        BaseLogger.info("===== CHUNKS ANTES DO RE-RANKING =====")
        for item in re_ranked:
            doc_id = item.get("metadata", {}).get("source_doc_id", "desconhecido")
            BaseLogger.info(
                f"chunk_id={item['chunk_id']} | "
                f"document_id={doc_id} | "
                f"similarity={item.get('similarity', 0.0)} | "
                f"overlap={item.get('overlap_ratio', 0.0)} | "
                f"hybrid_score={item.get('hybrid_score', 0.0)}"
            )

        # Ordenar por score híbrido descrescente (re-ranking)
        re_ranked.sort(key=lambda x: x["hybrid_score"], reverse=True)
        BaseLogger.info("[RETRIEVAL] Re-ranking concluído por score híbrido (70% vetorial + 30% overlap).")

        # Log detalhado DEPOIS do re-ranking (ordem classificada por score híbrido)
        BaseLogger.info("===== CHUNKS APÓS O RE-RANKING =====")
        for item in re_ranked:
            doc_id = item.get("metadata", {}).get("source_doc_id", "desconhecido")
            BaseLogger.info(
                f"chunk_id={item['chunk_id']} | "
                f"document_id={doc_id} | "
                f"similarity={item.get('similarity', 0.0)} | "
                f"overlap={item.get('overlap_ratio', 0.0)} | "
                f"hybrid_score={item.get('hybrid_score', 0.0)}"
            )

        # ── 8. Filtrar por similaridade mínima com limiar dinâmico ────────────
        max_hybrid_score = max([r.get("hybrid_score", 0.0) for r in re_ranked]) if re_ranked else 0.0
        
        # Limiar adaptativo dinâmico
        if is_cv:
            dynamic_threshold = max_hybrid_score * 0.50
        elif max_hybrid_score < settings.RAG_MIN_SIMILARITY:
            dynamic_threshold = max_hybrid_score * 0.70
        else:
            dynamic_threshold = max(
                max_hybrid_score * 0.70,
                settings.RAG_MIN_SIMILARITY
            )
        
        BaseLogger.info(f"Threshold calculado: {dynamic_threshold:.4f} (is_cv={is_cv})")

        filtered = []
        rejected = []
        for r in re_ranked:
            score = r.get("hybrid_score", 0.0)
            if score >= dynamic_threshold and score > 0.0:
                filtered.append(r)
            else:
                rejected.append(r)

        # Garantir que pelo menos os TOP 10 chunks sejam enviados ao LLM para perguntas sobre currículo/carreira
        if is_cv and len(filtered) < 10 and re_ranked:
            min_chunks = min(10, len(re_ranked))
            BaseLogger.info(f"Garantindo que pelo menos os TOP {min_chunks} chunks sejam enviados (atual: {len(filtered)})")
            filtered = re_ranked[:min_chunks]
            rejected = re_ranked[min_chunks:]

        # Fallback Top-K se nenhum passou no threshold mas temos chunks no re_ranked
        if not filtered and re_ranked:
            BaseLogger.warning("Nenhum chunk passou no threshold. Aplicando fallback Top-K.")
            filtered = re_ranked[:3]
            rejected = re_ranked[3:]
            for r in filtered:
                BaseLogger.info(f"Chunk aprovado via fallback Top-K: {r['chunk_id']}")

        # Log dos chunks descartados
        BaseLogger.info("===== CHUNKS DESCARTADOS =====")
        if not rejected:
            BaseLogger.info("Nenhum chunk descartado.")
        else:
            for idx, r in enumerate(rejected, 1):
                texto_inicial = r['text'].strip().replace('\n', ' ')[:80]
                BaseLogger.info(
                    f"[{idx}] Chunk ID: {r['chunk_id']} | "
                    f"Score Hibrido: {r.get('hybrid_score', 0.0):.4f} | "
                    f"Arquivo: {r.get('metadata', {}).get('filename', 'desconhecido')} | "
                    f"Texto inicial: {texto_inicial}..."
                )

        BaseLogger.info(
            f"[RETRIEVAL] Score híbrido máximo encontrado: {max_hybrid_score:.4f} | "
            f"Threshold dinâmico de corte: {dynamic_threshold:.4f} (baseline: {settings.RAG_MIN_SIMILARITY:.2f}) | "
            f"Passaram pelo filtro: {len(filtered)}/{len(re_ranked)} chunks."
        )

        context_found = len(filtered) > 0

        # ── 9. Fallback: sem contexto relevante ──────────────────────────────
        if not context_found:
            BaseLogger.warning("[RETRIEVAL] Nenhum contexto relevante passou no limiar de similaridade.")
            elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
            
            # Log de observabilidade completo para Cache Miss + Sem contexto
            BaseLogger.info(
                f"[OBSERVABILIDADE] Resumo RAG Pipeline:\n"
                f"- Question: {request.question}\n"
                f"- Limit: {limit}\n"
                f"- Document ID: {request.filter_document_id}\n"
                f"- Chunks Recuperados (ChromaDB): {len(raw_results)}\n"
                f"- Chunks Filtrados (Threshold): 0\n"
                f"- Threshold Final: {dynamic_threshold:.4f}\n"
                f"- Scores: {[round(r.get('hybrid_score', 0.0), 4) for r in re_ranked]}\n"
                f"- Tempo Execucao (Total): {elapsed_ms:.2f}ms\n"
                f"- Cache: MISS"
            )
            fallback_response = RAGResponse(
                question=request.question,
                answer=(
                    "Não encontrei informações suficientes nos documentos para responder essa pergunta."
                ),
                sources=[],
                context_found=False,
                llm_used=False,
                cache_hit=False,
                latency_ms=elapsed_ms
            )
            return fallback_response

        # Retém apenas os melhores chunks de acordo com o limite resolvido
        final_chunks = filtered[:limit]
        
        # ── 10. Controle de Tamanho de Contexto (Evitar estouro de janela) ────
        MAX_CONTEXT_CHARS = 25000
        safe_chunks = []
        current_chars = 0
        for chunk in final_chunks:
            chunk_len = len(chunk["text"])
            if current_chars + chunk_len > MAX_CONTEXT_CHARS:
                BaseLogger.warning(
                    f"[CONTEXT] Limite de caracteres atingido ({current_chars}/{MAX_CONTEXT_CHARS}). "
                    f"Ignorando chunk {chunk['chunk_id']} para evitar estouro de contexto."
                )
                break
            safe_chunks.append(chunk)
            current_chars += chunk_len

        BaseLogger.info(
            f"[CONTEXT] Controle de tamanho: selecionados {len(safe_chunks)}/{len(final_chunks)} chunks "
            f"com total de {current_chars} caracteres."
        )

        # Log dos chunks enviados ao Gemini
        BaseLogger.info("===== CHUNKS ENVIADOS AO GEMINI =====")
        for idx, r in enumerate(safe_chunks, 1):
            texto_inicial = r['text'].strip().replace('\n', ' ')[:80]
            BaseLogger.info(
                f"[{idx}] Chunk ID: {r['chunk_id']} | "
                f"Score Hibrido: {r.get('hybrid_score', 0.0):.4f} | "
                f"Arquivo: {r.get('metadata', {}).get('filename', 'desconhecido')} | "
                f"Texto inicial: {texto_inicial}..."
            )

        # Construir referências de fontes (com tamanho de trecho configurável)
        sources: List[SourceReference] = [
            SourceReference(
                chunk_id=item["chunk_id"],
                filename=item.get("metadata", {}).get("filename", "desconhecido"),
                excerpt=item["text"][:settings.EXCERPT_LENGTH] + "..." if len(item["text"]) > settings.EXCERPT_LENGTH else item["text"],
                similarity=round(item["hybrid_score"], 4)
            )
            for item in safe_chunks
        ]

        # ── 11. Geração da resposta via LLM ou fallback ───────────────────────
        context_str = self._build_context_string(safe_chunks)
        llm_used = False
        answer_text = ""
        llm_ms = 0.0

        # Preparar pergunta com instrução especial para currículo se for o caso
        question_to_send = request.question
        if is_cv:
            question_to_send += (
                "\n\nInstrução adicional: Liste TODAS as experiências profissionais encontradas no contexto. "
                "Não resuma para apenas uma. Extraia cada empresa, cargo e período identificado no currículo. "
                "Caso existam múltiplas experiências, apresente todas em formato de lista."
            )

        # Log detalhado do prompt logo antes do envio
        total_chars = len(context_str) + len(question_to_send)
        estimated_tokens = total_chars // 4
        BaseLogger.info(
            f"[LLM_CALL] Preparando chamada ao Gemini:\n"
            f"- Chunks enviados: {[c['chunk_id'] for c in safe_chunks]}\n"
            f"- Quantidade total de caracteres: {total_chars}\n"
            f"- Quantidade total de tokens estimados: {estimated_tokens}\n"
            f"- Question original: {request.question}\n"
            f"- Question enviada: {question_to_send}"
        )

        if self._llm_chain:
            try:
                BaseLogger.info("Chamando LLM (Google Gemini) para geração de resposta...")
                t_llm_start = time.monotonic()
                answer_text = await self._llm_chain.ainvoke({
                    "context": context_str,
                    "question": question_to_send
                })
                llm_ms = round((time.monotonic() - t_llm_start) * 1000, 2)
                llm_used = True
                BaseLogger.info("Resposta gerada com sucesso pelo LLM.")
            except Exception as e:
                BaseLogger.error(f"Erro na chamada ao LLM: {str(e)}. Ativando fallback de contexto.")
                answer_text = self._build_fallback_answer(safe_chunks)
        else:
            answer_text = self._build_fallback_answer(safe_chunks)

        # ── 12. Montagem da resposta final ─────────────────────────────────────
        elapsed_ms = round((time.monotonic() - start_time) * 1000, 2)
        BaseLogger.info(f"Pipeline RAG concluído em {elapsed_ms}ms. LLM usado: {llm_used}")

        # Log de observabilidade completo para Cache Miss + Sucesso
        BaseLogger.info(
            f"[OBSERVABILIDADE] Resumo RAG Pipeline:\n"
            f"- Question: {request.question}\n"
            f"- Limit: {limit}\n"
            f"- Document ID: {request.filter_document_id}\n"
            f"- Chunks Recuperados (ChromaDB): {len(raw_results)}\n"
            f"- Chunks Filtrados (Threshold): {len(filtered)}\n"
            f"- Threshold Final: {dynamic_threshold:.4f}\n"
            f"- Scores: {[round(r.get('hybrid_score', 0.0), 4) for r in re_ranked]}\n"
            f"- Tempo Execucao (Total): {elapsed_ms:.2f}ms\n"
            f"- Cache: MISS"
        )

        final_response = RAGResponse(
            question=request.question,
            answer=answer_text,
            sources=sources,
            context_found=context_found,
            llm_used=llm_used,
            cache_hit=False,
            latency_ms=elapsed_ms
        )

        # ── 13. Persistência no Redis ─────────────────────────────────────────
        BaseLogger.info(f"[REDIS] Salvando resposta no cache → TTL: {settings.CACHE_TTL_SECONDS}s")
        await cache_service.set(
            key=cache_key,
            value=self._response_to_dict(final_response),
            ttl=settings.CACHE_TTL_SECONDS,
        )

        return final_response


# Instanciação Singleton do serviço RAG
rag_service = RAGService()
