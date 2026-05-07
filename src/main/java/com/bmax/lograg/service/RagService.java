package com.bmax.lograg.service;

import com.bmax.lograg.model.AppLog;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Pipeline RAG hybride (Retrieval-Augmented Generation) :
 *
 *  ┌─────────────────────────────────────────────────────────────────────┐
 *  │  Question utilisateur                                               │
 *  │          │                                                          │
 *  │    ┌─────┴──────┐                                                  │
 *  │    │            │                                                   │
 *  │    ▼            ▼                                                   │
 *  │  ┌──────┐   ┌─────────┐                                            │
 *  │  │  ES  │   │PGVector │   ← Récupération hybride                   │
 *  │  │keyword│  │semantic │                                            │
 *  │  └──┬───┘   └────┬────┘                                            │
 *  │     └─────┬──────┘                                                 │
 *  │           ▼                                                        │
 *  │    ┌─────────────┐                                                 │
 *  │    │   Contexte  │   ← Fusion + formatage                          │
 *  │    └──────┬──────┘                                                 │
 *  │           ▼                                                        │
 *  │    ┌─────────────┐                                                 │
 *  │    │  Ollama LLM │   qwen2.5:7b                                    │
 *  │    └──────┬──────┘                                                 │
 *  │           ▼                                                        │
 *  │       Réponse structurée                                           │
 *  └─────────────────────────────────────────────────────────────────────┘
 */
@Service
@Slf4j
public class RagService {

    // ── Prompt système : expert en analyse de logs Spring Boot ────────────────
    private static final String SYSTEM_PROMPT = """
        Tu es un expert senior en analyse de logs Spring Boot, DevOps et architecture de microservices.
        Tu analyses des logs applicatifs en temps réel pour identifier des problèmes, patterns et anomalies.

        Tes compétences :
        - Analyser les erreurs, exceptions et stack traces Java / Spring Boot
        - Identifier les patterns de performance (requêtes lentes, timeouts, memory leaks)
        - Détecter les problèmes de sécurité (authentifications échouées, tentatives d'intrusion)
        - Corréler des événements entre différents microservices via traceId
        - Détecter les déclenchements de circuit breakers et cascades d'erreurs
        - Proposer des solutions concrètes et du code Spring Boot

        Règles de réponse :
        - Base-toi UNIQUEMENT sur les logs fournis dans le contexte
        - Cite des extraits précis des logs pour étayer ton analyse
        - Priorise les problèmes par criticité (ERROR > WARN > INFO)
        - Sois concis mais exhaustif
        - Réponds en français

        Structure OBLIGATOIRE de ta réponse :
        📊 **Résumé** — synthèse en 1-2 lignes
        🔍 **Analyse détaillée** — avec citations de logs spécifiques
        ⚠️  **Problèmes détectés** — liste priorisée par sévérité
        💡 **Recommandations** — actions correctives avec exemples Spring Boot si pertinent
        """;

    // ── Template de prompt utilisateur ────────────────────────────────────────
    private static final String USER_PROMPT = """
        === LOGS RÉCUPÉRÉS (Elasticsearch keywords + PGVector sémantique) ===

        {context}

        === FIN DES LOGS ===

        Question : {question}

        Analyse les logs ci-dessus et réponds à la question en suivant la structure demandée.
        """;

    private final ChatClient             chatClient;
    private final VectorStore            vectorStore;
    private final ElasticsearchOperations esOperations;

    public RagService(ChatModel chatModel, VectorStore vectorStore, ElasticsearchOperations esOperations) {
        this.chatClient   = ChatClient.builder(chatModel)
                                      .defaultSystem(SYSTEM_PROMPT)
                                      .build();
        this.vectorStore  = vectorStore;
        this.esOperations = esOperations;
    }

    /**
     * Point d'entrée du pipeline RAG.
     *
     * @param question  Question de l'utilisateur en langage naturel
     * @return          Réponse structurée de l'LLM, contextualisée par les logs
     */
    public RagResponse chat(String question) {
        long start = System.currentTimeMillis();

        // ── Étape 1 : Récupération Elasticsearch (mots-clés + niveau) ─────────
        List<AppLog> esLogs = searchElasticsearch(question);
        log.debug("ES retrieved {} logs for: '{}'", esLogs.size(), question);

        // ── Étape 2 : Récupération PGVector (similarité sémantique cosinus) ──
        List<Document> vecDocs = searchVectorStore(question);
        log.debug("PGVector retrieved {} documents", vecDocs.size());

        // ── Étape 3 : Fusion et construction du contexte ──────────────────────
        String context = buildContext(esLogs, vecDocs);

        // ── Étape 4 : Génération LLM (Ollama qwen2.5:7b) ─────────────────────
        String answer = chatClient.prompt()
            .user(u -> u.text(USER_PROMPT)
                        .param("context", context)
                        .param("question", question))
            .call()
            .content();

        long duration = System.currentTimeMillis() - start;
        log.info("RAG pipeline: {}ms — ES:{} logs, Vec:{} docs", duration, esLogs.size(), vecDocs.size());

        return new RagResponse(answer, esLogs.size(), vecDocs.size(), duration);
    }

    // ── Mapping français → mots-clés anglais présents dans les logs ─────────

    private static final java.util.Map<String, String[]> FR_TO_EN = java.util.Map.of(
        "erreur",    new String[]{"error", "exception", "failed"},
        "lent",      new String[]{"slow", "timeout", "ms"},
        "lente",     new String[]{"slow", "timeout", "ms"},
        "timeout",   new String[]{"timeout", "timed out"},
        "circuit",   new String[]{"circuit", "breaker"},
        "mémoire",   new String[]{"memory", "OutOfMemory", "heap"},
        "sécurité",  new String[]{"auth", "unauthorized", "forbidden"},
        "paiement",  new String[]{"payment", "PaymentService"},
        "base",      new String[]{"database", "SQL", "jdbc"},
        "cache",     new String[]{"cache", "evict", "hit"}
    );

    private String translateQuery(String query) {
        String lower = query.toLowerCase();
        for (var entry : FR_TO_EN.entrySet()) {
            if (lower.contains(entry.getKey())) {
                return String.join(" ", entry.getValue());
            }
        }
        return query;
    }

    // ── Recherche Elasticsearch ───────────────────────────────────────────────

    private List<AppLog> searchElasticsearch(String query) {
        boolean focusErrors = matchesPattern(query, "error", "erreur", "exception", "fail", "crash",
                                             "failed", "failure", "timeout", "circuit");
        boolean focusWarns  = matchesPattern(query, "warn", "slow", "lent", "retry", "pool");

        // Traduit les termes français en anglais pour matcher les logs
        String esQuery = translateQuery(query);

        try {
            NativeQuery nativeQuery = NativeQuery.builder()
                .withQuery(q -> {
                    if (focusErrors) {
                        return q.bool(b -> b
                            .must(m -> m.multiMatch(mm -> mm
                                .query(esQuery)
                                .fields("message^2", "exception^1.5", "service")))
                            .filter(f -> f.term(t -> t.field("level").value("ERROR"))));
                    } else if (focusWarns) {
                        return q.bool(b -> b
                            .must(m -> m.multiMatch(mm -> mm
                                .query(esQuery)
                                .fields("message^2", "service")))
                            .filter(f -> f.bool(fb -> fb
                                .should(s -> s.term(t -> t.field("level").value("WARN")))
                                .should(s -> s.term(t -> t.field("level").value("ERROR"))))));
                    } else {
                        return q.multiMatch(mm -> mm
                            .query(esQuery)
                            .fields("message^2", "exception", "service")
                            .fuzziness("AUTO"));
                    }
                })
                .withMaxResults(5)
                .build();

            return esOperations.search(nativeQuery, AppLog.class)
                .stream()
                .map(SearchHit::getContent)
                .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Elasticsearch search failed: {}", e.getMessage());
            return List.of();
        }
    }

    // ── Recherche PGVector ────────────────────────────────────────────────────

    private List<Document> searchVectorStore(String query) {
        try {
            return vectorStore.similaritySearch(
                SearchRequest.builder()
                    .query(query)
                    .topK(3)
                    .similarityThreshold(0.55)
                    .build()
            );
        } catch (Exception e) {
            log.error("PGVector search failed: {}", e.getMessage());
            return List.of();
        }
    }

    // ── Construction du contexte RAG ─────────────────────────────────────────

    private String buildContext(List<AppLog> esLogs, List<Document> vecDocs) {
        if (esLogs.isEmpty() && vecDocs.isEmpty()) {
            return "Aucun log pertinent trouvé. Les données sont peut-être encore en cours d'ingestion.";
        }

        StringBuilder ctx = new StringBuilder();

        if (!esLogs.isEmpty()) {
            ctx.append("─── Source : Elasticsearch (recherche par mots-clés) ───\n");
            esLogs.forEach(l -> ctx.append(l.toLogString()).append("\n"));
        }

        if (!vecDocs.isEmpty()) {
            ctx.append("\n─── Source : PGVector (recherche sémantique) ───\n");
            vecDocs.forEach(d -> ctx.append(d.getText()).append("\n"));
        }

        return ctx.toString();
    }

    // ── Utilitaires ───────────────────────────────────────────────────────────

    private boolean matchesPattern(String query, String... keywords) {
        String lower = query.toLowerCase();
        for (String kw : keywords) {
            if (lower.contains(kw)) return true;
        }
        return false;
    }

    /**
     * Version streaming : envoie les tokens au fur et à mesure (SSE).
     * Contourne les timeouts des proxies/cloud agents.
     */
    public Flux<String> stream(String question) {
        List<AppLog>   esLogs  = searchElasticsearch(question);
        List<Document> vecDocs = searchVectorStore(question);
        String         context = buildContext(esLogs, vecDocs);

        return chatClient.prompt()
            .user(u -> u.text(USER_PROMPT)
                        .param("context", context)
                        .param("question", question))
            .stream()
            .content();
    }

    // ── DTO interne ───────────────────────────────────────────────────────────

    public record RagResponse(
        String answer,
        int    esLogsCount,
        int    vectorDocsCount,
        long   processingTimeMs
    ) {}
}
