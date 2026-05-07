package com.bmax.lograg.service;

import com.bmax.lograg.model.AppLog;
import com.bmax.lograg.repository.AppLogRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrateur de l'ingestion des logs vers deux stores :
 *
 *  1. Elasticsearch  — indexation full-text pour recherche par mots-clés,
 *                      filtrage par niveau, service, plage temporelle.
 *
 *  2. PGVector       — embedding sémantique (nomic-embed-text 768 dims)
 *                      pour la recherche par similarité cosinus.
 *
 * Au démarrage : 1 000 logs générés en bulk.
 * En continu    : 1 log par seconde (simule un flux applicatif temps réel).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LogIngestionService {

    private static final int INITIAL_LOG_COUNT   = 1_000;
    private static final int ES_BATCH_SIZE       = 100;
    private static final int VECTOR_BATCH_SIZE   = 50;   // Limité par temps d'embedding Ollama

    private final LogGeneratorService logGenerator;
    private final AppLogRepository    logRepository;
    private final VectorStore         vectorStore;

    private final AtomicLong totalIngested    = new AtomicLong(0);
    private volatile boolean initialComplete  = false;

    /**
     * Ingestion initiale après démarrage complet du contexte Spring.
     * Génère 1 000 logs et les indexe dans Elasticsearch + PGVector (en batchs).
     *
     * Note : l'embedding de 1 000 logs via Ollama prend ~2-5 minutes.
     *        Le scheduling temps réel ne démarre qu'une fois ce chargement terminé.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void ingestInitialLogs() {
        log.info("╔══════════════════════════════════════════════════════════════╗");
        log.info("║  INGESTION INITIALE — {} logs → Elasticsearch + PGVector  ║", INITIAL_LOG_COUNT);
        log.info("║  L'embedding Ollama prend environ 2-5 minutes…              ║");
        log.info("╚══════════════════════════════════════════════════════════════╝");

        long start = System.currentTimeMillis();

        List<AppLog>  esBatch  = new ArrayList<>();
        List<Document> vecBatch = new ArrayList<>();

        for (int i = 1; i <= INITIAL_LOG_COUNT; i++) {
            AppLog log = logGenerator.generate();
            esBatch.add(log);
            vecBatch.add(toDocument(log));

            if (esBatch.size() >= ES_BATCH_SIZE) {
                logRepository.saveAll(esBatch);
                esBatch.clear();
                this.log.debug("ES batch sauvegardé — {} / {} logs", i, INITIAL_LOG_COUNT);
            }

            if (vecBatch.size() >= VECTOR_BATCH_SIZE) {
                vectorStore.add(vecBatch);
                vecBatch.clear();
                this.log.info("PGVector batch embeddi — {} / {} logs", i, INITIAL_LOG_COUNT);
            }
        }

        // Flush des restes
        if (!esBatch.isEmpty())  logRepository.saveAll(esBatch);
        if (!vecBatch.isEmpty()) vectorStore.add(vecBatch);

        totalIngested.addAndGet(INITIAL_LOG_COUNT);
        initialComplete = true;

        long elapsed = System.currentTimeMillis() - start;
        log.info("✅ Ingestion initiale terminée : {} logs en {}s", INITIAL_LOG_COUNT, elapsed / 1000);
        log.info("▶  Démarrage du flux temps réel (1 log/seconde)…");
    }

    /**
     * Ingestion temps réel : un nouveau log toutes les secondes vers Elasticsearch uniquement.
     * PGVector n'est pas mis à jour en temps réel pour ne pas bloquer Ollama (embedding CPU).
     */
    @Scheduled(fixedRate = 1_000)
    public void ingestRealTimeLog() {
        if (!initialComplete) return;

        AppLog appLog = logGenerator.generate();
        logRepository.save(appLog);

        long total = totalIngested.incrementAndGet();
        if (total % 100 == 0) {
            log.info("📊 Flux temps réel : {} logs ES indexés", total);
        }
    }

    public long getTotalIngested() {
        return totalIngested.get();
    }

    public boolean isInitialIngestionComplete() {
        return initialComplete;
    }

    // ── Conversion AppLog → Document Spring AI ────────────────────────────────

    private Document toDocument(AppLog appLog) {
        return new Document(
            appLog.toLogString(),
            Map.of(
                "log_id",    appLog.getId(),
                "level",     appLog.getLevel(),
                "service",   appLog.getService(),
                "timestamp", appLog.getTimestamp().toString(),
                "trace_id",  appLog.getTraceId()
            )
        );
    }
}
