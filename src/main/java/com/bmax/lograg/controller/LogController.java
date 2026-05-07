package com.bmax.lograg.controller;

import com.bmax.lograg.model.AppLog;
import com.bmax.lograg.repository.AppLogRepository;
import com.bmax.lograg.service.LogIngestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * API d'observation des logs indexés en temps réel.
 *
 * GET /api/logs              — logs récents paginés (filtrable par level/service)
 * GET /api/logs/stats        — statistiques globales
 * GET /api/logs/stream       — flux SSE temps réel (Server-Sent Events)
 */
@RestController
@RequestMapping("/api/logs")
@RequiredArgsConstructor
public class LogController {

    private final AppLogRepository   logRepository;
    private final LogIngestionService ingestionService;

    // SSE : liste des clients connectés
    private final List<SseEmitter> sseClients = new CopyOnWriteArrayList<>();

    /**
     * Logs récents filtrables.
     * GET /api/logs?size=20&level=ERROR&service=PaymentService
     */
    @GetMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public List<AppLog> getLogs(
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String level,
            @RequestParam(required = false) String service) {

        int safeSize = Math.min(size, 200);
        PageRequest page = PageRequest.of(0, safeSize, Sort.by("timestamp").descending());

        if (level != null && service != null) {
            return logRepository.findByLevel(level.toUpperCase(), PageRequest.of(0, safeSize))
                .getContent()
                .stream()
                .filter(l -> service.equalsIgnoreCase(l.getService()))
                .toList();
        }
        if (level != null) {
            return logRepository.findByLevel(level.toUpperCase(), page).getContent();
        }
        if (service != null) {
            return logRepository.findByService(service, page).getContent();
        }
        return logRepository.findAllByOrderByTimestampDesc(page).getContent();
    }

    /**
     * Statistiques d'ingestion et de répartition des niveaux.
     * GET /api/logs/stats
     */
    @GetMapping(value = "/stats", produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> getStats() {
        long total   = logRepository.count();
        long errors  = logRepository.countByLevel("ERROR");
        long warns   = logRepository.countByLevel("WARN");
        long infos   = logRepository.countByLevel("INFO");
        long debugs  = logRepository.countByLevel("DEBUG");

        return Map.of(
            "totalIndexedInElasticsearch", total,
            "totalIngested",               ingestionService.getTotalIngested(),
            "initialIngestionComplete",    ingestionService.isInitialIngestionComplete(),
            "byLevel", Map.of(
                "ERROR", errors,
                "WARN",  warns,
                "INFO",  infos,
                "DEBUG", debugs
            ),
            "errorRate", total > 0 ? String.format("%.1f%%", (errors * 100.0 / total)) : "0%"
        );
    }

    /**
     * Flux SSE : reçoit les nouveaux logs en temps réel dans le navigateur.
     * GET /api/logs/stream
     *
     * Exemple JS :
     *   const evtSource = new EventSource('/api/logs/stream');
     *   evtSource.onmessage = e => console.log(JSON.parse(e.data));
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        sseClients.add(emitter);
        emitter.onCompletion(() -> sseClients.remove(emitter));
        emitter.onTimeout(()    -> sseClients.remove(emitter));
        emitter.onError(e       -> sseClients.remove(emitter));
        return emitter;
    }

    /** Appelé par LogIngestionService pour pousser les nouveaux logs aux clients SSE. */
    public void broadcast(AppLog log) {
        List<SseEmitter> dead = new CopyOnWriteArrayList<>();
        for (SseEmitter emitter : sseClients) {
            try {
                emitter.send(SseEmitter.event().data(log));
            } catch (IOException e) {
                dead.add(emitter);
            }
        }
        sseClients.removeAll(dead);
    }
}
