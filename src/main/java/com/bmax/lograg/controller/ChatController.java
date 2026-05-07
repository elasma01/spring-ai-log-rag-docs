package com.bmax.lograg.controller;

import com.bmax.lograg.dto.ChatRequest;
import com.bmax.lograg.dto.ChatResponse;
import com.bmax.lograg.service.RagService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/chat")
@RequiredArgsConstructor
@Slf4j
public class ChatController {

    private final RagService ragService;

    /** Réponse JSON complète — peut dépasser 30s avec qwen2.5:7b */
    @PostMapping(
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    public ChatResponse chat(@RequestBody ChatRequest request) {
        log.info("Chat: '{}'", request.question());
        RagService.RagResponse rag = ragService.chat(request.question());
        return ChatResponse.from(rag.answer(), rag.esLogsCount(), rag.vectorDocsCount(), rag.processingTimeMs());
    }

    /**
     * Streaming SSE — les tokens arrivent au fur et à mesure.
     * Contourne les timeouts Postman Cloud / proxies.
     *
     * Postman : cocher "Send and Download" ou utiliser l'agent Desktop.
     */
    @PostMapping(
        value = "/stream",
        consumes = MediaType.APPLICATION_JSON_VALUE,
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<String> stream(@RequestBody ChatRequest request) {
        log.info("Stream: '{}'", request.question());
        return ragService.stream(request.question());
    }
}
