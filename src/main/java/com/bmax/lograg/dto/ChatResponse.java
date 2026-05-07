package com.bmax.lograg.dto;

import java.time.Instant;

public record ChatResponse(
    String  answer,
    int     sourcesFromElasticsearch,
    int     sourcesFromPgVector,
    long    processingTimeMs,
    String  timestamp
) {
    public static ChatResponse from(String answer, int esCount, int vecCount, long ms) {
        return new ChatResponse(answer, esCount, vecCount, ms, Instant.now().toString());
    }
}
