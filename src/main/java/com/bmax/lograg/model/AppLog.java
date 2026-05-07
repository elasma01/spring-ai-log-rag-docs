package com.bmax.lograg.model;

import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;

/**
 * Document Elasticsearch représentant un log applicatif Spring Boot.
 * Chaque log est indexé dans ES pour la recherche par mots-clés
 * ET converti en vecteur dans PGVector pour la recherche sémantique.
 */
@Document(indexName = "spring-boot-logs")
@Getter @Setter @Builder @NoArgsConstructor @AllArgsConstructor
public class AppLog {

    @Id
    private String id;

    // Date indexée en epoch_millis pour les tris et agrégations
    @Field(type = FieldType.Date, format = DateFormat.epoch_millis)
    private Instant timestamp;

    // Keyword : pas d'analyse, filtrage exact (INFO / WARN / ERROR / DEBUG)
    @Field(type = FieldType.Keyword)
    private String level;

    @Field(type = FieldType.Keyword)
    private String service;

    @Field(type = FieldType.Keyword)
    private String thread;

    @Field(type = FieldType.Keyword)
    private String logger;

    // Text : tokenisé + analysé pour full-text search
    @Field(type = FieldType.Text, analyzer = "standard")
    private String message;

    @Field(type = FieldType.Text, analyzer = "standard")
    private String exception;

    @Field(type = FieldType.Keyword)
    private String traceId;

    @Field(type = FieldType.Keyword)
    private String environment;

    /**
     * Représentation textuelle complète du log — utilisée pour générer l'embedding PGVector.
     */
    public String toLogString() {
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp).append("  ");
        sb.append(String.format("%-5s", level)).append("  ");
        sb.append("[").append(thread).append("]  ");
        sb.append(logger).append(" - ");
        sb.append(message);
        if (exception != null && !exception.isBlank()) {
            sb.append("\n").append(exception);
        }
        return sb.toString();
    }
}
