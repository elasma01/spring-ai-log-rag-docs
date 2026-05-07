# Spring AI Log RAG

> **Assistant IA personnel pour l'analyse de logs des differentes applications en temps reel**  
> Architecture RAG hybride : recherche par mots-clés (Elasticsearch) + recherche sémantique (PGVector) → génération de réponses contextualisées via un LLM local (Ollama / qwen2.5:3b).

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Stack technique](#stack-technique)
4. [Prérequis](#prérequis)
5. [Structure du projet](#structure-du-projet)
6. [Démarrage rapide](#démarrage-rapide)
7. [Configuration](#configuration)
8. [Services en détail](#services-en-détail)
9. [Pipeline RAG](#pipeline-rag)
10. [API Reference](#api-reference)
11. [Génération de logs](#génération-de-logs)
12. [Streaming SSE](#streaming-sse)
13. [Collection Postman](#collection-postman)
14. [Tests](#tests)
15. [Dépannage](#dépannage)

---

## Vue d'ensemble

Spring AI Log RAG est un assistant d'analyse de logs applicatifs qui combine deux stratégies de recherche complémentaires pour enrichir le contexte fourni à un LLM local :

```
Question utilisateur
        │
        ▼
┌───────────────────────────────────────────────────┐
│              Pipeline RAG Hybride                  │
│                                                   │
│   ┌─────────────────┐    ┌───────────────────┐   │
│   │  Elasticsearch   │    │    PGVector        │   │
│   │  Recherche par  │    │  Recherche         │   │
│   │  mots-clés      │    │  sémantique        │   │
│   │  (top 5 logs)   │    │  cosinus (top 3)   │   │
│   └────────┬────────┘    └────────┬──────────┘   │
│            │                      │               │
│            └──────────┬───────────┘               │
│                       ▼                            │
│              Fusion du contexte                    │
│                       │                            │
│                       ▼                            │
│           Ollama qwen2.5:3b (LLM local)           │
└───────────────────────┬───────────────────────────┘
                        │
                        ▼
            Réponse structurée en français
      (Résumé / Analyse / Problèmes / Recommandations)
```

**Pendant ce temps**, un scheduler ingère automatiquement :
- **1 000 logs** au démarrage (batch ES + PGVector)
- **1 log/seconde** en continu dans Elasticsearch

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                Spring Boot 3.4.3 — Java 21                       │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐   │
│  │ REST API (port 8080)                                      │   │
│  │                                                           │   │
│  │  ChatController                  LogController           │   │
│  │  POST /api/chat       ────────   GET  /api/logs          │   │
│  │  POST /api/chat/stream           GET  /api/logs/stats    │   │
│  │                                  GET  /api/logs/stream   │   │
│  └──────────────────┬────────────────────────────────────────┘   │
│                     │                                            │
│  ┌──────────────────▼────────────────────────────────────────┐   │
│  │ Services                                                  │   │
│  │                                                           │   │
│  │  RagService           — Pipeline RAG hybride             │   │
│  │  LogIngestionService  — Orchestrateur dual-store         │   │
│  │  LogGeneratorService  — Générateur de logs réalistes     │   │
│  └────────────┬─────────────────────────┬─────────────────── ┘   │
│               │                         │                         │
│  ┌────────────▼──────────┐  ┌───────────▼──────────────────┐    │
│  │  Elasticsearch 8.15.3 │  │  PostgreSQL 16 + pgvector     │    │
│  │  Index : spring-boot- │  │  Table : vector_store         │    │
│  │  logs                 │  │  Index : HNSW                 │    │
│  │  Full-text search     │  │  Distance : COSINE            │    │
│  │  :9200                │  │  Dimensions : 768             │    │
│  └───────────────────────┘  │  :5433                        │    │
│                              └──────────────────────────────┘    │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Ollama (local — non dockerisé)                            │  │
│  │  Chat model : qwen2.5:3b   (génération de réponses)        │  │
│  │  Embedding  : nomic-embed-text  (vecteurs 768 dims)        │  │
│  │  Port : 11434                                              │  │
│  └────────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────┐                                    │
│  │  Kibana 8.15.3 (: 5602)  │  — Visualisation dashboards ES     │
│  └──────────────────────────┘                                    │
└──────────────────────────────────────────────────────────────────┘
```

---

## Stack technique

| Composant         | Technologie                                  | Version      |
|-------------------|----------------------------------------------|--------------|
| Application       | Spring Boot + Java                           | 3.4.3 / 21   |
| Framework IA      | Spring AI (Milestone)                        | 1.0.0-M6     |
| LLM local         | Ollama — qwen2.5:3b                          | latest       |
| Embeddings        | Ollama — nomic-embed-text (768 dims)         | latest       |
| Full-text search  | Elasticsearch                                | 8.15.3       |
| Semantic search   | PostgreSQL 16 + pgvector (HNSW / COSINE)     | pg16         |
| Visualisation     | Kibana                                       | 8.15.3       |
| Conteneurisation  | Docker Compose                               | v3.8         |
| Build             | Maven                                        | 3.9+         |

---

## Prérequis

### Obligatoires

- **Docker** >= 24.x + **Docker Compose** >= 2.x
- **Java 21** + **Maven 3.9+**
- **Ollama** installé et en cours d'exécution

### Ollama — Installation et modèles

```bash
# Installation Ollama (Linux)
curl -fsSL https://ollama.ai/install.sh | sh

# Démarrer le service
ollama serve &

# Télécharger les modèles requis
ollama pull qwen2.5:3b           # ~2 GB — modèle de chat
ollama pull nomic-embed-text     # ~274 MB — modèle d'embeddings

# Vérifier
ollama list
```

### Vérification des prérequis

```bash
docker --version       # >= 24.x
docker compose version # >= 2.x
java -version          # 21
curl http://localhost:11434/api/tags  # Ollama running
```

---

## Structure du projet

```
spring-ai-log-rag/
│
├── docker-compose.yml          # 3 services : ES, Kibana, PGVector
├── pom.xml                     # Spring Boot 3.4.3 + Spring AI 1.0.0-M6
├── deploy.sh                   # Script de déploiement automatisé
│
└── src/main/
    ├── java/com/bmax/lograg/
    │   │
    │   ├── LogRagApplication.java          # Point d'entrée + @EnableScheduling
    │   │
    │   ├── controller/
    │   │   ├── ChatController.java         # POST /api/chat (JSON + SSE stream)
    │   │   └── LogController.java          # GET /api/logs (pagination, filtres, SSE)
    │   │
    │   ├── dto/
    │   │   ├── ChatRequest.java            # Record { question: String }
    │   │   └── ChatResponse.java           # Record { answer, esCount, vecCount, ms }
    │   │
    │   ├── model/
    │   │   └── AppLog.java                 # @Document ES — 10 champs (level, service...)
    │   │
    │   ├── repository/
    │   │   └── AppLogRepository.java       # ElasticsearchRepository — 5 méthodes
    │   │
    │   └── service/
    │       ├── RagService.java             # Pipeline hybride ES + PGVector → Ollama
    │       ├── LogIngestionService.java    # Batch 1000 logs + 1/s temps réel
    │       └── LogGeneratorService.java    # Générateur 10 services, 4 niveaux
    │
    └── resources/
        └── application.yml                # Config ES, PostgreSQL, Ollama, PGVector
```

---

## Démarrage rapide

### 1. Cloner

```bash
git clone https://github.com/elasma01/spring-ai-log-rag.git
cd spring-ai-log-rag
```

### 2. Déploiement automatique

```bash
chmod +x deploy.sh
./deploy.sh
```

Le script :
1. Vérifie les prérequis (Docker, Java, Ollama)
2. Démarre Elasticsearch, Kibana, PostgreSQL+PGVector via Docker Compose
3. Attend que les services soient `healthy`
4. Build le projet Maven
5. Lance l'application Spring Boot
6. Affiche les URLs et le statut

### 3. Déploiement manuel étape par étape

```bash
# Étape 1 — Infrastructure
docker compose up -d
docker compose ps   # attendre que tous soient healthy

# Étape 2 — Build
mvn clean package -DskipTests -B

# Étape 3 — Démarrer l'application
java -jar target/spring-ai-log-rag-1.0.0-SNAPSHOT.jar

# Ou avec Spring Boot Maven plugin
mvn spring-boot:run
```

### 4. Attendre l'ingestion initiale

Au premier démarrage, l'application génère et indexe **1 000 logs** en Elasticsearch **et** en PGVector (embedding via Ollama nomic-embed-text). Cette opération prend **2 à 5 minutes** selon les performances du CPU.

Surveiller la progression :

```bash
# Logs de l'application
tail -f logs/spring.log

# Ou via l'API une fois démarrée
curl http://localhost:8080/api/logs/stats
```

Attendre `"initialIngestionComplete": true` avant de commencer à poser des questions.

---

## Configuration

Fichier : `src/main/resources/application.yml`

```yaml
spring:
  application:
    name: spring-ai-log-rag

  # Elasticsearch (port non-standard pour éviter les conflits)
  elasticsearch:
    uris: http://localhost:9200
    connection-timeout: 5s
    socket-timeout: 30s

  # PostgreSQL avec extension pgvector
  datasource:
    url: jdbc:postgresql://localhost:5433/vectordb
    username: postgres
    password: postgres

  ai:
    ollama:
      base-url: http://localhost:11434
      chat:
        model: qwen2.5:3b
        options:
          temperature: 0.2   # Réponses factuelles (faible aléatoire)
          num-ctx: 2048       # Fenêtre de contexte
          top-p: 0.9
      embedding:
        model: nomic-embed-text

    vectorstore:
      pgvector:
        initialize-schema: true      # Crée la table vector_store au démarrage
        index-type: HNSW             # Hierarchical Navigable Small World
        distance-type: COSINE_DISTANCE
        dimensions: 768              # nomic-embed-text
```

### Variables d'environnement (override)

```bash
# Elasticsearch distant
SPRING_ELASTICSEARCH_URIS=http://es-host:9200

# Ollama distant / GPU
SPRING_AI_OLLAMA_BASE_URL=http://ollama-host:11434

# PostgreSQL différent
SPRING_DATASOURCE_URL=jdbc:postgresql://pg-host:5432/vectordb
SPRING_DATASOURCE_USERNAME=myuser
SPRING_DATASOURCE_PASSWORD=mypassword
```

---

## Services en détail

### ChatController

**Responsabilité** : Point d'entrée des questions utilisateur, délègue au pipeline RAG.

| Méthode | Endpoint            | Content-Type produit      | Description                         |
|---------|---------------------|---------------------------|-------------------------------------|
| POST    | `/api/chat`         | `application/json`        | Réponse complète (peut dépasser 30s)|
| POST    | `/api/chat/stream`  | `text/event-stream` (SSE) | Streaming token par token           |

### LogController

**Responsabilité** : Observabilité des logs indexés en temps réel.

| Méthode | Endpoint             | Description                                         |
|---------|----------------------|-----------------------------------------------------|
| GET     | `/api/logs`          | Logs récents paginés (filtres `level`, `service`)   |
| GET     | `/api/logs/stats`    | Statistiques globales (total, répartition niveaux)  |
| GET     | `/api/logs/stream`   | Flux SSE — nouveaux logs en temps réel              |

### RagService — Pipeline hybride

**Étape 1 — Elasticsearch** : Recherche par mots-clés avec scoring BM25.
- Si la question contient `error/erreur/exception/fail` → filtre `level=ERROR`
- Si la question contient `warn/slow/lent/retry` → filtre `WARN + ERROR`
- Sinon → `multi_match` avec fuzzy sur `message`, `exception`, `service`
- Traduction automatique français → anglais pour les termes clés
- Retourne les **5 logs** les plus pertinents

**Étape 2 — PGVector** : Recherche cosinus sur les embeddings.
- La question est transformée en vecteur 768 dims via `nomic-embed-text`
- Recherche des **3 documents** les plus similaires (seuil 0.55)

**Étape 3 — Fusion et génération** : Le contexte fusionné (ES + PGVector) est passé au LLM `qwen2.5:3b` avec un system prompt structurant la réponse en 4 sections.

### LogIngestionService

**Ingestion initiale** (`@EventListener(ApplicationReadyEvent)`) :
- 1 000 logs générés
- Batch ES : 100 logs / requête
- Batch PGVector : 50 logs / requête (contrainte temps d'embedding Ollama)
- Durée : 2–5 minutes

**Ingestion temps réel** (`@Scheduled(fixedRate=1000)`) :
- 1 log/seconde → Elasticsearch uniquement
- PGVector non mis à jour en temps réel (évite de bloquer Ollama)

### LogGeneratorService

Génère des logs Spring Boot **réalistes** simulant 10 microservices :

| Service            | Exemples de messages générés                               |
|--------------------|-------------------------------------------------------------|
| `UserService`      | Auth success/fail, requêtes profil, CRUD utilisateurs       |
| `OrderService`     | Checkout, status updates, transactions                      |
| `PaymentService`   | Gateway timeouts, Stripe errors, transactions réussies      |
| `AuthService`      | Login/logout, JWT expiration, BadCredentialsException       |
| `ProductService`   | Catalogue, inventory lookups                                |
| `CartService`      | Ajout/suppression articles, sessions                        |
| `InventoryService` | Stock checks, réapprovisionnement                           |
| `NotificationService` | Kafka topics, email sending, push notifications          |
| `ReportService`    | Batch processing, scheduled tasks                           |
| `EmailService`     | SMTP errors, template rendering                             |

**Distribution des niveaux** : INFO 60% · WARN 20% · ERROR 15% · DEBUG 5%

**Types d'erreurs simulées** :
- `BadCredentialsException` (auth)
- `TimeoutException` Stripe API (payment)
- `NullPointerException` (divers)
- `DataIntegrityViolationException` (DB duplicate key)
- Circuit breaker OPENED
- `OutOfMemoryError` (heap exhausted)

---

## Pipeline RAG

### Prompt système (LLM)

Le LLM reçoit ce rôle :

```
Tu es un expert senior en analyse de logs Spring Boot, DevOps et architecture microservices.

Compétences :
- Analyser erreurs, exceptions et stack traces Java/Spring Boot
- Identifier patterns de performance (slow requests, timeouts, fuites mémoire)
- Détecter problèmes de sécurité (auth échouées, tentatives d'intrusion)
- Corréler événements via traceId
- Détecter circuits breaker et cascades d'erreurs
- Proposer solutions Spring Boot concrètes

Structure de réponse OBLIGATOIRE :
🔍 Résumé — synthèse en 1-2 lignes
📋 Analyse détaillée — avec citations de logs
⚠️ Problèmes détectés — liste par sévérité
🔧 Recommandations — actions correctives avec exemples Spring Boot
```

### Mapping traduction français → anglais

Le service traduit automatiquement les termes français pour les requêtes Elasticsearch :

| Terme français | Termes anglais recherchés          |
|----------------|------------------------------------|
| erreur         | error, exception, failed           |
| lent / lente   | slow, timeout, ms                  |
| timeout        | timeout, timed out                 |
| circuit        | circuit, breaker                   |
| mémoire        | memory, OutOfMemory, heap          |
| sécurité       | auth, unauthorized, forbidden      |
| paiement       | payment, PaymentService            |
| base           | database, SQL, jdbc                |
| cache          | cache, evict, hit                  |

---

## API Reference

### POST `/api/chat`

Pose une question en langage naturel sur les logs. Réponse JSON complète.

**Corps de la requête** :
```json
{
  "question": "Quelles sont les erreurs les plus fréquentes ?"
}
```

**Réponse 200 OK** :
```json
{
  "answer": "🔍 **Résumé** — 3 types d'erreurs critiques détectés...\n\n📋 **Analyse détaillée**...",
  "sourcesFromElasticsearch": 5,
  "sourcesFromPgVector": 3,
  "processingTimeMs": 8420,
  "timestamp": "2026-05-07T15:30:00Z"
}
```

| Champ                      | Description                                       |
|----------------------------|---------------------------------------------------|
| `answer`                   | Réponse structurée générée par qwen2.5:3b         |
| `sourcesFromElasticsearch` | Nombre de logs récupérés par recherche BM25       |
| `sourcesFromPgVector`      | Nombre de documents récupérés par similarité      |
| `processingTimeMs`         | Temps total du pipeline (retrieval + génération)  |
| `timestamp`                | Horodatage ISO 8601 de la réponse                 |

> **Note** : La génération via `qwen2.5:3b` peut prendre **15 à 60 secondes** selon le matériel. Utiliser `/api/chat/stream` pour éviter les timeouts.

---

### POST `/api/chat/stream`

Version **streaming SSE** — les tokens sont envoyés au fur et à mesure de leur génération.

**Corps** : identique à `/api/chat`

**Réponse** : `text/event-stream`
```
data: 🔍

data:  **

data: Résumé

data:  —

data:  3

data:  erreurs...
```

**Utilisation curl** :
```bash
curl -X POST http://localhost:8080/api/chat/stream \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{"question": "Quelles erreurs critiques ?"}' \
  --no-buffer
```

**Utilisation dans le navigateur (JavaScript)** :
```javascript
const response = await fetch('/api/chat/stream', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ question: 'Quelles erreurs critiques ?' })
});

const reader = response.body.getReader();
const decoder = new TextDecoder();

while (true) {
  const { done, value } = await reader.read();
  if (done) break;
  const text = decoder.decode(value);
  process.stdout.write(text); // ou afficher dans le DOM
}
```

---

### GET `/api/logs`

Retourne les logs indexés dans Elasticsearch, paginés et filtrables.

**Paramètres** :

| Paramètre | Type   | Défaut | Description                             |
|-----------|--------|--------|-----------------------------------------|
| `size`    | int    | 20     | Nombre de logs (max 200)               |
| `level`   | string | —      | Filtre par niveau : ERROR, WARN, INFO, DEBUG |
| `service` | string | —      | Filtre par service (ex: PaymentService) |

**Exemples** :
```bash
# 20 derniers logs
curl http://localhost:8080/api/logs

# 50 dernières erreurs
curl "http://localhost:8080/api/logs?size=50&level=ERROR"

# Logs du PaymentService
curl "http://localhost:8080/api/logs?service=PaymentService"

# Erreurs du PaymentService
curl "http://localhost:8080/api/logs?level=ERROR&service=PaymentService"
```

**Réponse 200** :
```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "timestamp": "2026-05-07T15:30:00Z",
    "level": "ERROR",
    "service": "PaymentService",
    "thread": "http-nio-8080-exec-3",
    "logger": "c.b.l.service.PaymentService",
    "message": "Payment processing FAILED for ORD-54321 — Gateway timeout after 30000ms",
    "exception": "java.util.concurrent.TimeoutException: Stripe API did not respond...",
    "traceId": "trace-a1b2c3d4",
    "environment": "production"
  }
]
```

---

### GET `/api/logs/stats`

Statistiques globales sur les logs ingérés.

```bash
curl http://localhost:8080/api/logs/stats
```

**Réponse 200** :
```json
{
  "totalIndexedInElasticsearch": 1247,
  "totalIngested": 1247,
  "initialIngestionComplete": true,
  "byLevel": {
    "ERROR": 187,
    "WARN":  249,
    "INFO":  748,
    "DEBUG":  63
  },
  "errorRate": "15.0%"
}
```

---

### GET `/api/logs/stream`

Flux SSE — reçoit les nouveaux logs au fur et à mesure de leur ingestion (1/seconde).

```bash
curl -H "Accept: text/event-stream" http://localhost:8080/api/logs/stream
```

**Événements reçus** :
```
data: {"id":"abc","timestamp":"...","level":"INFO","service":"OrderService","message":"..."}

data: {"id":"def","timestamp":"...","level":"ERROR","service":"PaymentService","message":"..."}
```

**Dans le navigateur** :
```javascript
const evtSource = new EventSource('/api/logs/stream');
evtSource.onmessage = e => {
  const log = JSON.parse(e.data);
  console.log(`[${log.level}] ${log.service}: ${log.message}`);
};
```

---

## Génération de logs

Les logs sont générés par `LogGeneratorService` avec des patterns réalistes :

### Exemples de logs INFO
```
2026-05-07T15:30:00Z  INFO   [http-nio-8080-exec-1]  c.b.l.service.OrderService
  Request completed: POST /api/orders/12345 — status=201 — duration=87ms — ip=192.168.1.100

2026-05-07T15:30:01Z  INFO   [kafka-consumer-1]  c.b.l.service.NotificationService
  Kafka message published to topic: order-events — partition=2 — offset=845231
```

### Exemples de logs ERROR
```
2026-05-07T15:30:05Z  ERROR  [http-nio-8080-exec-2]  c.b.l.service.PaymentService
  Payment processing FAILED for ORD-54321 — Gateway timeout after 30000ms
  java.util.concurrent.TimeoutException: Stripe API did not respond within 30000ms
      at c.b.l.service.PaymentService.processPayment(PaymentService.java:89)
  Caused by: java.net.SocketTimeoutException: Read timed out

2026-05-07T15:30:10Z  ERROR  [http-nio-8080-exec-4]  c.b.l.service.AuthService
  Authentication FAILED for john.doe@example.com from ip=203.0.113.42
  org.springframework.security.authentication.BadCredentialsException: Invalid credentials
```

### Exemples de logs WARN
```
2026-05-07T15:30:15Z  WARN   [http-nio-8080-exec-5]  c.b.l.service.OrderService
  Slow query detected: SELECT * FROM orders — duration=2341ms — threshold=500ms

2026-05-07T15:30:20Z  WARN   [scheduling-1]  c.b.l.service.UserService
  Database connection pool near capacity: 18/20 active connections
```

---

## Streaming SSE

### Pourquoi utiliser `/api/chat/stream` ?

Le modèle `qwen2.5:3b` peut prendre **15 à 60 secondes** pour générer une réponse complète. Les proxies, Postman Cloud, et navigateurs ferment les connexions après 30s d'inactivité.

**Solutions** :
- Utiliser `/api/chat/stream` avec SSE → tokens reçus immédiatement
- Dans Postman : cocher **"Send and Download"** ou utiliser **Postman Agent Desktop**
- Avec curl : ajouter `--no-buffer`

### Comportement du streaming

```
Temps 0s    : Connexion établie
Temps 0.1s  : Premier token "🔍" reçu
Temps 0.5s  : "**Résumé**" affiché
...
Temps 10s   : Section "📋 Analyse détaillée" en cours
Temps 25s   : Réponse complète
```

---

## Collection Postman

Le fichier `postman/spring-ai-log-rag.postman_collection.json` contient toutes les requêtes avec tests automatisés.

**Import** : `File → Import → postman/spring-ai-log-rag.postman_collection.json`

**Workflow recommandé** :
1. `01 - Status / Vérifier l'ingestion` — attendre `initialIngestionComplete: true`
2. `02 - Chat RAG / POST /api/chat — question simple` — premier test
3. `02 - Chat RAG / POST /api/chat/stream` — si timeout
4. Explorer `03 - Logs` pour inspecter les données
5. `04 - Actuator` pour le monitoring

---

## Tests

### Démarrer les tests unitaires

```bash
mvn test
```

### Test de bout en bout manuel

```bash
# 1. Vérifier l'état de l'ingestion
curl http://localhost:8080/api/logs/stats | python3 -m json.tool

# 2. Attendre que initialIngestionComplete soit true
# 3. Poser une question
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "Quelles sont les erreurs PaymentService ?"}' \
  | python3 -m json.tool

# 4. Vérifier les sources
curl "http://localhost:8080/api/logs?level=ERROR&service=PaymentService&size=5"

# 5. Kibana — explorer l'index Elasticsearch
open http://localhost:5602
```

### Questions de test recommandées

```
Questions en français :
- "Quelles sont les erreurs les plus fréquentes ?"
- "Y a-t-il des problèmes de performance ?"
- "Analyse les erreurs du PaymentService"
- "Y a-t-il des problèmes d'authentification ?"
- "Quels services ont des circuit breakers ouverts ?"
- "Y a-t-il des fuites mémoire ?"
- "Analyse les timeouts base de données"
- "Quels logs Kafka sont anormaux ?"

Questions en anglais :
- "What are the most critical errors?"
- "Show me all authentication failures"
- "Are there any memory issues?"
- "What payment errors occurred?"
```

---

## Dépannage

### Ollama ne répond pas

```bash
# Vérifier que le service tourne
curl http://localhost:11434/api/tags

# Démarrer si nécessaire
ollama serve &

# Vérifier que les modèles sont présents
ollama list
# doit afficher: qwen2.5:3b, nomic-embed-text
```

### Ingestion initiale bloquée

```bash
# Vérifier les logs de l'application
curl http://localhost:8080/api/logs/stats

# Si totalIndexedInElasticsearch > 0 mais initialIngestionComplete est false,
# l'embedding PGVector est en cours (normal, attendre)

# Vérifier Elasticsearch directement
curl http://localhost:9200/spring-boot-logs/_count

# Vérifier PGVector
psql postgresql://postgres:postgres@localhost:5433/vectordb \
  -c "SELECT COUNT(*) FROM vector_store;"
```

### Elasticsearch non disponible

```bash
# Vérifier le statut du conteneur
docker compose ps elasticsearch-rag

# Voir les logs
docker compose logs elasticsearch-rag --tail 20

# Redémarrer
docker compose restart elasticsearch-rag

# Test direct
curl http://localhost:9200/_cluster/health
```

### Réponse LLM vide ou "No relevant logs found"

Causes possibles :
1. Ingestion pas encore terminée → attendre `initialIngestionComplete: true`
2. PGVector vide → vérifier le count dans PostgreSQL
3. Question trop spécifique → reformuler en termes généraux
4. Modèle Ollama non chargé → vérifier `ollama list`

### Timeout sur `/api/chat`

Utiliser l'endpoint streaming à la place :
```bash
curl -X POST http://localhost:8080/api/chat/stream \
  -H "Content-Type: application/json" \
  -d '{"question": "Votre question ici"}' \
  --no-buffer
```

### Erreur `dimension mismatch` PGVector

Se produit si la dimension configurée (768) ne correspond pas au modèle d'embedding :
```bash
# Reset de la table vector_store
psql postgresql://postgres:postgres@localhost:5433/vectordb \
  -c "DROP TABLE IF EXISTS vector_store; DROP INDEX IF EXISTS vector_store_embedding_idx;"

# Redémarrer l'application (initialize-schema: true recrée la table)
```

---

## Ports et accès rapide

| Service                  | URL                                          |
|--------------------------|----------------------------------------------|
| **Application Spring**   | http://localhost:8080                        |
| **Chat API**             | http://localhost:8080/api/chat               |
| **Logs API**             | http://localhost:8080/api/logs               |
| **Stats**                | http://localhost:8080/api/logs/stats         |
| **Actuator Health**      | http://localhost:8080/actuator/health        |
| **Elasticsearch**        | http://localhost:9200                        |
| **ES Index logs**        | http://localhost:9200/spring-boot-logs/_count|
| **Kibana**               | http://localhost:5602                        |
| **PostgreSQL+PGVector**  | localhost:5433 / vectordb / postgres         |
| **Ollama**               | http://localhost:11434                       |

---

## Licence

POC — Amine El Asmai — 2026  
RAG assistant pour analyse de logs Spring Boot — Non destiné à la production en l'état.
