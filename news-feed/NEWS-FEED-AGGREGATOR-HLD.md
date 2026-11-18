# News Feed Aggregator — Complete High-Level Design

> **Target Scale**: 500M peak DAU · Sub-200ms P99 feed latency · <5-min breaking news discovery  
> **Author**: Senior Architect Review  
> **Date**: November 2026

---

## Table of Contents

1. [Problem Statement & Requirements](#1-problem-statement--requirements)
2. [Architecture Overview](#2-architecture-overview)
3. [Core Entities & Data Model](#3-core-entities--data-model)
4. [API Design](#4-api-design)
5. [Ingestion Pipeline — Detailed Flow](#5-ingestion-pipeline--detailed-flow)
6. [Content Processing Pipeline — Detailed Flow](#6-content-processing-pipeline--detailed-flow)
7. [Feed Assembly & Read Path — Detailed Flow](#7-feed-assembly--read-path--detailed-flow)
8. [3-Tier Caching Architecture](#8-3-tier-caching-architecture)
9. [Feed Pre-Computation (Fan-Out on Write)](#9-feed-pre-computation-fan-out-on-write)
10. [Cursor-Based Pagination — Detailed Flow](#10-cursor-based-pagination--detailed-flow)
11. [Breaking News Detection — Detailed Flow](#11-breaking-news-detection--detailed-flow)
12. [Personalization & Ranking — Detailed Flow](#12-personalization--ranking--detailed-flow)
13. [Media Processing Pipeline — Detailed Flow](#13-media-processing-pipeline--detailed-flow)
14. [Search Flow](#14-search-flow)
15. [Multi-Region Deployment](#15-multi-region-deployment)
16. [Resilience & Graceful Degradation](#16-resilience--graceful-degradation)
17. [Capacity Estimation](#17-capacity-estimation)
18. [Database Indexing & Query Strategy](#18-database-indexing--query-strategy)
19. [Kafka Topic Design](#19-kafka-topic-design)
20. [Redis Data Structure Design](#20-redis-data-structure-design)
21. [Monitoring & Observability](#21-monitoring--observability)
22. [Tech Stack Rationale](#22-tech-stack-rationale)
23. [Improvements Over the Original Design](#23-improvements-over-the-original-design)
24. [Architectural Trade-offs & Decisions](#24-architectural-trade-offs--decisions)

---

## 1. Problem Statement & Requirements

### 1.1 What We're Building

A global news aggregation platform (like Google News) that collects articles from 50,000+ publishers worldwide and serves a personalized, scrollable news feed to hundreds of millions of users.

### 1.2 Functional Requirements

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1 | Users can view an aggregated feed of news articles from thousands of publishers worldwide | P0 |
| FR-2 | Users can scroll through the feed "infinitely" with seamless pagination | P0 |
| FR-3 | Users can click on articles and be redirected to the publisher's website | P0 |
| FR-4 | Articles are categorized (Sports, Politics, Tech, etc.) and users can browse by category | P1 |
| FR-5 | Users can search for articles by keyword, date range, and category | P1 |
| FR-6 | Breaking news surfaces within minutes across all user feeds | P1 |

### 1.3 Non-Functional Requirements

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-1 | Availability over consistency (AP in CAP theorem) | 99.99% uptime |
| NFR-2 | Scale to 500M peak DAU | 230K peak QPS on feed |
| NFR-3 | Low latency feed load | P50 < 50ms, P99 < 200ms |
| NFR-4 | Article discovery freshness | < 5 minutes for top publishers |
| NFR-5 | Global reach with low latency everywhere | Multi-region active-active |
| NFR-6 | Graceful degradation under failure | Stale data > no data |

### 1.4 Back-of-the-Envelope Estimates

```
DAU (peak)             = 500,000,000
Avg feed loads/user/day = 8
Total feed requests/day = 4,000,000,000
Peak QPS (5x avg)      = (4B / 86400) × 5 ≈ 230,000 QPS

Publishers              = 50,000
Avg articles/publisher  = 10/day
New articles/day        = 500,000
Avg article metadata    = 2 KB
Daily article storage   = 500K × 2KB = 1 GB/day ≈ 365 GB/year

Thumbnails/article      = 3 sizes (100px, 300px, 600px)
Avg thumbnail size      = 50 KB (across sizes)
Daily thumbnail storage = 500K × 50KB × 3 = 75 GB/day ≈ 27 TB/year
```

---

## 2. Architecture Overview

### 2.1 High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                      CLIENT LAYER                                           │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                                  │
│   │  Web App      │    │  iOS App      │    │  Android App  │                                │
│   │  (Next.js)    │    │  (Swift)      │    │  (Kotlin)     │                                │
│   └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                                  │
└──────────┼───────────────────┼───────────────────┼──────────────────────────────────────────┘
           │                   │                   │
           ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                      EDGE LAYER                                             │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                                  │
│   │  GeoDNS       │───▶│  CDN Edge     │───▶│  WAF/DDoS    │                                │
│   │  (Route 53)   │    │  (CloudFront) │    │  (Shield)    │                                │
│   └──────────────┘    └──────────────┘    └──────┬───────┘                                  │
└──────────────────────────────────────────────────┼──────────────────────────────────────────┘
                                                   │
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   API GATEWAY LAYER                                         │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                                  │
│   │  API Gateway   │───▶│  Auth Service  │    │  Rate Limiter │                              │
│   │  (Kong)        │    │  (JWT/OAuth)   │    │  (Token Bucket)│                             │
│   └──────┬───────┘    └──────────────┘    └──────────────┘                                  │
└──────────┼──────────────────────────────────────────────────────────────────────────────────┘
           │
     ┌─────┴──────────────────────────────────────────┐
     │                                                 │
     ▼                                                 ▼
┌─────────────────────────────────┐  ┌──────────────────────────────────────────────────────┐
│        READ PATH (Hot)          │  │              WRITE PATH (Ingestion)                   │
│                                 │  │                                                      │
│  ┌────────────────────────┐     │  │  ┌──────────────┐  ┌──────────────┐                  │
│  │  Feed Assembly Service  │     │  │  │  Webhook      │  │  Crawler      │                │
│  │  (Go, x20-50 instances) │     │  │  │  Receiver     │  │  Fleet        │                │
│  └───────┬────────────────┘     │  │  └──────┬───────┘  └──────┬───────┘                  │
│          │                      │  │         │                  │                          │
│  ┌───────▼────────────────┐     │  │         ▼                  ▼                          │
│  │  Ranking Service        │     │  │  ┌─────────────────────────────┐                     │
│  │  (ML inference, x10-20) │     │  │  │  Kafka (Event Backbone)     │                     │
│  └───────┬────────────────┘     │  │  └──────┬──────────────────────┘                     │
│          │                      │  │         │                                            │
│  ┌───────▼────────────────┐     │  │  ┌──────▼──────────────────────┐                     │
│  │  Search Service         │     │  │  │  Content Processing Pipeline │                    │
│  │  (Elasticsearch)        │     │  │  │  (Dedup → NLP → Quality)    │                    │
│  └────────────────────────┘     │  │  └──────┬──────────────────────┘                     │
│                                 │  │         │                                            │
│  ┌────────────────────────┐     │  │  ┌──────▼──────────────────────┐                     │
│  │  CACHE TIER             │     │  │  │  Media Processing Service   │                     │
│  │  L1: In-process (30s)   │     │  │  └──────┬──────────────────────┘                     │
│  │  L2: Redis Cluster (5m) │     │  │         │                                            │
│  └────────────────────────┘     │  │         ▼                                            │
└─────────────────────────────────┘  │  ┌─────────────────────────────┐                     │
                                     │  │  DB Writer Service           │                     │
                                     │  └──────────────────────────────┘                     │
                                     └──────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    STORAGE LAYER                                            │
│                                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  PostgreSQL    │  │  Redis        │  │  S3           │  │  Elasticsearch│  │  TimescaleDB  │
│  │  (Articles,    │  │  (Feed Cache, │  │  (Thumbnails, │  │  (Full-text   │  │  (Analytics,  │
│  │   Publishers,  │  │   Sessions)   │  │   Media)      │  │   Search)     │  │   Metrics)    │
│  │   Users)       │  │               │  │               │  │               │  │               │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
                                                   │
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              ASYNC / BACKGROUND LAYER                                       │
│                                                                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │  Feed Pre-     │  │  Trending     │  │  Analytics    │  │  CDC           │  │  Notification│
│  │  Computation   │  │  Detection    │  │  Pipeline     │  │  (Debezium)    │  │  Service     │
│  │  Workers       │  │  Service      │  │               │  │                │  │  (FCM/APNs)  │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Service Catalog

| Service | Language | Responsibility | Instances/Region | Scaling Trigger |
|---------|----------|----------------|-------------------|-----------------|
| API Gateway | - (Kong) | Routing, auth, rate limiting | 5-10 | QPS > 50K/instance |
| Feed Assembly Service | Go | Assembles user feeds from cache/DB | 20-50 | CPU > 60% |
| Ranking Service | Python/Go | ML-based article scoring | 10-20 | P99 > 30ms |
| Search Service | Go | Proxies Elasticsearch queries | 5-10 | QPS per instance |
| Webhook Receiver | Go | Receives publisher push notifications | 5 | Queue depth |
| High-Priority Crawler | Go | Crawls critical publishers (Reuters, AP) | 20 | Scheduler queue |
| Mid-Priority Crawler | Go | Crawls regional publishers | 50 | Scheduler queue |
| Low-Priority Crawler | Go | Crawls blogs, niche sites | 100 | Scheduler queue |
| Deduplication Service | Go | SimHash near-duplicate detection | 10-20 | Kafka lag |
| NLP Pipeline | Python | Categorization, NER, embeddings | 20-40 | Kafka lag |
| Quality Scoring Service | Python | Article quality assessment | 10 | Kafka lag |
| Media Processing Service | Go | Thumbnail download, resize, upload | 20-30 | Kafka lag |
| DB Writer Service | Go | Batch writes to PostgreSQL | 10 | Kafka lag |
| Feed Pre-computation Workers | Go | Recomputes regional feed caches | 10-20 | CDC event lag |
| Trending Detection Service | Go/Python | Identifies breaking/trending stories | 5 | Always on |
| Analytics Pipeline | Python/Spark | Click-through, engagement metrics | - | Batch scheduled |
| CDC Service | - (Debezium) | Captures DB changes to Kafka | 3 | Replication lag |
| Notification Service | Go | Breaking news push notifications | 5 | Queue depth |

---

## 3. Core Entities & Data Model

### 3.1 Entity Relationship Diagram

```
┌──────────────────────────────┐       ┌──────────────────────────────┐
│          PUBLISHER           │       │           CATEGORY           │
├──────────────────────────────┤       ├──────────────────────────────┤
│ id           UUID     PK     │       │ slug         VARCHAR  PK     │
│ name         VARCHAR         │       │ display_name VARCHAR         │
│ domain       VARCHAR  UNIQUE │       │ sort_order   INT             │
│ rss_feed_url VARCHAR         │       │ icon_url     VARCHAR         │
│ scrape_type  ENUM            │       │ parent_slug  VARCHAR  FK     │──┐
│   (RSS|ATOM|WEB|WEBHOOK|API) │       └──────────────────────────────┘  │
│ priority     ENUM            │                     ▲                   │
│   (CRITICAL|HIGH|MED|LOW)    │                     │                   │
│ poll_interval_sec  INT       │       ┌─────────────┴────────────────┐  │
│ last_scraped_at TIMESTAMP    │       │     ARTICLE_CATEGORY         │  │
│ region       VARCHAR         │       ├──────────────────────────────┤  │
│ language     VARCHAR         │       │ article_id    BIGINT  FK     │──┼──┐
│ reliability_score FLOAT      │       │ category_slug VARCHAR FK     │──┘  │
│ is_active    BOOLEAN         │       │ confidence    FLOAT          │     │
│ robots_crawl_delay INT       │       └──────────────────────────────┘     │
│ config       JSONB           │                                           │
│ created_at   TIMESTAMP       │                                           │
│ updated_at   TIMESTAMP       │       ┌──────────────────────────────┐    │
└──────────┬───────────────────┘       │       ARTICLE_ENTITY         │    │
           │                           ├──────────────────────────────┤    │
           │ 1:N                       │ article_id    BIGINT  FK     │────┤
           │                           │ entity_name   VARCHAR         │    │
           ▼                           │ entity_type   ENUM           │    │
┌──────────────────────────────┐       │  (PERSON|ORG|LOCATION|EVENT) │    │
│           ARTICLE            │       │ salience      FLOAT          │    │
├──────────────────────────────┤       └──────────────────────────────┘    │
│ id           BIGINT   PK     │◄─────────────────────────────────────────┘
│   (Snowflake ID - monotonic) │
│ publisher_id UUID     FK     │       ┌──────────────────────────────┐
│ title        VARCHAR(500)    │       │        STORY_CLUSTER         │
│ summary      TEXT            │       ├──────────────────────────────┤
│ original_url VARCHAR  UNIQUE │       │ id            UUID    PK     │
│ canonical_url VARCHAR        │       │ label         VARCHAR         │
│ thumbnail_cdn_url VARCHAR    │       │ article_count INT             │
│ published_at TIMESTAMP       │       │ first_seen_at TIMESTAMP       │
│ discovered_at TIMESTAMP      │       │ velocity      FLOAT          │
│ region       VARCHAR(5)      │       │ is_breaking   BOOLEAN        │
│ language     VARCHAR(5)      │       │ representative_article_id BIGINT│
│ simhash      BIGINT          │       └──────────┬───────────────────┘
│ content_hash VARCHAR(64)     │                  │
│ quality_score FLOAT          │                  │ 1:N
│ trending_score FLOAT         │                  │
│ click_count  INT DEFAULT 0   │       ┌──────────▼───────────────────┐
│ categories   JSONB           │       │   STORY_CLUSTER_ARTICLE      │
│ entities     JSONB           │       ├──────────────────────────────┤
│ embedding    VECTOR(768)     │       │ cluster_id    UUID    FK     │
│ search_vector TSVECTOR       │       │ article_id    BIGINT  FK     │───▶ ARTICLE
│ status       ENUM            │       │ is_canonical  BOOLEAN        │
│  (PENDING|ACTIVE|DUPLICATE|  │       └──────────────────────────────┘
│   REMOVED|FLAGGED)           │
│ duplicate_of BIGINT FK NULL  │
│ created_at   TIMESTAMP       │       ┌──────────────────────────────┐
│ updated_at   TIMESTAMP       │       │            USER              │
└──────────────────────────────┘       ├──────────────────────────────┤
                                       │ id           UUID     PK     │
                                       │ region       VARCHAR(5)      │
                                       │ language     VARCHAR(5)      │
                                       │ preferences  JSONB           │
                                       │   { "fav_categories": [...], │
                                       │     "blocked_publishers": [] │
                                       │     "category_weights": {}   │
                                       │   }                          │
                                       │ last_active_at TIMESTAMP     │
                                       │ created_at   TIMESTAMP       │
                                       └──────────┬───────────────────┘
                                                  │
                                                  │ 1:N
                                                  ▼
                                       ┌──────────────────────────────┐
                                       │     USER_INTERACTION         │
                                       ├──────────────────────────────┤
                                       │ id           BIGINT   PK    │
                                       │ user_id      UUID     FK    │
                                       │ article_id   BIGINT   FK    │
                                       │ type         ENUM           │
                                       │  (CLICK|SKIP|SHARE|SAVE|    │
                                       │   DWELL|DISMISS)            │
                                       │ dwell_time_ms INT           │
                                       │ created_at   TIMESTAMP      │
                                       └──────────────────────────────┘
```

### 3.2 Snowflake ID Design

Using Snowflake IDs instead of UUIDs for articles is a **critical architectural decision**:

```
┌───────┬────────────────────────────┬──────────┬──────────┬───────────────┐
│ 1 bit │       41 bits              │  5 bits  │  5 bits  │   12 bits     │
│unused │  timestamp (ms since epoch)│datacenter│ worker   │ sequence num  │
├───────┼────────────────────────────┼──────────┼──────────┼───────────────┤
│   0   │   ~69 years of range       │  32 DCs  │32 workers│ 4096/ms/worker│
└───────┴────────────────────────────┴──────────┴──────────┴───────────────┘

Benefits:
  ✅ Monotonically increasing → efficient B-tree indexing
  ✅ Time-encoded → sortable by creation order
  ✅ Distributed generation → no central bottleneck
  ✅ 64-bit BIGINT → smaller than UUID, cache-friendly
  ✅ Enables cursor-based pagination: WHERE id > :cursor ORDER BY id DESC
```

### 3.3 Why Not UUID?

| Aspect | Snowflake ID | UUID v4 | UUID v7 |
|--------|-------------|---------|---------|
| Size | 8 bytes | 16 bytes | 16 bytes |
| Sortable | ✅ Yes | ❌ No | ✅ Yes |
| B-tree friendly | ✅ Sequential | ❌ Random inserts | ✅ Sequential |
| Cursor pagination | ✅ Natural | ❌ Requires timestamp column | ✅ Possible |
| Index size | Smaller | 2x larger | 2x larger |

---

## 4. API Design

### 4.1 Feed APIs

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GET /v1/feed
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Description: Get personalized news feed for the user

Query Parameters:
  region    (required)  string   User's region code (US, UK, IN, etc.)
  category  (optional)  string   Category slug (default: "general")
  cursor    (optional)  string   Opaque base64 cursor for pagination
  limit     (optional)  int      Articles per page (default: 20, max: 50)

Headers:
  Authorization: Bearer {jwt}    (optional — anonymous users get generic feed)
  Accept-Language: en-US          (content language preference)
  X-Request-ID: {uuid}           (distributed tracing)

Response: 200 OK
{
  "articles": [
    {
      "id": "1823749182374",
      "title": "Global Climate Summit Reaches Historic Agreement",
      "summary": "World leaders have agreed to a landmark climate deal that...",
      "publisher": {
        "id": "pub-reuters-001",
        "name": "Reuters",
        "logo_url": "https://cdn.newsfeed.com/logos/reuters.webp"
      },
      "thumbnail": {
        "small": "https://cdn.newsfeed.com/thumb/100/abc123.webp",
        "medium": "https://cdn.newsfeed.com/thumb/300/abc123.webp",
        "large": "https://cdn.newsfeed.com/thumb/600/abc123.webp"
      },
      "published_at": "2026-11-18T16:30:00Z",
      "original_url": "https://reuters.com/article/climate-summit-2026",
      "categories": ["politics", "environment"],
      "entities": ["UN", "Paris", "Climate Summit"],
      "is_breaking": false,
      "source_count": 47
    }
  ],
  "pagination": {
    "next_cursor": "eyJsIjoiMTgyMzc0OTE4MjM1NCIsInMiOiIwLjcyIn0=",
    "has_more": true,
    "total_estimate": 2000
  },
  "meta": {
    "region": "US",
    "category": "general",
    "served_from": "cache",
    "cache_age_seconds": 12
  }
}

Error Responses:
  400 Bad Request   — invalid region or category
  429 Too Many Reqs — rate limit exceeded (Retry-After header)
  503 Service Unavail — degraded mode, stale feed served

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GET /v1/feed/breaking
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Description: Get current breaking news stories

Query Parameters:
  region    (required)  string   User's region code

Response: 200 OK
{
  "stories": [
    {
      "cluster_id": "cluster-xyz",
      "headline": "Earthquake Strikes Pacific Coast",
      "source_count": 127,
      "first_reported_at": "2026-11-18T15:02:00Z",
      "lead_article": { ... },      // Same structure as feed article
      "other_sources": [ ... ],      // Top 5 alternate publisher articles
      "is_developing": true
    }
  ],
  "banner": {
    "text": "Breaking: 6.4 Earthquake in Pacific Coast Region",
    "severity": "HIGH",
    "updated_at": "2026-11-18T15:15:00Z"
  }
}
```

### 4.2 Article Click-Through API

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GET /v1/articles/{article_id}/click
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Description: Track article click and redirect to publisher website

Path Parameters:
  article_id  (required)  string   Article Snowflake ID

Headers:
  Authorization: Bearer {jwt}  (optional)

Response: 302 Found
  Location: https://reuters.com/article/climate-summit-2026
  
Side Effects:
  - Logs click event to Kafka topic 'user-interactions'
  - Increments article click counter (async, eventually consistent)
  - Updates user interaction history (for personalization)
```

### 4.3 Search API

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GET /v1/search
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Query Parameters:
  q         (required)  string   Search query
  region    (optional)  string   Filter by region
  category  (optional)  string   Filter by category
  from      (optional)  date     Start date (ISO 8601)
  to        (optional)  date     End date (ISO 8601)
  sort      (optional)  string   "relevance" (default) | "date"
  cursor    (optional)  string   Pagination cursor
  limit     (optional)  int      Results per page (default: 20)

Response: 200 OK
{
  "results": [ ... ],    // Same article structure
  "total_hits": 1523,
  "pagination": { "next_cursor": "...", "has_more": true },
  "suggestions": ["climate change policy", "climate summit 2026"]
}
```

### 4.4 User Preferences API

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PUT /v1/users/me/preferences
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Headers:
  Authorization: Bearer {jwt}  (required)

Body:
{
  "favorite_categories": ["tech", "science", "sports"],
  "blocked_publishers": ["pub-tabloid-001"],
  "category_weights": {
    "tech": 1.5,
    "politics": 0.8,
    "entertainment": 0.3
  },
  "language": "en",
  "region": "US"
}

Response: 200 OK
{ "status": "updated", "effective_at": "2026-11-18T16:00:00Z" }
```

### 4.5 Publisher Webhook API

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
POST /v1/webhooks/articles
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Description: Publisher pushes new article notification (PubSubHubbub / WebSub)

Headers:
  X-Publisher-Key: {api_key}   (required)
  X-Hub-Signature: sha256=...  (HMAC verification)
  Content-Type: application/json

Body:
{
  "url": "https://reuters.com/article/breaking-news-123",
  "title": "Breaking: Major Development in...",
  "summary": "In a dramatic turn of events...",
  "published_at": "2026-11-18T16:30:00Z",
  "categories": ["politics", "world"],
  "thumbnail_url": "https://reuters.com/images/article-123.jpg"
}

Response: 202 Accepted
{ "status": "queued", "tracking_id": "ingest-abc-123" }
```

### 4.6 Categories API

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GET /v1/categories
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Response: 200 OK
{
  "categories": [
    { "slug": "general", "name": "Top Stories", "icon": "📰", "sort": 0 },
    { "slug": "politics", "name": "Politics", "icon": "🏛️", "sort": 1 },
    { "slug": "sports", "name": "Sports", "icon": "⚽", "sort": 2 },
    { "slug": "tech", "name": "Technology", "icon": "💻", "sort": 3 },
    { "slug": "business", "name": "Business", "icon": "💼", "sort": 4 },
    { "slug": "science", "name": "Science", "icon": "🔬", "sort": 5 },
    { "slug": "health", "name": "Health", "icon": "🏥", "sort": 6 },
    { "slug": "entertainment", "name": "Entertainment", "icon": "🎬", "sort": 7 },
    { "slug": "world", "name": "World", "icon": "🌍", "sort": 8 }
  ]
}
```

---

## 5. Ingestion Pipeline — Detailed Flow

### 5.1 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION PIPELINE                               │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────┐         │
│  │              ADAPTIVE SCHEDULER                            │         │
│  │                                                            │         │
│  │  ┌──────────────────┐    ┌──────────────────────────┐     │         │
│  │  │ Publisher Registry│───▶│ Priority Queue             │    │         │
│  │  │ (PostgreSQL)      │    │ (Redis Sorted Set)         │    │         │
│  │  └──────────────────┘    │                            │    │         │
│  │                          │ Score = next_scrape_time    │    │         │
│  │                          │ Member = publisher_id       │    │         │
│  │                          └────────┬───────────────────┘    │         │
│  └───────────────────────────────────┼────────────────────────┘         │
│                                      │                                  │
│                    ┌─────────────────┼─────────────────┐               │
│                    │                 │                  │               │
│                    ▼                 ▼                  ▼               │
│  ┌─────────────────────┐ ┌──────────────────┐ ┌───────────────────┐   │
│  │ HIGH-PRI CRAWLERS   │ │ MID-PRI CRAWLERS │ │ LOW-PRI CRAWLERS  │   │
│  │ (x20 workers)       │ │ (x50 workers)    │ │ (x100 workers)    │   │
│  │                     │ │                  │ │                    │   │
│  │ Reuters, AP, BBC    │ │ NYT, CNN, etc.   │ │ Blogs, niche      │   │
│  │ Poll: 5 min         │ │ Poll: 15-30 min  │ │ Poll: 2-6 hours   │   │
│  └────────┬────────────┘ └────────┬─────────┘ └────────┬──────────┘   │
│           │                       │                     │              │
│           └───────────────────────┼─────────────────────┘              │
│                                   │                                    │
│                                   ▼                                    │
│  ┌────────────────────────────────────────────────────────────┐        │
│  │                 WEBHOOK RECEIVER                            │        │
│  │   (Receives instant push from publishers with API keys)    │───┐    │
│  └────────────────────────────────────────────────────────────┘   │    │
│                                                                   │    │
│                                   ┌───────────────────────────────┘    │
│                                   │                                    │
│                                   ▼                                    │
│  ┌────────────────────────────────────────────────────────────┐        │
│  │          KAFKA TOPIC: raw-articles                         │        │
│  │   Partitioned by publisher_id (ensures ordering per pub)   │        │
│  └────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 Detailed Crawling Flow

```
FLOW: RSS Crawl Cycle
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Step 1: Scheduler picks next publisher to crawl
  ┌──────────────────────────────────────────────────────┐
  │ SCHEDULER                                            │
  │                                                      │
  │ ZRANGEBYSCORE publisher_queue 0 {now} LIMIT 0 100    │
  │   → Returns publishers whose next_scrape_time ≤ now  │
  │                                                      │
  │ For each publisher:                                  │
  │   ZREM publisher_queue {publisher_id}                │
  │   Push to crawler work queue                         │
  └──────────────────────────────────────────────────────┘
                           │
                           ▼
Step 2: Crawler fetches RSS feed
  ┌──────────────────────────────────────────────────────┐
  │ CRAWLER WORKER                                       │
  │                                                      │
  │ 1. GET https://reuters.com/rss/topnews               │
  │    Headers:                                          │
  │      If-None-Match: "etag-from-last-crawl"           │
  │      If-Modified-Since: "last-crawl-timestamp"       │
  │                                                      │
  │ 2. If 304 Not Modified → skip, reschedule            │
  │    If 200 OK → parse XML response                    │
  │                                                      │
  │ 3. Parse RSS/Atom XML:                               │
  │    For each <item> in feed:                          │
  │      Extract: title, link, description, pubDate,     │
  │               enclosure (image), guid                │
  │                                                      │
  │ 4. Compute content_hash = SHA256(canonical_url)      │
  │    Check Redis bloom filter for existing URLs:       │
  │      BF.EXISTS article_urls {content_hash}           │
  │    If exists → skip (already ingested)               │
  │    If not → produce to Kafka                         │
  │                                                      │
  │ 5. Produce to Kafka topic 'raw-articles':            │
  │    Key: publisher_id                                 │
  │    Value: {                                          │
  │      publisher_id, title, summary, url,              │
  │      published_at, thumbnail_source_url,             │
  │      raw_content, discovered_at                      │
  │    }                                                 │
  │                                                      │
  │ 6. Update publisher metadata:                        │
  │    UPDATE publishers SET                             │
  │      last_scraped_at = NOW(),                        │
  │      articles_found_this_cycle = {count}             │
  │    WHERE id = {publisher_id}                         │
  │                                                      │
  │ 7. Reschedule: ZADD publisher_queue                  │
  │      {now + adaptive_interval} {publisher_id}        │
  └──────────────────────────────────────────────────────┘
                           │
                           ▼
Step 3: Update bloom filter
  ┌──────────────────────────────────────────────────────┐
  │ For each new article URL:                            │
  │   BF.ADD article_urls {content_hash}                 │
  │                                                      │
  │ Bloom filter config:                                 │
  │   Expected items: 50M (2 years of articles)          │
  │   False positive rate: 0.01%                         │
  │   Memory: ~60 MB                                     │
  └──────────────────────────────────────────────────────┘
```

### 5.3 Adaptive Polling Algorithm

```
ALGORITHM: Compute Next Poll Interval
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

function computePollInterval(publisher):
  
  base_interval = 1800  // 30 minutes in seconds
  
  // Priority multiplier (higher priority = more frequent)
  priority_weights = {
    CRITICAL: 6.0,   // → effective 5 min
    HIGH:     3.0,   // → effective 10 min
    MEDIUM:   1.5,   // → effective 20 min
    LOW:      1.0    // → effective 30 min
  }
  
  // Freshness decay: publishers publishing more → poll more
  articles_last_hour = getRecentArticleCount(publisher, hours=1)
  freshness_decay = max(0.5, 1.0 - (articles_last_hour / 10.0))
  
  // Time-of-day adjustment: less frequent at night (publisher's timezone)
  hour = getCurrentHour(publisher.timezone)
  time_factor = 1.0
  if hour >= 0 and hour <= 6:    // Late night
    time_factor = 3.0            // 3x longer interval at night
  elif hour >= 22:               // Late evening
    time_factor = 2.0
  
  // Reliability penalty: if publisher often returns errors, back off
  error_rate = getErrorRate(publisher, last_24h)
  reliability_factor = 1.0 + (error_rate * 5.0)  // 20% errors → 2x interval
  
  interval = base_interval 
             / priority_weights[publisher.priority]
             * freshness_decay
             * time_factor
             * reliability_factor
  
  // Clamp to reasonable bounds
  return clamp(interval, min=300, max=21600)  // 5 min to 6 hours

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EXAMPLES:

  Reuters (CRITICAL, 5 articles/hour, daytime):
    1800 / 6.0 × 0.5 × 1.0 × 1.0 = 150s → clamped to 300s (5 min)

  NYT (HIGH, 2 articles/hour, daytime):
    1800 / 3.0 × 0.8 × 1.0 × 1.0 = 480s → 8 min

  Regional Paper (MEDIUM, 0 articles/hour, nighttime):
    1800 / 1.5 × 1.0 × 3.0 × 1.0 = 3600s → 60 min

  Blog (LOW, 0 articles/hour, daytime, 10% errors):
    1800 / 1.0 × 1.0 × 1.0 × 1.5 = 2700s → 45 min
```

### 5.4 Webhook Flow (Instant Ingestion)

```
FLOW: Publisher Webhook Push
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Publisher ──POST──▶ Webhook Receiver ──▶ Kafka (raw-articles)

Detailed Steps:

1. Publisher sends HTTP POST to /v1/webhooks/articles
   with article metadata and HMAC signature

2. Webhook Receiver:
   a. Verify HMAC signature using publisher's shared secret
      computed_sig = HMAC-SHA256(request_body, publisher_secret)
      if computed_sig != X-Hub-Signature → 401 Unauthorized
   
   b. Validate payload schema (title, url required)
   
   c. Rate limit check: max 100 articles/hour per publisher
      INCR webhook_rate:{publisher_id}:{hour}
      if count > 100 → 429 Too Many Requests
   
   d. Produce to Kafka 'raw-articles' (same topic as crawled articles)
   
   e. Return 202 Accepted immediately (async processing)

3. Article enters same processing pipeline as crawled articles
   (dedup → NLP → quality scoring → DB write)

Latency: Webhook → article in feed cache: ~30 seconds
  vs. RSS poll: 5 minutes (critical) to 6 hours (low priority)
```

---

## 6. Content Processing Pipeline — Detailed Flow

### 6.1 Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    CONTENT PROCESSING PIPELINE                           │
│                                                                          │
│   Kafka: raw-articles                                                    │
│         │                                                                │
│         ▼                                                                │
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │  STAGE 1: DEDUPLICATION SERVICE                             │       │
│   │                                                             │       │
│   │  Input:  Raw article from crawler/webhook                   │       │
│   │  Output: Article marked as UNIQUE or DUPLICATE              │       │
│   │  Method: SimHash fingerprinting + Hamming distance          │       │
│   │  Latency: ~5ms per article                                  │       │
│   └────────────────────────┬────────────────────────────────────┘       │
│                            │                                             │
│                     ┌──────┴──────┐                                      │
│                     │             │                                       │
│               UNIQUE         DUPLICATE                                   │
│                     │             │                                       │
│                     ▼             ▼                                       │
│   Kafka: unique-articles    Link to canonical                            │
│         │                   (update story cluster)                       │
│         │                                                                │
│         ▼                                                                │
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │  STAGE 2: NLP ENRICHMENT PIPELINE                           │       │
│   │                                                             │       │
│   │  2a. Language Detection (fastText)         → language: "en" │       │
│   │  2b. Category Classification (fine-tuned BERT)              │       │
│   │      → categories: ["politics", "economy"] + confidence     │       │
│   │  2c. Named Entity Recognition (spaCy)                       │       │
│   │      → entities: ["Biden", "White House", "NATO"]           │       │
│   │  2d. Embedding Generation (Sentence-BERT)                   │       │
│   │      → 768-dim vector for similarity/personalization        │       │
│   │  Latency: ~50ms per article (can run 2a-2d in parallel)     │       │
│   └────────────────────────┬────────────────────────────────────┘       │
│                            │                                             │
│                            ▼                                             │
│   Kafka: enriched-articles                                               │
│         │                                                                │
│         ▼                                                                │
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │  STAGE 3: QUALITY SCORING SERVICE                           │       │
│   │                                                             │       │
│   │  Inputs:                                                    │       │
│   │    publisher.reliability_score    (0.0 - 1.0)               │       │
│   │    article.published_at           (recency)                 │       │
│   │    story_cluster.article_count    (source count)             │       │
│   │    article.summary.length         (content depth)            │       │
│   │    article.has_thumbnail          (visual quality)           │       │
│   │                                                             │       │
│   │  Formula:                                                   │       │
│   │    quality = 0.35 × publisher_trust                         │       │
│   │            + 0.25 × recency_score                           │       │
│   │            + 0.20 × source_diversity                        │       │
│   │            + 0.10 × content_depth                           │       │
│   │            + 0.10 × visual_quality                          │       │
│   │  Latency: ~5ms per article                                  │       │
│   └────────────────────────┬────────────────────────────────────┘       │
│                            │                                             │
│                            ├──────────────────────┐                      │
│                            │                      │                      │
│                            ▼                      ▼                      │
│   ┌─────────────────────────────┐  ┌────────────────────────────┐       │
│   │  STAGE 4: MEDIA PROCESSING  │  │  STAGE 5: DB WRITER        │       │
│   │                             │  │                            │       │
│   │  1. Download source image   │  │  Batch insert into         │       │
│   │  2. Validate (not broken,   │  │  PostgreSQL:               │       │
│   │     not offensive)          │  │    - articles table        │       │
│   │  3. Resize to 3 variants:  │  │    - article_categories    │       │
│   │     100px, 300px, 600px    │  │    - article_entities      │       │
│   │  4. Convert to WebP        │  │    - story_clusters        │       │
│   │  5. Upload to S3           │  │                            │       │
│   │  6. Generate CDN URL       │  │  Also index in             │       │
│   │  Latency: ~200ms           │  │  Elasticsearch             │       │
│   └─────────────────────────────┘  └────────────────────────────┘       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘

Total pipeline latency: ~300ms per article (excluding media download)
Throughput: ~5,000 articles/minute with current worker counts
```

### 6.2 Deduplication — SimHash Algorithm Detail

```
ALGORITHM: SimHash-Based Near-Duplicate Detection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHY THIS MATTERS:
  When a major story breaks, 200+ publishers cover it.
  Without dedup, the feed shows the SAME story 200 times.
  This is the #1 missing piece in most interview designs.

HOW SIMHASH WORKS:

  Input: "Global leaders reach historic climate agreement at UN summit"

  Step 1: Tokenize into shingles (3-word sliding window)
    ["global leaders reach", "leaders reach historic", 
     "reach historic climate", "historic climate agreement",
     "climate agreement at", "agreement at UN", "at UN summit"]

  Step 2: Hash each shingle to 64-bit value
    hash("global leaders reach")     = 1011001...0  (64 bits)
    hash("leaders reach historic")   = 0110110...1
    hash("reach historic climate")   = 1100101...0
    ...

  Step 3: Create weighted column sum
    For each bit position (0-63):
      If bit = 1 → add +1 to column sum
      If bit = 0 → add -1 to column sum

  Step 4: Produce final SimHash
    For each column:
      If sum > 0 → bit = 1
      If sum ≤ 0 → bit = 0
    
    Result: 64-bit SimHash fingerprint

  Step 5: Compare against existing articles
    Hamming distance = popcount(simhash_new XOR simhash_existing)
    
    If distance ≤ 3 → NEAR-DUPLICATE (same story, different words)
    If distance ≤ 6 → RELATED (same topic, different angle)
    If distance > 6  → UNIQUE

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

LOOKUP OPTIMIZATION (avoid O(N) scan of all articles):

  Problem: Comparing against millions of existing articles is too slow
  
  Solution: Bit-sampling tables (band technique)
  
  Split 64-bit SimHash into 4 blocks of 16 bits each:
    Block A: bits 0-15
    Block B: bits 16-31
    Block C: bits 32-47
    Block D: bits 48-63
  
  Maintain 4 Redis hash tables:
    simhash_block_a: { "1011001101010011" → [article_id_1, article_id_7, ...] }
    simhash_block_b: { "0110110010110101" → [article_id_2, article_id_9, ...] }
    simhash_block_c: { ... }
    simhash_block_d: { ... }
  
  Lookup: query all 4 tables, union candidates, compute exact Hamming distance
  
  This guarantees finding duplicates with Hamming distance ≤ 3
  because at least one 16-bit block must be identical.
  
  Lookup complexity: O(1) per table × 4 tables + O(candidates) verification

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

WHAT HAPPENS WHEN A DUPLICATE IS FOUND:

  1. Do NOT insert as a new feed item
  2. Link to the canonical (first-seen) article:
     UPDATE articles SET duplicate_of = {canonical_id} 
       WHERE id = {new_id}
  3. Add to story cluster:
     INSERT INTO story_cluster_articles (cluster_id, article_id)
  4. Boost canonical article's quality score:
     quality_score += 0.02 per additional source (more sources = more credible)
  5. Update source_count on the canonical article
  6. Record the alternate publisher as an "other source" for the story
```

### 6.3 NLP Enrichment Detail

```
STAGE 2a: LANGUAGE DETECTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Model: fastText lid.176.bin (compressed, ~900KB)
  Input: article.title + first 200 chars of summary
  Output: { language: "en", confidence: 0.98 }
  Latency: < 1ms
  
  Action: Set article.language = detected language
  If confidence < 0.7 → flag for manual review

STAGE 2b: CATEGORY CLASSIFICATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Model: Fine-tuned DistilBERT (multi-label classifier)
  Input: article.title + article.summary
  Output: [
    { category: "politics", confidence: 0.92 },
    { category: "economy",  confidence: 0.78 },
    { category: "world",    confidence: 0.65 }
  ]
  Latency: ~15ms (CPU), ~3ms (GPU)
  
  Categories (25):
    general, politics, business, tech, science, health,
    sports, entertainment, world, environment, education,
    lifestyle, travel, food, automotive, real_estate,
    opinion, local, national, crime, weather, culture,
    finance, energy, defense
  
  Action: Insert into article_categories with confidence scores
  Threshold: Only keep categories with confidence > 0.5

STAGE 2c: NAMED ENTITY RECOGNITION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Model: spaCy en_core_web_lg (or custom fine-tuned)
  Input: article.title + article.summary
  Output: [
    { text: "Biden",       type: "PERSON",   salience: 0.9 },
    { text: "White House",  type: "ORG",      salience: 0.8 },
    { text: "NATO",         type: "ORG",      salience: 0.7 },
    { text: "Brussels",     type: "LOCATION", salience: 0.5 }
  ]
  Latency: ~10ms
  
  Action: 
    - Store in article_entities table
    - Index in Elasticsearch for entity-based search
    - Use for story clustering (articles about same entities)

STAGE 2d: EMBEDDING GENERATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Model: all-MiniLM-L6-v2 (Sentence-BERT, 384-dim)
         or all-mpnet-base-v2 (768-dim, higher quality)
  Input: article.title + article.summary
  Output: 768-dimensional float vector
  Latency: ~20ms
  
  Purpose:
    - Content similarity for "More like this" features
    - User interest modeling (avg of clicked article embeddings)
    - Story clustering refinement
  
  Storage: PostgreSQL with pgvector extension
    CREATE INDEX ON articles USING ivfflat (embedding vector_cosine_ops)
```

---

## 7. Feed Assembly & Read Path — Detailed Flow

### 7.1 Complete Request Flow

```
FLOW: User Opens News Feed
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Client                  CDN         API Gateway    Feed Service     Redis       PostgreSQL
  │                      │              │              │              │              │
  │  GET /v1/feed        │              │              │              │              │
  │  ?region=US          │              │              │              │              │
  │  &category=tech      │              │              │              │              │
  │  &limit=20           │              │              │              │              │
  │─────────────────────▶│              │              │              │              │
  │                      │              │              │              │              │
  │   ┌─────────────────┐│              │              │              │              │
  │   │ L0: CDN Edge    ││              │              │              │              │
  │   │ Cache Check     ││              │              │              │              │
  │   │                 ││              │              │              │              │
  │   │ Key: /v1/feed   ││              │              │              │              │
  │   │   ?region=US    ││              │              │              │              │
  │   │   &category=tech││              │              │              │              │
  │   │   (anon only)   ││              │              │              │              │
  │   │                 ││              │              │              │              │
  │   │ HIT? → return   ││              │              │              │              │
  │   │ MISS? → forward ││              │              │              │              │
  │   └─────────────────┘│              │              │              │              │
  │                      │──────────────▶              │              │              │
  │                      │              │              │              │              │
  │                      │   ┌──────────────────────┐  │              │              │
  │                      │   │ 1. Auth check (JWT)  │  │              │              │
  │                      │   │ 2. Rate limit check  │  │              │              │
  │                      │   │ 3. Route to Feed Svc │  │              │              │
  │                      │   └──────────────────────┘  │              │              │
  │                      │              │──────────────▶│              │              │
  │                      │              │              │              │              │
  │                      │              │   ┌──────────────────────────────────┐     │
  │                      │              │   │ FEED ASSEMBLY SERVICE            │     │
  │                      │              │   │                                  │     │
  │                      │              │   │ Step 1: Check L1 (in-process)    │     │
  │                      │              │   │   key = "feed:US:tech:page1"     │     │
  │                      │              │   │   Caffeine cache, TTL 30s        │     │
  │                      │              │   │   HIT? → skip to Step 5          │     │
  │                      │              │   │   MISS? → continue               │     │
  │                      │              │   │                                  │     │
  │                      │              │   │ Step 2: Check L2 (Redis)         │     │
  │                      │              │   │──────────────────────────────────▶│     │
  │                      │              │   │   ZREVRANGE feed:US:tech 0 19    │     │
  │                      │              │   │◀──────────────────────────────────│     │
  │                      │              │   │   Returns: [article_id_1, ...]   │     │
  │                      │              │   │   HIT? → continue to Step 4      │     │
  │                      │              │   │   MISS? → Step 3 (DB fallback)   │     │
  │                      │              │   │                                  │     │
  │                      │              │   │ Step 3: DB Fallback (rare)       │     │
  │                      │              │   │──────────────────────────────────────────▶
  │                      │              │   │   SELECT * FROM articles          │     │
  │                      │              │   │   WHERE region='US'               │     │
  │                      │              │   │     AND status='ACTIVE'           │     │
  │                      │              │   │     AND 'tech' = ANY(categories)  │     │
  │                      │              │   │   ORDER BY quality_score DESC,    │     │
  │                      │              │   │           id DESC                 │     │
  │                      │              │   │   LIMIT 20                        │     │
  │                      │              │   │◀──────────────────────────────────────────
  │                      │              │   │                                  │     │
  │                      │              │   │ Step 4: Hydrate article metadata │     │
  │                      │              │   │──────────────────────────────────▶│     │
  │                      │              │   │   MGET article:{id1} article:{id2}│     │
  │                      │              │   │   ...article:{id20}              │     │
  │                      │              │   │◀──────────────────────────────────│     │
  │                      │              │   │                                  │     │
  │                      │              │   │   If any miss → batch fetch from │     │
  │                      │              │   │   PostgreSQL and backfill cache  │     │
  │                      │              │   │                                  │     │
  │                      │              │   │ Step 5: Apply personalization    │     │
  │                      │              │   │   (if authenticated user)        │     │
  │                      │              │   │   - Boost user's fav categories  │     │
  │                      │              │   │   - Filter blocked publishers    │     │
  │                      │              │   │   - Demote already-clicked       │     │
  │                      │              │   │   - Re-rank top 20              │     │
  │                      │              │   │                                  │     │
  │                      │              │   │ Step 6: Build response           │     │
  │                      │              │   │   - Construct cursor from        │     │
  │                      │              │   │     last article (id + score)    │     │
  │                      │              │   │   - Set Cache-Control headers    │     │
  │                      │              │   │   - Populate L1 cache            │     │
  │                      │              │   └──────────────────────────────────┘     │
  │                      │              │◀─────────────│              │              │
  │                      │◀─────────────│              │              │              │
  │◀─────────────────────│              │              │              │              │
  │                      │              │              │              │              │
  │   200 OK             │              │              │              │              │
  │   {articles, cursor} │              │              │              │              │
  │                      │              │              │              │              │

TOTAL LATENCY BUDGET:
  ┌─────────────────────────┬──────────┬──────────┬──────────┐
  │ Component               │  P50     │  P99     │  Budget  │
  ├─────────────────────────┼──────────┼──────────┼──────────┤
  │ GeoDNS + TLS handshake  │   10ms   │   30ms   │   30ms   │
  │ CDN Edge processing     │    5ms   │   15ms   │   15ms   │
  │ API Gateway + Auth      │    5ms   │   15ms   │   15ms   │
  │ Feed Assembly (L1 hit)  │    1ms   │    3ms   │    5ms   │
  │ Feed Assembly (L2 hit)  │    3ms   │   10ms   │   15ms   │
  │ Article hydration       │    5ms   │   15ms   │   15ms   │
  │ Personalization ranking │   10ms   │   30ms   │   35ms   │
  │ Serialization (JSON)    │    2ms   │    5ms   │    5ms   │
  │ Network (edge → user)   │   10ms   │   40ms   │   40ms   │
  ├─────────────────────────┼──────────┼──────────┼──────────┤
  │ TOTAL (L2 hit)          │  ~44ms   │ ~143ms   │ < 200ms  │
  │ TOTAL (L1 hit)          │  ~28ms   │  ~93ms   │ < 100ms  │
  │ TOTAL (CDN hit)         │  ~15ms   │  ~45ms   │ <  50ms  │
  └─────────────────────────┴──────────┴──────────┴──────────┘
```

### 7.2 Scroll / Next Page Flow

```
FLOW: User Scrolls Down (Infinite Scroll)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Client (Browser/App)                       Feed Assembly Service
  │                                              │
  │  User scrolls to bottom of current articles  │
  │  Client JS detects scroll position           │
  │  (IntersectionObserver on sentinel element)   │
  │                                              │
  │  GET /v1/feed                                │
  │  ?region=US                                  │
  │  &category=tech                              │
  │  &limit=20                                   │
  │  &cursor=eyJsIjoiMTgyMzc0OTE4MjM1NCIsInMiOiIwLjcyIn0=
  │─────────────────────────────────────────────▶│
  │                                              │
  │                              ┌───────────────────────────────────┐
  │                              │ Decode cursor:                    │
  │                              │   { last_id: 1823749182354,       │
  │                              │     last_score: 0.72 }            │
  │                              │                                   │
  │                              │ Redis query:                      │
  │                              │   ZREVRANGEBYSCORE feed:US:tech   │
  │                              │     0.72 -inf                     │
  │                              │     LIMIT 0 20                    │
  │                              │                                   │
  │                              │ For tied scores, filter by id:    │
  │                              │   Skip articles with id >=        │
  │                              │   last_id when score == 0.72      │
  │                              │                                   │
  │                              │ Hydrate + personalize + respond   │
  │                              └───────────────────────────────────┘
  │                                              │
  │◀─────────────────────────────────────────────│
  │   200 OK                                     │
  │   { articles: [...20 more...],               │
  │     pagination: {                            │
  │       next_cursor: "eyJs...",                │
  │       has_more: true                         │
  │     }                                        │
  │   }                                          │
  │                                              │
  │  Client appends articles to DOM              │
  │  Updates cursor for next scroll              │
  │                                              │

WHY CURSOR > OFFSET PAGINATION:

  OFFSET-based (page=3, limit=20):
    SELECT ... ORDER BY score DESC OFFSET 40 LIMIT 20
    
    Problems:
    ❌ If 5 new articles inserted → user sees 5 duplicates on next page
    ❌ OFFSET 40 still scans and discards 40 rows → O(offset) cost
    ❌ At OFFSET 10000, query is very slow
  
  CURSOR-based (last_score=0.72, last_id=182374):
    SELECT ... WHERE (score, id) < (0.72, 182374) LIMIT 20
    
    Benefits:
    ✅ Stable — new articles don't affect position
    ✅ Index-only scan — O(limit) cost regardless of position
    ✅ Works perfectly with sorted sets in Redis (ZREVRANGEBYSCORE)
    ✅ No duplicates, no skipped articles
```

---

## 8. 3-Tier Caching Architecture

### 8.1 Cache Hierarchy

```
┌─────────────────────────────────────────────────────────────────────┐
│                      CACHE HIERARCHY                                │
│                                                                     │
│  ┌───────────────────────────────────────────────────────────┐     │
│  │  L0: CDN EDGE CACHE (CloudFront / Akamai)                │     │
│  │                                                           │     │
│  │  What:    Full HTTP responses for anonymous feed requests  │     │
│  │  TTL:     60 seconds                                      │     │
│  │  Key:     URL path + query params (region, category, page)│     │
│  │  Hit Rate: ~40% of all requests (anonymous users)         │     │
│  │  Latency: 5-15ms (served from nearest PoP)               │     │
│  │                                                           │     │
│  │  Cache-Control: public, max-age=60, stale-while-          │     │
│  │                 revalidate=300, stale-if-error=3600       │     │
│  │                                                           │     │
│  │  NOT cached: authenticated requests, search, breaking     │     │
│  └───────────────────────────┬───────────────────────────────┘     │
│                              │ MISS                                 │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────┐     │
│  │  L1: IN-PROCESS CACHE (Caffeine / Guava)                  │     │
│  │                                                           │     │
│  │  What:    Deserialized feed objects in JVM/Go memory      │     │
│  │  TTL:     30 seconds                                      │     │
│  │  Size:    ~500 hot feeds per instance (~50 MB)            │     │
│  │  Hit Rate: ~60% of requests reaching this layer           │     │
│  │  Latency: < 1ms (zero network hop)                        │     │
│  │                                                           │     │
│  │  Key:     "feed:{region}:{category}:{cursor_bucket}"      │     │
│  │  Eviction: Size-based LRU + TTL                           │     │
│  │                                                           │     │
│  │  WHY: Eliminates Redis round-trip for hot feeds.          │     │
│  │  With 50 Feed Service instances, top feeds are cached     │     │
│  │  in EVERY instance, absorbing massive read amplification. │     │
│  └───────────────────────────┬───────────────────────────────┘     │
│                              │ MISS                                 │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────┐     │
│  │  L2: REGIONAL REDIS CLUSTER                               │     │
│  │                                                           │     │
│  │  What:    Pre-computed sorted feeds + article metadata     │     │
│  │  TTL:     5 minutes (soft) / 30 minutes (hard)            │     │
│  │  Size:    ~50 GB per region                               │     │
│  │  Hit Rate: ~95% of requests reaching this layer           │     │
│  │  Latency: 3-10ms                                          │     │
│  │                                                           │     │
│  │  Topology:                                                │     │
│  │    1 Master (writes only) + N Read Replicas               │     │
│  │    N scales based on regional traffic:                    │     │
│  │      US: 20 replicas                                      │     │
│  │      EU: 15 replicas                                      │     │
│  │      APAC: 25 replicas (highest population)               │     │
│  │                                                           │     │
│  │  Sentinel: Automatic master failover                      │     │
│  │  Replication lag: < 200ms (acceptable for news)           │     │
│  │                                                           │     │
│  │  DUAL TTL STRATEGY:                                       │     │
│  │    Soft TTL (5 min): triggers async background refresh    │     │
│  │    Hard TTL (30 min): data expires, force DB query        │     │
│  │    → Ensures always-fresh without thundering herd         │     │
│  └───────────────────────────┬───────────────────────────────┘     │
│                              │ MISS (< 5% of all requests)         │
│                              ▼                                      │
│  ┌───────────────────────────────────────────────────────────┐     │
│  │  L3: POSTGRESQL READ REPLICAS                             │     │
│  │                                                           │     │
│  │  What:    Source of truth for article data                 │     │
│  │  Latency: 10-50ms depending on query complexity           │     │
│  │  Topology: 1 Primary + 3-5 Read Replicas per region       │     │
│  │                                                           │     │
│  │  On query: also backfill L2 cache for future requests     │     │
│  └───────────────────────────────────────────────────────────┘     │
│                                                                     │
│  COMBINED HIT RATES:                                                │
│    L0 (CDN):  40% of total traffic → never hits origin             │
│    L1 (proc): 60% of remaining → ~36% of total                     │
│    L2 (Redis): 95% of remaining → ~23% of total                    │
│    L3 (DB):   5% of remaining  → ~1% of total                      │
│                                                                     │
│    Result: Only ~1% of feed requests reach PostgreSQL!              │
│    At 230K peak QPS: only ~2,300 QPS hit the database               │
└─────────────────────────────────────────────────────────────────────┘
```

### 8.2 Cache Invalidation Strategy

```
STRATEGY: CDC-Based Cache Invalidation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PostgreSQL ──CDC (Debezium)──▶ Kafka ──▶ Feed Workers ──▶ Redis

Flow:
  1. New article INSERT into PostgreSQL
  2. Debezium captures WAL change, produces to Kafka topic 'db-changes'
  3. Feed Pre-computation Worker consumes event
  4. Worker determines affected feeds:
     - Article region = "US", categories = ["tech", "sports"]
     - Affected feeds: feed:US:general, feed:US:tech, feed:US:sports
  5. Worker updates Redis sorted sets:
     - ZADD feed:US:tech {quality_score} {article_id}
     - ZADD feed:US:general {quality_score} {article_id}
     - ZADD feed:US:sports {quality_score} {article_id}
  6. Worker trims feeds to keep top 2000:
     - ZREMRANGEBYRANK feed:US:tech 0 -2001
  7. Worker stores article metadata:
     - SET article:{article_id} {json_metadata} EX 86400
  8. CDN invalidation (optional, for breaking news only):
     - POST CloudFront invalidation for /v1/feed?region=US*

WHY CDC OVER OTHER APPROACHES:

  ❌ TTL-only: stale for up to TTL duration, breaking news delayed
  ❌ Application-level invalidation: tight coupling, easy to miss
  ❌ Pub/Sub: not durable, messages lost if consumer is down
  ✅ CDC: guaranteed delivery, replayable, decoupled, ordered
```

---

## 9. Feed Pre-Computation (Fan-Out on Write)

### 9.1 Concept

```
CONCEPT: Fan-Out on Write vs Fan-Out on Read
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FAN-OUT ON READ (what the original design does):
  Every user request → query DB → filter → sort → return
  
  Problem at scale:
    230K QPS × complex query = database melts
    Even with caching, cold starts and cache misses are painful

FAN-OUT ON WRITE (what we do):
  Every new article → compute which feeds it belongs to → update those feeds
  
  Math:
    15 regions × 25 categories = 375 feeds to maintain
    500K new articles/day = ~6 articles/second
    6 articles × ~3 feeds each = ~18 feed updates/second
    
    18 Redis ZADD operations/second is TRIVIAL.
    We pre-compute all feeds with negligible write cost.

HYBRID APPROACH (best of both):
  - Regional/category feeds: pre-computed (fan-out on write)
  - Personalization overlay: computed at read time (fan-out on read)
  
  This works because:
    - 375 regional feeds are shared by millions of users (huge cache hit rate)
    - Personalization is a lightweight re-ranking of an already-fetched list
    - User-specific data (click history, preferences) only affects ordering
```

### 9.2 Pre-Computation Worker Flow

```
FLOW: Feed Pre-Computation on New Article
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Kafka (db-changes)        Feed Worker              Redis
     │                        │                       │
     │ New article event      │                       │
     │ {                      │                       │
     │   id: 1823749182374    │                       │
     │   region: "US"         │                       │
     │   categories: [        │                       │
     │     "tech", "business" │                       │
     │   ]                    │                       │
     │   quality_score: 0.87  │                       │
     │   status: "ACTIVE"     │                       │
     │ }                      │                       │
     │───────────────────────▶│                       │
     │                        │                       │
     │               ┌────────────────────────┐       │
     │               │ 1. Determine affected  │       │
     │               │    feeds:              │       │
     │               │    - feed:US:general   │       │
     │               │    - feed:US:tech      │       │
     │               │    - feed:US:business  │       │
     │               │    - feed:GLOBAL:tech  │       │
     │               │    - feed:GLOBAL:general│      │
     │               └────────────────────────┘       │
     │                        │                       │
     │                        │ PIPELINE (atomic):    │
     │                        │                       │
     │                        │ ZADD feed:US:general  │
     │                        │   0.87 1823749182374  │
     │                        │──────────────────────▶│
     │                        │                       │
     │                        │ ZADD feed:US:tech     │
     │                        │   0.87 1823749182374  │
     │                        │──────────────────────▶│
     │                        │                       │
     │                        │ ZADD feed:US:business │
     │                        │   0.87 1823749182374  │
     │                        │──────────────────────▶│
     │                        │                       │
     │                        │ ZREMRANGEBYRANK       │
     │                        │   feed:US:general     │
     │                        │   0 -2001             │
     │                        │   (trim to 2000 max)  │
     │                        │──────────────────────▶│
     │                        │                       │
     │                        │ SET article:182374..  │
     │                        │   {full metadata JSON}│
     │                        │   EX 86400            │
     │                        │──────────────────────▶│
     │                        │                       │
     │                        │ EXEC pipeline         │
     │                        │──────────────────────▶│
     │                        │                       │
     │               ┌────────────────────────┐       │
     │               │ 2. If breaking news:   │       │
     │               │    Invalidate CDN edge │       │
     │               │    for affected regions│       │
     │               └────────────────────────┘       │
     │                        │                       │

FEED REFRESH CYCLE:
  New article published → in Redis feed cache within ~2 seconds
  Breaking news → CDN invalidated within ~5 seconds
  User sees new article → within 60 seconds (CDN TTL worst case)
```

---

## 10. Cursor-Based Pagination — Detailed Flow

```
PAGINATION: Cursor Design & Implementation
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CURSOR STRUCTURE:
  
  Plaintext: { "last_id": 1823749182374, "last_score": 0.87 }
  
  Encoded:   base64url(json) = "eyJsYXN0X2lkIjoxODIzNzQ5MTgyMzc0LCJsYXN0X3Njb3JlIjowLjg3fQ"
  
  Why base64? 
    - Opaque to clients (they can't construct or modify cursors)
    - Stable across API versions
    - Can add fields later without breaking clients

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FIRST PAGE REQUEST (no cursor):

  Client: GET /v1/feed?region=US&category=tech&limit=20
  
  Service:
    // Get top 20 from Redis sorted set
    articles = ZREVRANGE feed:US:tech 0 19 WITHSCORES
    
    // Build cursor from last item
    last = articles[19]
    cursor = base64({ last_id: last.id, last_score: last.score })
    
    // Response
    return {
      articles: [...20 articles...],
      pagination: {
        next_cursor: cursor,
        has_more: ZCARD feed:US:tech > 20
      }
    }

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SUBSEQUENT PAGE REQUEST (with cursor):

  Client: GET /v1/feed?region=US&category=tech&limit=20
          &cursor=eyJsYXN0X2lkIjoxODIzNzQ5MTgyMzc0LCJsYXN0X3Njb3JlIjowLjg3fQ
  
  Service:
    // Decode cursor
    { last_id: 1823749182374, last_score: 0.87 } = decode(cursor)
    
    // Get next 20 articles with score ≤ 0.87
    // ZREVRANGEBYSCORE returns items in descending score order
    candidates = ZREVRANGEBYSCORE feed:US:tech 0.87 -inf LIMIT 0 40
    
    // Filter out articles with score == 0.87 AND id >= last_id
    // (to handle score ties correctly)
    filtered = candidates.filter(a => 
      a.score < 0.87 || (a.score == 0.87 && a.id < last_id)
    ).take(20)
    
    // Build next cursor
    last = filtered[19]
    cursor = base64({ last_id: last.id, last_score: last.score })
    
    return {
      articles: filtered,
      pagination: {
        next_cursor: cursor,
        has_more: filtered.length == 20
      }
    }

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

EDGE CASES:

  1. New articles added while user is scrolling:
     ✅ No duplicates: cursor guarantees we only fetch items
        with (score, id) < cursor position
     ✅ New articles appear at TOP of feed on next refresh
  
  2. Articles removed/score changed while scrolling:
     ✅ Might skip a few articles (acceptable for news)
     ✅ No duplicates guaranteed
  
  3. User reaches end of feed:
     Return { has_more: false }
     Client shows "You're all caught up" message
  
  4. Cursor becomes invalid (e.g., cache rebuild):
     Return first page of results with fresh cursor
     Client may see some re-ordered articles (acceptable)
```

---

## 11. Breaking News Detection — Detailed Flow

### 11.1 Detection Pipeline

```
FLOW: Breaking News Detection
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Kafka: enriched-articles
     │
     ▼
┌──────────────────────────────────────────────────────────────┐
│  TRENDING DETECTION SERVICE                                   │
│                                                              │
│  STEP 1: Story Clustering (Sliding Window)                   │
│  ─────────────────────────────────────────                   │
│  For each incoming article:                                  │
│    1. Compute SimHash                                        │
│    2. Find existing cluster with Hamming distance ≤ 3        │
│    3. If found: add to cluster, increment count              │
│    4. If not: create new cluster                             │
│                                                              │
│  Clusters maintained in memory with 24-hour sliding window   │
│  Data structure: HashMap<cluster_id, StoryCluster>           │
│                                                              │
│  struct StoryCluster {                                       │
│    id: UUID                                                  │
│    simhash: u64                                              │
│    article_ids: Vec<ArticleId>                               │
│    publisher_ids: HashSet<PublisherId>  // unique sources     │
│    first_seen: Timestamp                                     │
│    articles_per_window: RingBuffer<(Timestamp, Count)>       │
│    regions: HashSet<String>                                   │
│    velocity: f64                                             │
│    status: Normal | Trending | Breaking                      │
│  }                                                           │
│                                                              │
│  STEP 2: Velocity Calculation (every 60 seconds)             │
│  ──────────────────────────────────────────────              │
│  For each active cluster:                                    │
│    velocity = articles_added_last_15_min / 15                │
│    acceleration = (velocity_now - velocity_15min_ago)        │
│    source_count = publisher_ids.len()                        │
│                                                              │
│  STEP 3: Classification                                      │
│  ───────────────────                                         │
│    if velocity > 10/15min AND source_count > 5:              │
│      status = BREAKING                                       │
│      trigger_breaking_news_actions(cluster)                  │
│                                                              │
│    elif velocity > 5/15min AND source_count > 3:             │
│      status = TRENDING                                       │
│      boost_in_feeds(cluster, multiplier=2.0)                 │
│                                                              │
│    elif velocity < 1/15min for 2 hours:                      │
│      status = NORMAL                                         │
│      remove_boost(cluster)                                   │
│                                                              │
│  STEP 4: Breaking News Actions                               │
│  ─────────────────────────────                               │
│  When status changes to BREAKING:                            │
│                                                              │
│    a. BOOST in feeds:                                        │
│       For each affected region:                              │
│         ZADD feed:{region}:general                           │
│           {quality_score × 5.0} {canonical_article_id}       │
│       Score multiplier ensures it appears at top of feed     │
│                                                              │
│    b. INVALIDATE caches:                                     │
│       Invalidate CDN edge for affected regions               │
│       Delete L1 cache entries (broadcast to all instances)   │
│       Refresh L2 Redis feeds                                 │
│                                                              │
│    c. SET breaking banner:                                   │
│       SET breaking:{region} {cluster_json} EX 3600           │
│       Banner shown at top of feed for 1 hour                 │
│                                                              │
│    d. PUSH notifications:                                    │
│       Produce to Kafka 'push-notifications' topic            │
│       Notification service sends via FCM/APNs                │
│       Throttle: max 1 push per user per hour                 │
│                                                              │
│    e. LOG for analytics:                                     │
│       Record breaking news event with metadata               │
│       Used for model improvement and postmortem              │
│                                                              │
└──────────────────────────────────────────────────────────────┘

TIMELINE EXAMPLE:

  T+0:00  Reuters publishes "Earthquake hits Pacific Coast"
  T+0:30  Webhook received, article in Kafka
  T+1:00  Article processed, in DB and Redis feeds
  T+2:00  AP publishes similar article
  T+3:00  CNN, BBC, NYT publish their versions
  T+5:00  10 articles in cluster from 8 publishers
          → velocity = 10/5min = 2.0/min
          → Classified as BREAKING
  T+5:05  Breaking news banner set for US region
  T+5:10  CDN invalidated, feeds refreshed
  T+5:30  Push notification sent to US users
  T+6:00  Users opening app see breaking news at top of feed
  
  Total: Article published → Users notified: ~6 minutes
```

---

## 12. Personalization & Ranking — Detailed Flow

### 12.1 Ranking Pipeline

```
FLOW: Personalized Feed Ranking
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

INPUT: 
  - base_feed: Top 100 articles from regional feed (pre-computed in Redis)
  - user: User object with preferences and interaction history

STEP 1: FETCH USER SIGNALS
  ┌────────────────────────────────────────────────────────────┐
  │ From Redis (cached user profile):                         │
  │   user_prefs = GET user_prefs:{user_id}                   │
  │   {                                                       │
  │     category_weights: {tech: 1.5, politics: 0.8, ...},    │
  │     blocked_publishers: ["pub-tabloid-001"],               │
  │     recently_clicked: [article_id_1, article_id_2, ...],  │
  │     click_history_categories: {                            │
  │       tech: 45, sports: 30, politics: 15, ...             │
  │     },                                                     │
  │     avg_dwell_time_by_category: {                          │
  │       tech: 120s, sports: 45s, politics: 90s              │
  │     }                                                      │
  │   }                                                       │
  └────────────────────────────────────────────────────────────┘

STEP 2: COMPUTE ARTICLE SCORES
  ┌────────────────────────────────────────────────────────────┐
  │ For each article in base_feed:                            │
  │                                                           │
  │   // Signal 1: Recency (30% weight)                       │
  │   hours_old = (now - article.published_at).hours          │
  │   recency = exp(-0.1 × hours_old)                         │
  │   // 1 hour old: 0.90, 6 hours: 0.55, 24 hours: 0.09     │
  │                                                           │
  │   // Signal 2: Content Quality (25% weight)               │
  │   quality = article.quality_score  // pre-computed         │
  │                                                           │
  │   // Signal 3: Category Affinity (20% weight)             │
  │   affinity = 0                                            │
  │   for cat in article.categories:                          │
  │     explicit_weight = user_prefs.category_weights[cat]    │
  │     implicit_weight = user_prefs.click_history[cat] /     │
  │                       total_clicks                        │
  │     dwell_weight = user_prefs.avg_dwell[cat] /            │
  │                    global_avg_dwell[cat]                   │
  │     affinity += 0.4 × explicit + 0.4 × implicit           │
  │                + 0.2 × dwell_weight                       │
  │   affinity = affinity / len(article.categories)           │
  │                                                           │
  │   // Signal 4: Publisher Trust (10% weight)               │
  │   trust = article.publisher.reliability_score             │
  │                                                           │
  │   // Signal 5: Trending Velocity (10% weight)             │
  │   trending = min(1.0, article.trending_score / 10.0)      │
  │                                                           │
  │   // Signal 6: Diversity Bonus (5% weight)                │
  │   diversity = computeDiversityBonus(article, ranked_so_far)│
  │   // Bonus for showing different categories, publishers   │
  │   // Prevents feed from being all tech or all from CNN    │
  │                                                           │
  │   // Final Score                                          │
  │   final_score = 0.30 × recency                            │
  │               + 0.25 × quality                             │
  │               + 0.20 × affinity                            │
  │               + 0.10 × trust                               │
  │               + 0.10 × trending                            │
  │               + 0.05 × diversity                           │
  └────────────────────────────────────────────────────────────┘

STEP 3: FILTER
  ┌────────────────────────────────────────────────────────────┐
  │ Remove:                                                    │
  │   - Articles from blocked publishers                       │
  │   - Articles user already clicked (recently_clicked set)   │
  │   - Duplicate stories (keep highest quality version)       │
  │   - Articles in user's blocked categories (if any)         │
  └────────────────────────────────────────────────────────────┘

STEP 4: DIVERSITY RE-RANKING
  ┌────────────────────────────────────────────────────────────┐
  │ Maximal Marginal Relevance (MMR) approach:                │
  │                                                           │
  │ ranked = []                                               │
  │ candidates = sorted(articles, by=final_score, desc)       │
  │                                                           │
  │ while len(ranked) < limit and candidates not empty:       │
  │   for each candidate:                                     │
  │     // Penalize if same category as recent additions      │
  │     category_penalty = 0                                  │
  │     for recent in ranked[-3:]:  // last 3 added           │
  │       if candidate.categories ∩ recent.categories:        │
  │         category_penalty += 0.1                           │
  │                                                           │
  │     // Penalize if same publisher as recent additions     │
  │     publisher_penalty = 0                                 │
  │     for recent in ranked[-5:]:  // last 5 added           │
  │       if candidate.publisher == recent.publisher:         │
  │         publisher_penalty += 0.15                         │
  │                                                           │
  │     adjusted_score = candidate.final_score                │
  │                    - category_penalty                     │
  │                    - publisher_penalty                    │
  │                                                           │
  │   best = candidate with highest adjusted_score            │
  │   ranked.append(best)                                     │
  │   candidates.remove(best)                                 │
  └────────────────────────────────────────────────────────────┘

STEP 5: RETURN
  Return top {limit} articles from ranked list
  Latency for entire ranking: 10-30ms
```

### 12.2 User Profile Update Flow

```
FLOW: Implicit User Profile Learning
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

User clicks article
  │
  ▼
GET /v1/articles/{id}/click
  │
  ├──▶ 302 Redirect to publisher URL (immediate)
  │
  └──▶ Async: produce to Kafka 'user-interactions'
       {
         user_id, article_id, type: "CLICK",
         timestamp, article_categories, article_publisher
       }
            │
            ▼
       User Interaction Consumer:
         1. UPDATE user click history in Redis:
            HINCRBY user_clicks:{user_id} tech 1
            HINCRBY user_clicks:{user_id} sports 1
         
         2. ADD to recently_clicked set:
            SADD user_recent:{user_id} {article_id}
            EXPIRE user_recent:{user_id} 86400  // 24 hours
         
         3. INSERT into user_interactions table (async batch)
         
         4. Recompute user category weights (every 100 clicks):
            For each category:
              implicit_weight = clicks_in_category / total_clicks
            Normalize weights to sum to 1.0
            Cache in Redis: SET user_prefs:{user_id} {json} EX 86400

Dwell time tracking (client-side):
  Client measures time between click and return to feed
  Sends dwell_time via POST /v1/interactions
  Longer dwell = higher interest signal
  Used in category affinity calculation
```

---

## 13. Media Processing Pipeline — Detailed Flow

```
FLOW: Thumbnail Processing
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Kafka: enriched-articles (contains thumbnail_source_url)
  │
  ▼
┌──────────────────────────────────────────────────────────────┐
│  MEDIA PROCESSING SERVICE                                     │
│                                                              │
│  Step 1: Download source image                               │
│  ───────────────────────────                                 │
│    GET {article.thumbnail_source_url}                        │
│    Timeout: 10 seconds                                       │
│    Max size: 10 MB                                           │
│    Retry: 3 times with exponential backoff                   │
│                                                              │
│    If download fails → use publisher's default logo          │
│    If no thumbnail_source_url → generate text-based thumb    │
│                                                              │
│  Step 2: Validate image                                      │
│  ──────────────────────                                      │
│    - Check MIME type (JPEG, PNG, GIF, WebP only)             │
│    - Check dimensions (minimum 100x100)                      │
│    - Check for corruption (can be decoded)                   │
│    - Optional: NSFW check (ML model, flag if score > 0.8)   │
│                                                              │
│  Step 3: Generate thumbnail variants                         │
│  ──────────────────────────────────                          │
│    Using libvips (fastest image processing library):         │
│                                                              │
│    ┌───────────────┬────────────┬──────────┬────────────┐    │
│    │ Variant       │ Dimensions │ Quality  │ Use Case   │    │
│    ├───────────────┼────────────┼──────────┼────────────┤    │
│    │ small (100px) │ 100×75     │ 70%      │ List view  │    │
│    │ medium (300px)│ 300×200    │ 80%      │ Card view  │    │
│    │ large (600px) │ 600×400    │ 85%      │ Hero/detail│    │
│    └───────────────┴────────────┴──────────┴────────────┘    │
│                                                              │
│    Format: WebP (30% smaller than JPEG at same quality)      │
│    Fallback: JPEG for browsers without WebP support          │
│                                                              │
│  Step 4: Upload to S3                                        │
│  ────────────────────                                        │
│    Bucket structure:                                         │
│      s3://newsfeed-media/{region}/{YYYY/MM/DD}/{article_id}/ │
│        thumb_100.webp                                        │
│        thumb_300.webp                                        │
│        thumb_600.webp                                        │
│                                                              │
│    Set headers:                                              │
│      Content-Type: image/webp                                │
│      Cache-Control: public, max-age=31536000, immutable      │
│      (images are immutable — 1 year cache)                   │
│                                                              │
│  Step 5: Generate CDN URLs                                   │
│  ────────────────────────                                    │
│    CDN URL pattern:                                          │
│      https://cdn.newsfeed.com/thumb/{size}/{article_id}.webp │
│                                                              │
│    CloudFront distribution maps to S3 bucket                 │
│    Edge caching: max-age=31536000 (1 year)                   │
│    Origin Shield: enabled (reduces S3 requests)              │
│                                                              │
│  Step 6: Update article record                               │
│  ─────────────────────────────                               │
│    UPDATE articles SET                                       │
│      thumbnail_cdn_url = 'https://cdn.newsfeed.com/...'      │
│    WHERE id = {article_id}                                   │
│                                                              │
│    Also update Redis article cache:                          │
│    HSET article:{article_id} thumbnail_cdn_url {url}         │
│                                                              │
└──────────────────────────────────────────────────────────────┘

WHY WE STORE OUR OWN THUMBNAILS:

  ❌ Direct linking to publisher images:
     - Publisher servers may be slow (adds latency to our feed)
     - Publisher may change or delete images (broken thumbnails)
     - Publisher servers may go down (no images during outages)
     - Different publishers use wildly different image sizes
     - No control over caching behavior
     - Hot-linking may violate publisher ToS or be blocked
  
  ✅ Our own copies in S3 + CDN:
     - Consistent load times (< 50ms from edge)
     - Standardized sizes for uniform UI
     - WebP format for 30% bandwidth savings
     - Always available (S3: 99.999999999% durability)
     - Full control over caching (1-year immutable cache)
     - Responsive images: serve appropriate size for viewport
```

---

## 14. Search Flow

```
FLOW: Article Search
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Client                Search Service         Elasticsearch
  │                        │                       │
  │  GET /v1/search        │                       │
  │  ?q=climate+change     │                       │
  │  &region=US            │                       │
  │  &category=science     │                       │
  │  &from=2026-11-01      │                       │
  │  &limit=20             │                       │
  │───────────────────────▶│                       │
  │                        │                       │
  │               ┌────────────────────────┐       │
  │               │ 1. Parse & validate    │       │
  │               │ 2. Build ES query:     │       │
  │               │                        │       │
  │               │ {                      │       │
  │               │   "query": {           │       │
  │               │     "bool": {          │       │
  │               │       "must": [{       │       │
  │               │         "multi_match": {│      │
  │               │           "query":      │      │
  │               │            "climate     │      │
  │               │             change",    │      │
  │               │           "fields": [   │      │
  │               │            "title^3",   │      │
  │               │            "summary^2", │      │
  │               │            "entities^2" │      │
  │               │           ]             │      │
  │               │         }               │      │
  │               │       }],               │      │
  │               │       "filter": [       │      │
  │               │         {"term":        │      │
  │               │          {"region":"US"}│      │
  │               │         },              │      │
  │               │         {"term":        │      │
  │               │          {"categories": │      │
  │               │           "science"}},  │      │
  │               │         {"range":       │      │
  │               │          {"published_at"│      │
  │               │           :{"gte":      │      │
  │               │            "2026-11-01" │      │
  │               │         }}}             │      │
  │               │       ]                 │      │
  │               │     }                   │      │
  │               │   },                    │      │
  │               │   "sort": [             │      │
  │               │     "_score",           │      │
  │               │     {"published_at":    │      │
  │               │      "desc"}            │      │
  │               │   ],                    │      │
  │               │   "size": 20            │      │
  │               │ }                       │      │
  │               └────────────────────────┘       │
  │                        │                       │
  │                        │  ES query             │
  │                        │──────────────────────▶│
  │                        │                       │
  │                        │  Search results       │
  │                        │◀──────────────────────│
  │                        │                       │
  │               ┌────────────────────────┐       │
  │               │ 3. Hydrate with full   │       │
  │               │    article metadata    │       │
  │               │    from Redis/DB       │       │
  │               │                        │       │
  │               │ 4. Build response with │       │
  │               │    cursor and total    │       │
  │               └────────────────────────┘       │
  │                        │                       │
  │◀───────────────────────│                       │
  │  200 OK with results   │                       │

ELASTICSEARCH INDEX MAPPING:

  articles_index:
    title:         text (analyzed, english analyzer, boosted ×3)
    summary:       text (analyzed, english analyzer, boosted ×2)
    entities:      keyword[] (exact match, boosted ×2)
    categories:    keyword[] (exact match, for filtering)
    region:        keyword (for filtering)
    publisher_name: keyword (for filtering)
    published_at:  date (for range queries and sorting)
    quality_score: float (for boosting results)
    language:      keyword (for filtering)
    
  Index Settings:
    Shards: 5 per region (parallelism)
    Replicas: 2 (high availability)
    Refresh interval: 30 seconds (near real-time)
    Retention: 6 months (older articles archived)
```

---

## 15. Multi-Region Deployment

### 15.1 Global Architecture

```
                           ┌────────────────────┐
                           │     GeoDNS          │
                           │   (Route 53)        │
                           │                    │
                           │ US users → US PoP  │
                           │ EU users → EU PoP  │
                           │ APAC users → APAC  │
                           └───────┬────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    ▼              ▼              ▼
         ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
         │   US-EAST    │ │   EU-WEST    │ │  APAC-SOUTH  │
         │   REGION     │ │   REGION     │ │   REGION     │
         │              │ │              │ │              │
         │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
         │ │CDN PoPs  │ │ │ │CDN PoPs  │ │ │ │CDN PoPs  │ │
         │ │(10+)     │ │ │ │(8+)      │ │ │ │(12+)     │ │
         │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
         │              │ │              │ │              │
         │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
         │ │API GW ×5 │ │ │ │API GW ×4 │ │ │ │API GW ×6 │ │
         │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
         │              │ │              │ │              │
         │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
         │ │Feed ×20  │ │ │ │Feed ×15  │ │ │ │Feed ×25  │ │
         │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
         │              │ │              │ │              │
         │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
         │ │Redis     │ │ │ │Redis     │ │ │ │Redis     │ │
         │ │Master +  │ │ │ │Master +  │ │ │ │Master +  │ │
         │ │20 replica│ │ │ │15 replica│ │ │ │25 replica│ │
         │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
         │              │ │              │ │              │
         │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
         │ │PG Primary│ │ │ │PG Primary│ │ │ │PG Primary│ │
         │ │+ 3 Read  │ │ │ │+ 3 Read  │ │ │ │+ 4 Read  │ │
         │ │Replicas  │ │ │ │Replicas  │ │ │ │Replicas  │ │
         │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
         │              │ │              │ │              │
         │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
         │ │Kafka     │ │ │ │Kafka     │ │ │ │Kafka     │ │
         │ │Cluster   │◀┼─┼▶│Cluster   │◀┼─┼▶│Cluster   │ │
         │ │(6 nodes) │ │ │ │(6 nodes) │ │ │ │(6 nodes) │ │
         │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
         │              │ │              │ │              │
         │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
         │ │Crawlers  │ │ │ │Crawlers  │ │ │ │Crawlers  │ │
         │ │(US pubs) │ │ │ │(EU pubs) │ │ │ │(APAC pubs)│ │
         │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
         └──────────────┘ └──────────────┘ └──────────────┘
                 │                │                │
                 └────────────────┼────────────────┘
                                  │
                    Kafka MirrorMaker 2
                   (Cross-region article sync)
```

### 15.2 Cross-Region Sync Strategy

```
STRATEGY: Cross-Region Article Distribution
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PRINCIPLE: Each region is SELF-SUFFICIENT for reads.
           No cross-region calls on the hot path (feed serving).
           Cross-region only for data replication (async).

HOW IT WORKS:

  1. CRAWLING is region-aware:
     - US region crawls US publishers (NYT, CNN, WSJ, etc.)
     - EU region crawls EU publishers (BBC, Guardian, Le Monde, etc.)
     - APAC region crawls APAC publishers (Times of India, NHK, etc.)
     
  2. ARTICLE SYNC via Kafka MirrorMaker 2:
     - Each region produces to local 'enriched-articles' topic
     - MirrorMaker replicates to other regions:
       US.enriched-articles → EU Kafka, APAC Kafka
       EU.enriched-articles → US Kafka, APAC Kafka
     - Each region's feed workers consume both local AND remote articles
     
  3. FEED COMPOSITION:
     US region feed might contain:
       70% US publisher articles (local)
       20% EU/APAC articles about US topics (replicated)
       10% International/global stories (replicated)
     
  4. WHAT DOESN'T SYNC:
     - User data stays in user's home region
     - Click analytics stay local (aggregated daily)
     - Cache state is per-region (independent)

BENEFITS:
  ✅ No cross-region latency on reads
  ✅ Regional failures are isolated
  ✅ Each region scales independently
  ✅ Compliance with data residency requirements
  ✅ CDN caching is more effective (regional content)

REPLICATION LAG:
  Kafka MirrorMaker latency: 100-500ms between regions
  Acceptable: users don't notice 500ms delay for international news
```

---

## 16. Resilience & Graceful Degradation

### 16.1 Degradation Levels

```
SYSTEM HEALTH STATES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STATE 1: HEALTHY (Normal Operation)
─────────────────────────────────────
  Conditions: All systems green
  Behavior:
    ✅ Full personalization
    ✅ Fresh feeds from Redis (< 5 min old)
    ✅ Real-time search
    ✅ Breaking news detection active
    ✅ Full analytics tracking

STATE 2: DEGRADED (Partial Failure)
─────────────────────────────────────
  Triggers:
    - Cache miss rate > 20%
    - DB read latency P99 > 100ms
    - Single Redis replica failure
    - Elasticsearch cluster yellow
    
  Automatic Actions:
    ⚠️ Disable personalization (serve generic regional feed)
    ⚠️ Extend cache TTLs (soft: 5min → 15min, hard: 30min → 2hr)
    ⚠️ Reduce feed size (20 articles → 10)
    ⚠️ Disable analytics tracking (reduce write load)
    ⚠️ Enable stale-while-revalidate aggressively
    ⚠️ Circuit breaker on slow dependencies

STATE 3: EMERGENCY (Major Failure)
─────────────────────────────────────
  Triggers:
    - PostgreSQL primary unreachable
    - Redis cluster majority down
    - Kafka cluster unavailable
    
  Automatic Actions:
    🔴 Serve pre-generated static feeds from S3/CDN
    🔴 Static feeds pre-rendered every 15 minutes as JSON files
    🔴 Return 503 with Retry-After for write operations
    🔴 Page on-call team immediately
    🔴 All feeds served from CDN edge (infinite TTL override)

STATIC FALLBACK FEED:
  - Every 15 minutes, a background job:
    1. Generates top 50 articles per region per category
    2. Renders as static JSON files
    3. Uploads to S3
    4. CDN points to these as fallback origin
  - Even if ALL backend services die, users still see a feed
  - Stale but functional > blank page
```

### 16.2 Resilience Patterns

```
PATTERN: Circuit Breaker (on all downstream calls)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  CLOSED ──(failures > threshold)──▶ OPEN
    ▲                                   │
    │                            (timeout expires)
    │                                   │
    │                                   ▼
    └──────(probe succeeds)────── HALF-OPEN

  Config per dependency:
    Redis:    failure_threshold=5,  timeout=30s,  probe_interval=5s
    Postgres: failure_threshold=3,  timeout=60s,  probe_interval=10s
    ES:       failure_threshold=5,  timeout=30s,  probe_interval=5s
    
  When OPEN:
    Redis down → serve from L1 cache or DB fallback
    DB down → serve from Redis cache (stale OK)
    ES down → disable search, return cached popular searches

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PATTERN: Bulkhead (Thread Pool Isolation)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Feed Service thread pools:
    feed_pool:    max=200 threads  (feed assembly)
    search_pool:  max=50 threads   (search queries)
    analytics_pool: max=20 threads (click tracking)
    
  If search is slow → only search_pool saturates
  Feed serving continues unaffected on its own pool

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PATTERN: Stale-Serve with Dual TTL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Redis key: feed:US:tech
    Soft TTL: 5 minutes (after this, trigger background refresh)
    Hard TTL: 30 minutes (after this, data actually expires)

  Flow:
    t=0: Feed cached
    t=5min: Soft TTL expires
      → Serve stale data to current request (fast)
      → Trigger async background refresh (non-blocking)
    t=5min+2s: Background refresh completes, cache updated
    t=30min: Hard TTL expires (only if refresh repeatedly failed)
      → Force DB query on next request

  Result: Users NEVER wait for a cache refresh. They always get
          instant responses, with at most 5-minute-old data.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PATTERN: Rate Limiting (per-publisher crawl)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Token bucket per publisher domain:
    Reuters:  10 requests/minute  (they can handle it)
    Small blog: 1 request/minute  (don't overwhelm them)
    
  Also respect: robots.txt crawl-delay directive
  Also respect: HTTP 429 responses (back off exponentially)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PATTERN: Retry with Exponential Backoff + Jitter
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  delay = min(base × 2^attempt + random_jitter, max_delay)
  
  Attempt 1: 1s + jitter(0-500ms)
  Attempt 2: 2s + jitter(0-500ms)
  Attempt 3: 4s + jitter(0-500ms)
  Max retries: 3
  Max delay: 30s
  
  Jitter prevents thundering herd when many crawlers
  retry the same publisher simultaneously.
```

---

## 17. Capacity Estimation

### 17.1 Traffic Model

```
CAPACITY ESTIMATION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

READS (Feed Serving):

  DAU (peak)                    = 500,000,000
  Avg feed loads/user/day       = 8
  Total feed requests/day       = 4,000,000,000
  Avg QPS                       = 4B / 86,400 ≈ 46,000 QPS
  Peak QPS (5× multiplier)      = 230,000 QPS
  Breaking news spike (10×)     = 460,000 QPS
  
  Per region (5 regions):
    Normal peak QPS/region      = 46,000 QPS
    Breaking news QPS/region    = 92,000 QPS

WRITES (Ingestion):

  Publishers                    = 50,000
  Avg articles/publisher/day    = 10
  New articles/day              = 500,000
  Articles/second               = ~6/second
  Peak articles/second          = ~30/second (breaking news)
  
  Read:Write ratio              = 230,000 : 6 ≈ 38,000:1
  → Extreme read-heavy workload. Cache everything.

CLICK-THROUGHS:

  Click-through rate            = ~5% of viewed articles
  Articles viewed per feed load = 20
  Clicks per feed load          = 1
  Total clicks/day              = 4B × 1 / 8 = 500M clicks
  Click tracking QPS            = ~6,000 QPS (async, batchable)
```

### 17.2 Storage Estimates

```
STORAGE ESTIMATES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

POSTGRESQL:

  Articles table:
    500K articles/day × 2 KB = 1 GB/day
    365 GB/year × 2 year retention = 730 GB
    With indexes: ~1.5 TB total
    
  User interactions:
    500M clicks/day × 100 bytes = 50 GB/day
    90 day retention = 4.5 TB
    After that: aggregate and archive
    
  Users table:
    500M users × 500 bytes = 250 GB (mostly static)
    
  Total PostgreSQL: ~6 TB per region (with replicas: ~24 TB)

REDIS:

  Feed sorted sets:
    15 regions × 25 categories = 375 feeds
    Each feed: 2000 article IDs × 8 bytes = 16 KB
    Total feed data: 375 × 16 KB = 6 MB
    
  Article metadata cache:
    500K articles × 2 KB = 1 GB (last 24 hours)
    Older articles: expire after 48 hours
    
  User profiles:
    Active users (10% of 500M): 50M × 500 bytes = 25 GB
    
  Bloom filter (URL dedup):
    50M URLs, 0.01% FP rate = 60 MB
    
  Total Redis: ~30 GB per region
  With replicas (20 per region): ~600 GB total RAM

S3:

  Thumbnails:
    500K articles/day × 3 sizes × 50 KB = 75 GB/day
    1 year retention = 27 TB
    
  Static fallback feeds:
    5 regions × 25 categories × 100 KB = 12.5 MB per snapshot
    Every 15 minutes: negligible

ELASTICSEARCH:

  Article index:
    500K articles/day × 3 KB (indexed fields) = 1.5 GB/day
    6 month retention = 270 GB
    With replicas (2×): 810 GB
    Total per region: ~1 TB
```

### 17.3 Compute Estimates

```
COMPUTE ESTIMATES (Per Region)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FEED SERVICE:
  Peak QPS: 46,000 per region
  Per instance: ~2,000 QPS (Go, 8 vCPU)
  Instances needed: 46,000 / 2,000 = 23 → round to 25
  Breaking news: scale to 50 instances
  
  Sizing: 8 vCPU, 16 GB RAM
  Auto-scale trigger: CPU > 60% or P99 > 100ms

RANKING SERVICE:
  Called for personalized feeds only (~30% of requests)
  Peak: 14,000 QPS
  Per instance: ~1,000 QPS (ML inference)
  Instances: 14 → round to 15
  
  Sizing: 8 vCPU, 32 GB RAM (model in memory)

CRAWLER FLEET:
  50,000 publishers with varying intervals
  High-pri (5 min): 1,000 pubs → 200 crawls/min → 3 instances
  Mid-pri (15 min): 10,000 pubs → 667 crawls/min → 10 instances
  Low-pri (2 hr): 39,000 pubs → 325 crawls/min → 5 instances
  Total: ~20 crawler instances
  
  Sizing: 4 vCPU, 8 GB RAM

NLP PIPELINE:
  Articles/second: ~6 (normal), ~30 (peak)
  Processing time: ~50ms per article
  Instances: 5 (normal), 10 (peak)
  
  Sizing: 8 vCPU, 16 GB RAM (or 4 vCPU + 1 GPU)

REDIS CLUSTER:
  Peak read QPS: 46,000
  Per replica: ~100,000 QPS (simple ZRANGE)
  Replicas needed: ceil(46,000 / 100,000) = 1
  But for HA and headroom: 10-20 replicas
  
  Sizing: 32 GB RAM each

POSTGRESQL:
  Read QPS (after cache): ~2,300 (1% of total)
  Per read replica: ~5,000 QPS
  Replicas needed: 1 (but 3 for HA)
  
  Sizing: 64 vCPU, 256 GB RAM, NVMe SSD
```

---

## 18. Database Indexing & Query Strategy

```
POSTGRESQL INDEXES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- Primary feed query (cursor-based pagination)
CREATE INDEX idx_articles_feed 
  ON articles (region, status, quality_score DESC, id DESC)
  WHERE status = 'ACTIVE';

-- Category feed query
CREATE INDEX idx_articles_category 
  ON article_categories (category_slug, article_id DESC)
  INCLUDE (confidence);

-- Publisher lookup
CREATE INDEX idx_articles_publisher 
  ON articles (publisher_id, published_at DESC);

-- Deduplication lookup (SimHash)
CREATE INDEX idx_articles_simhash 
  ON articles (simhash)
  WHERE status = 'ACTIVE' 
  AND discovered_at > NOW() - INTERVAL '48 hours';

-- URL uniqueness (prevent duplicate ingestion)
CREATE UNIQUE INDEX idx_articles_url 
  ON articles (original_url);

-- Content hash (exact duplicate check)
CREATE INDEX idx_articles_content_hash 
  ON articles (content_hash);

-- Full-text search (PostgreSQL native, backup for ES)
CREATE INDEX idx_articles_search 
  ON articles USING GIN (search_vector);

-- User interaction queries
CREATE INDEX idx_interactions_user 
  ON user_interactions (user_id, created_at DESC);

CREATE INDEX idx_interactions_article 
  ON user_interactions (article_id, type);

-- Publisher scheduling
CREATE INDEX idx_publishers_schedule 
  ON publishers (is_active, priority, last_scraped_at);

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

QUERY PATTERNS:

1. FEED QUERY (rare - only on cache miss):
   
   SELECT a.id, a.title, a.summary, a.thumbnail_cdn_url,
          a.published_at, a.original_url, a.categories,
          p.name as publisher_name, p.id as publisher_id
   FROM articles a
   JOIN publishers p ON a.publisher_id = p.id
   WHERE a.region = 'US'
     AND a.status = 'ACTIVE'
     AND (a.quality_score, a.id) < (:last_score, :last_id)
   ORDER BY a.quality_score DESC, a.id DESC
   LIMIT 20;
   
   Uses: idx_articles_feed
   Expected: < 10ms with index

2. CATEGORY FEED QUERY:
   
   SELECT a.*
   FROM articles a
   JOIN article_categories ac ON a.id = ac.article_id
   WHERE ac.category_slug = 'tech'
     AND a.region = 'US'
     AND a.status = 'ACTIVE'
   ORDER BY a.quality_score DESC, a.id DESC
   LIMIT 20;

3. DEDUP QUERY:
   
   SELECT id, simhash
   FROM articles
   WHERE status = 'ACTIVE'
     AND discovered_at > NOW() - INTERVAL '48 hours'
     AND simhash BETWEEN :lower_bound AND :upper_bound;
   
   -- Note: actual SimHash lookup uses Redis for speed (see dedup section)

TABLE PARTITIONING:
  
  -- Partition articles by month for efficient cleanup
  CREATE TABLE articles (
    ...
  ) PARTITION BY RANGE (discovered_at);
  
  CREATE TABLE articles_2026_11 PARTITION OF articles
    FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
  
  -- Drop old partitions instead of DELETE (instant, no vacuum)
  DROP TABLE articles_2024_11;  -- 2-year retention
```

---

## 19. Kafka Topic Design

```
KAFKA TOPIC DESIGN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TOPIC 1: raw-articles
  Purpose:    Raw articles from crawlers and webhooks
  Key:        publisher_id (ordering per publisher)
  Partitions: 32
  Retention:  7 days
  Consumers:  Deduplication Service (consumer group: dedup-cg)
  Throughput: ~30 msgs/sec peak

TOPIC 2: unique-articles
  Purpose:    Articles that passed dedup check
  Key:        article_id
  Partitions: 32
  Retention:  7 days
  Consumers:  NLP Pipeline (consumer group: nlp-cg)
  Throughput: ~25 msgs/sec (some filtered by dedup)

TOPIC 3: enriched-articles
  Purpose:    Fully processed articles with categories, entities, scores
  Key:        article_id
  Partitions: 32
  Retention:  7 days
  Consumers:
    - DB Writer (consumer group: db-writer-cg)
    - Trending Detection (consumer group: trending-cg)
    - Media Processing (consumer group: media-cg)
    - MirrorMaker 2 (cross-region replication)
  Throughput: ~25 msgs/sec

TOPIC 4: db-changes (CDC)
  Purpose:    PostgreSQL WAL changes captured by Debezium
  Key:        table_name + primary_key
  Partitions: 16
  Retention:  3 days
  Consumers:  Feed Pre-computation Workers (consumer group: feed-cg)
  Throughput: ~50 msgs/sec (includes all table changes)

TOPIC 5: user-interactions
  Purpose:    Click events, dwell time, shares
  Key:        user_id
  Partitions: 64 (higher volume)
  Retention:  7 days
  Consumers:
    - User Profile Updater (consumer group: profile-cg)
    - Analytics Pipeline (consumer group: analytics-cg)
  Throughput: ~6,000 msgs/sec peak

TOPIC 6: push-notifications
  Purpose:    Breaking news push notification requests
  Key:        region
  Partitions: 16
  Retention:  1 day
  Consumers:  Notification Service (consumer group: notif-cg)
  Throughput: Bursty (0 normally, 10K+ during breaking news)

TOPIC 7: dead-letter-queue
  Purpose:    Failed messages from any consumer
  Key:        original_topic + original_key
  Partitions: 8
  Retention:  30 days
  Consumers:  Ops team (manual review)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

KAFKA CLUSTER SIZING (per region):
  Brokers: 6
  Sizing: 16 vCPU, 64 GB RAM, 2 TB SSD each
  Replication factor: 3
  Min ISR: 2
  Total storage: 12 TB per region
```

---

## 20. Redis Data Structure Design

```
REDIS DATA STRUCTURES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. FEED SORTED SETS (pre-computed feeds)
─────────────────────────────────────────
  Key:    feed:{region}:{category}
  Type:   Sorted Set (ZSET)
  Score:  quality_score (float, 0.0 - 1.0)
  Member: article_id (string)
  Size:   Max 2000 members per set
  TTL:    None (explicitly managed by feed workers)
  
  Example:
    feed:US:general → {
      (0.95, "1823749182400"),  // highest quality article
      (0.92, "1823749182398"),
      (0.87, "1823749182374"),
      ...
      (0.15, "1823749180001")   // 2000th article
    }
  
  Operations:
    ZADD feed:US:tech 0.87 1823749182374            // Add article
    ZREVRANGE feed:US:tech 0 19 WITHSCORES          // Get top 20
    ZREVRANGEBYSCORE feed:US:tech 0.72 -inf LIMIT 0 20  // Cursor page
    ZREMRANGEBYRANK feed:US:tech 0 -2001            // Trim to 2000
    ZCARD feed:US:tech                               // Count articles

2. ARTICLE METADATA CACHE (hydration cache)
─────────────────────────────────────────────
  Key:    article:{article_id}
  Type:   String (JSON)
  TTL:    86400 (24 hours)
  Size:   ~2 KB per article
  
  Example:
    article:1823749182374 → {
      "id": "1823749182374",
      "title": "Global Climate Summit...",
      "summary": "World leaders have...",
      "publisher": {"id": "pub-reuters", "name": "Reuters"},
      "thumbnail_cdn_url": "https://cdn.newsfeed.com/...",
      "published_at": "2026-11-18T16:30:00Z",
      "original_url": "https://reuters.com/...",
      "categories": ["politics", "environment"],
      "quality_score": 0.87,
      "source_count": 47,
      "is_breaking": false
    }
  
  Operations:
    SET article:1823749182374 '{json}' EX 86400     // Set with TTL
    MGET article:182... article:182... ...          // Batch fetch

3. USER PROFILES (personalization cache)
─────────────────────────────────────────
  Key:    user_prefs:{user_id}
  Type:   String (JSON)
  TTL:    86400 (24 hours)
  
  Key:    user_clicks:{user_id}
  Type:   Hash (category → click count)
  TTL:    2592000 (30 days)
  
  Key:    user_recent:{user_id}
  Type:   Set (recently clicked article IDs)
  TTL:    86400 (24 hours)
  
  Operations:
    GET user_prefs:user-abc123
    HINCRBY user_clicks:user-abc123 tech 1
    SADD user_recent:user-abc123 1823749182374
    SISMEMBER user_recent:user-abc123 1823749182374

4. PUBLISHER SCHEDULING QUEUE
──────────────────────────────
  Key:    publisher_queue
  Type:   Sorted Set
  Score:  next_scrape_timestamp (Unix epoch)
  Member: publisher_id
  
  Operations:
    ZADD publisher_queue 1732000000 pub-reuters-001
    ZRANGEBYSCORE publisher_queue 0 {now} LIMIT 0 100  // Due publishers
    ZREM publisher_queue pub-reuters-001                // Remove when processing

5. BLOOM FILTER (URL deduplication)
────────────────────────────────────
  Key:    article_urls
  Type:   Bloom Filter (RedisBloom module)
  Config: Expected 50M items, 0.01% FP rate
  Size:   ~60 MB
  
  Operations:
    BF.ADD article_urls {sha256_of_url}
    BF.EXISTS article_urls {sha256_of_url}

6. BREAKING NEWS BANNER
────────────────────────
  Key:    breaking:{region}
  Type:   String (JSON)
  TTL:    3600 (1 hour)
  
  Example:
    breaking:US → {
      "cluster_id": "cluster-xyz",
      "headline": "6.4 Earthquake...",
      "severity": "HIGH",
      "lead_article_id": "1823749182400",
      "source_count": 127,
      "updated_at": "2026-11-18T15:15:00Z"
    }

7. RATE LIMITING COUNTERS
──────────────────────────
  Key:    ratelimit:{client_ip}:{minute}
  Type:   String (counter)
  TTL:    60
  
  Key:    webhook_rate:{publisher_id}:{hour}
  Type:   String (counter)
  TTL:    3600

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

REDIS MEMORY BREAKDOWN (per region):
  Feed sorted sets:        6 MB
  Article metadata:        1-2 GB (last 24-48 hours)
  User profiles:           10-25 GB (active users)
  Bloom filter:            60 MB
  Publisher queue:          500 KB
  Breaking news:           10 KB
  Rate limiting:           100 MB
  ────────────────────────────────
  Total per master:        ~28 GB
  Recommended instance:    32 GB RAM (r6g.xlarge)
```

---

## 21. Monitoring & Observability

```
MONITORING STACK
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TOOLS:
  Metrics:     Prometheus + Grafana
  Logging:     Structured JSON → Fluentd → Elasticsearch → Kibana
  Tracing:     OpenTelemetry → Jaeger
  Alerting:    PagerDuty (P1), Slack (P2-P4)

KEY SLOs:
  ┌──────────────────────────────────┬────────┬───────────────┐
  │ SLO                              │ Target │ Alert Threshold│
  ├──────────────────────────────────┼────────┼───────────────┤
  │ Feed API availability            │ 99.99% │ < 99.95%      │
  │ Feed API latency P50             │ < 50ms │ > 75ms        │
  │ Feed API latency P99             │ < 200ms│ > 300ms       │
  │ Article discovery freshness      │ < 5min │ > 10min       │
  │ Cache hit rate (L2)              │ > 95%  │ < 90%         │
  │ Error rate (5xx)                 │ < 0.1% │ > 0.5%        │
  │ Kafka consumer lag               │ < 1000 │ > 5000        │
  │ DB replication lag               │ < 1s   │ > 5s          │
  └──────────────────────────────────┴────────┴───────────────┘

KEY METRICS TO TRACK:

  Feed Service:
    - feed_requests_total (counter, by region, category, status)
    - feed_latency_seconds (histogram, by cache_tier_hit)
    - feed_articles_returned (histogram)
    - personalization_applied (counter, yes/no)

  Ingestion:
    - articles_ingested_total (counter, by publisher, priority)
    - articles_deduplicated_total (counter)
    - crawl_errors_total (counter, by publisher, error_type)
    - crawl_latency_seconds (histogram, by publisher_priority)
    - publisher_queue_depth (gauge)
  
  Cache:
    - cache_hits_total (counter, by tier: L0/L1/L2)
    - cache_misses_total (counter, by tier)
    - cache_hit_rate (gauge, by tier)
    - redis_memory_used_bytes (gauge)
    - redis_connected_replicas (gauge)
  
  Breaking News:
    - breaking_news_detected_total (counter, by region)
    - time_to_detect_seconds (histogram)
    - push_notifications_sent_total (counter, by region)

DASHBOARDS:
  1. System Overview — all SLOs, traffic, error rates
  2. Feed Performance — latency percentiles, cache hit rates
  3. Ingestion Health — articles/sec, dedup rate, errors
  4. Regional View — per-region traffic, latency, capacity
  5. Breaking News — detection timeline, notification delivery
  6. Infrastructure — CPU, memory, disk, network per service
```

---

## 22. Tech Stack Rationale

```
TECH STACK DECISIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

┌───────────────┬────────────────┬──────────────────────────────────────────┐
│ Layer         │ Technology     │ Why This Over Alternatives               │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Feed Service  │ Go             │ High concurrency (goroutines), low GC    │
│               │                │ pause, 50K+ conn/instance, tiny binary.  │
│               │                │ Alt: Java/Kotlin (heavier, GC pauses)    │
│               │                │ Alt: Rust (complexity not justified)     │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ NLP Pipeline  │ Python +       │ ML ecosystem (transformers, spaCy,       │
│               │ FastAPI        │ scikit-learn). Not on hot path so        │
│               │                │ Python's speed is acceptable.            │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Crawlers      │ Go + Colly     │ Efficient HTTP client, goroutine per     │
│               │                │ crawl, handles thousands of concurrent   │
│               │                │ requests with minimal memory.            │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Message Broker│ Apache Kafka   │ Durable, ordered, replayable. Perfect    │
│               │                │ for event-driven pipeline.               │
│               │                │ Alt: RabbitMQ (no replay, lower volume)  │
│               │                │ Alt: Pulsar (less mature ecosystem)      │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Cache         │ Redis 7.x      │ Sorted sets (ZSET) are PERFECT for      │
│               │                │ pre-computed feeds. Sub-ms latency.      │
│               │                │ Alt: Memcached (no sorted sets)          │
│               │                │ Alt: KeyDB (less battle-tested)          │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Primary DB    │ PostgreSQL 16  │ JSONB for flexible metadata, tsvector    │
│               │                │ for backup FTS, pgvector for embeddings, │
│               │                │ table partitioning, logical replication. │
│               │                │ Alt: MySQL (less flexible types)         │
│               │                │ Alt: Cassandra (overkill, no joins)      │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Search        │ Elasticsearch  │ Full-text search, faceting, boosting,    │
│               │  8.x           │ auto-complete. Industry standard.        │
│               │                │ Alt: OpenSearch (fork, compatible)       │
│               │                │ Alt: Meilisearch (simpler, less scale)   │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Object Store  │ S3 + CloudFront│ 11-nines durability, CDN edge caching.  │
│               │                │ Alt: GCS + Cloud CDN (GCP equivalent)   │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ CDC           │ Debezium       │ WAL-based CDC, Kafka-native, proven at   │
│               │                │ scale. Captures every DB change.         │
│               │                │ Alt: pg_logical (more manual)            │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Orchestration │ Kubernetes     │ Auto-scaling, rolling deploys, health    │
│               │  (EKS)         │ checks, service mesh (Istio optional).   │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Observability │ Prometheus +   │ Industry standard. Prometheus for TSDB,  │
│               │ Grafana +      │ Grafana for dashboards, Jaeger for       │
│               │ Jaeger         │ distributed tracing.                     │
├───────────────┼────────────────┼──────────────────────────────────────────┤
│ Client        │ Next.js (web)  │ SSR for SEO (critical for news), React  │
│               │ React Native   │ component reuse across platforms.        │
│               │  (mobile)      │ Alt: Flutter (less SEO-friendly)         │
└───────────────┴────────────────┴──────────────────────────────────────────┘
```

---

## 23. Improvements Over the Original Design

```
IMPROVEMENT SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

┌────┬──────────────────────┬──────────────────────┬──────────────────────────┐
│ #  │ Area                 │ Original Design      │ This Design              │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 1  │ Ingestion frequency  │ Fixed 3-6 hour poll  │ Adaptive 5min-6hr based  │
│    │                      │                      │ on publisher priority,   │
│    │                      │                      │ activity, time-of-day    │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 2  │ Ingestion methods    │ RSS only             │ RSS + Webhooks + Web     │
│    │                      │                      │ scraping (hybrid)        │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 3  │ Deduplication        │ Not addressed        │ SimHash fingerprinting   │
│    │                      │                      │ with Hamming distance,   │
│    │                      │                      │ story clustering         │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 4  │ Content enrichment   │ None                 │ Full NLP pipeline:       │
│    │                      │                      │ language detection,      │
│    │                      │                      │ categorization, NER,     │
│    │                      │                      │ embeddings, quality      │
│    │                      │                      │ scoring                  │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 5  │ Caching              │ Single Redis         │ 4-tier: CDN Edge (L0) →  │
│    │                      │                      │ In-process (L1) → Redis  │
│    │                      │                      │ Cluster (L2) → DB (L3)  │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 6  │ Cache invalidation   │ Not specified        │ CDC (Debezium) → Kafka → │
│    │                      │                      │ Workers. Guaranteed,     │
│    │                      │                      │ ordered, replayable      │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 7  │ Feed strategy        │ Fan-out on read      │ Hybrid: pre-computed     │
│    │                      │ (query DB per req)   │ regional feeds (fan-out  │
│    │                      │                      │ on write) + read-time    │
│    │                      │                      │ personalization overlay  │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 8  │ Breaking news        │ Not addressed        │ Velocity-based detection │
│    │                      │                      │ with auto-boost, CDN     │
│    │                      │                      │ invalidation, push       │
│    │                      │                      │ notifications            │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 9  │ URL dedup on ingest  │ Not addressed        │ Bloom filter (60MB,      │
│    │                      │                      │ 0.01% FP) prevents       │
│    │                      │                      │ re-crawling known URLs   │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 10 │ Personalization      │ Basic category prefs │ Multi-signal ranking:    │
│    │                      │                      │ recency, quality,        │
│    │                      │                      │ affinity, trust,         │
│    │                      │                      │ trending, diversity.     │
│    │                      │                      │ MMR for diverse results  │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 11 │ Resilience           │ Not addressed        │ Circuit breakers,        │
│    │                      │                      │ bulkheads, stale-serve   │
│    │                      │                      │ with dual TTL, static    │
│    │                      │                      │ fallback feeds, 3-level  │
│    │                      │                      │ degradation              │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 12 │ Global deployment    │ Single region implied│ Multi-region active-     │
│    │                      │                      │ active with Kafka        │
│    │                      │                      │ MirrorMaker cross-region │
│    │                      │                      │ sync. No cross-region    │
│    │                      │                      │ reads on hot path.       │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 13 │ Event backbone       │ Direct service calls │ Kafka-based event-driven │
│    │                      │                      │ architecture. Durable,   │
│    │                      │                      │ decoupled, replayable.   │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 14 │ Monitoring           │ Out of scope         │ Full observability:      │
│    │                      │                      │ Prometheus, Grafana,     │
│    │                      │                      │ Jaeger, SLOs with        │
│    │                      │                      │ alerting thresholds      │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 15 │ Article IDs          │ Not specified         │ Snowflake IDs: monotonic,│
│    │                      │                      │ distributed, time-       │
│    │                      │                      │ encoded, 64-bit.         │
│    │                      │                      │ Enables efficient cursor │
│    │                      │                      │ pagination               │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 16 │ Media processing     │ Basic S3 storage     │ 3-size WebP thumbnails,  │
│    │                      │                      │ async pipeline, CDN with │
│    │                      │                      │ 1-year immutable cache,  │
│    │                      │                      │ Origin Shield            │
├────┼──────────────────────┼──────────────────────┼──────────────────────────┤
│ 17 │ Search               │ Not addressed        │ Elasticsearch with       │
│    │                      │                      │ boosted multi-match,     │
│    │                      │                      │ entity search, faceting  │
└────┴──────────────────────┴──────────────────────┴──────────────────────────┘
```

---

## 24. Architectural Trade-offs & Decisions

```
KEY TRADE-OFFS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

DECISION 1: Fan-Out on Write (Hybrid) vs Pure Fan-Out on Read
─────────────────────────────────────────────────────────────
  Chose: Hybrid (pre-compute regional, personalize at read time)
  
  Trade-off:
    ✅ 375 regional feeds are cheap to pre-compute (~18 Redis ops/sec)
    ✅ Read path is mostly cache lookups (sub-10ms)
    ✅ Personalization is lightweight overlay (10-30ms)
    ❌ Additional complexity in feed workers
    ❌ Small delay (2-5 seconds) between article insert and feed update
  
  Alternative rejected: Pure fan-out on read
    Would mean every feed request queries the DB
    At 230K QPS, DB would need 100+ read replicas just for feeds

DECISION 2: PostgreSQL vs Cassandra for articles
─────────────────────────────────────────────────
  Chose: PostgreSQL
  
  Trade-off:
    ✅ Rich indexing (B-tree, GIN, GiST, pgvector)
    ✅ JSONB for flexible metadata
    ✅ JOIN support for publisher lookups
    ✅ ACID transactions for data integrity
    ✅ Mature ecosystem, easy to operate
    ❌ Harder to scale writes horizontally (but we have ~6 writes/sec — trivial)
    ❌ Read replicas add operational overhead
  
  Alternative rejected: Cassandra
    Overkill for our write volume. No JOINs, no rich indexing.
    Only justified at millions of writes/sec.

DECISION 3: Redis Sorted Sets vs Pre-computed DB tables for feeds
─────────────────────────────────────────────────────────────────
  Chose: Redis Sorted Sets
  
  Trade-off:
    ✅ Sub-ms sorted range queries (ZREVRANGE)
    ✅ Atomic score updates (ZADD)
    ✅ Built-in trimming (ZREMRANGEBYRANK)
    ✅ Perfect cursor pagination with ZREVRANGEBYSCORE
    ❌ Data loss risk if Redis crashes (mitigated by replicas + DB fallback)
    ❌ Additional infrastructure to manage
  
  Alternative rejected: Materialized views in PostgreSQL
    Slower refresh cycles, can't handle 230K QPS without massive replica count

DECISION 4: Kafka vs Direct Service-to-Service Calls
─────────────────────────────────────────────────────
  Chose: Kafka as event backbone
  
  Trade-off:
    ✅ Decoupled services (crawler doesn't know about NLP pipeline)
    ✅ Durable — messages survive service restarts
    ✅ Replayable — can reprocess all articles if NLP model improves
    ✅ Backpressure — slow consumers don't crash fast producers
    ✅ Cross-region replication built-in (MirrorMaker)
    ❌ Additional infrastructure (6-12 brokers per region)
    ❌ Operational complexity (partition management, consumer groups)
    ❌ Adds latency (~10-50ms per hop)
  
  Alternative rejected: Direct REST/gRPC calls
    Tight coupling. If NLP service is down, crawlers fail.
    No replay capability. No cross-region data sharing.

DECISION 5: SimHash vs MinHash vs Exact Title Match for dedup
─────────────────────────────────────────────────────────────
  Chose: SimHash with Hamming distance
  
  Trade-off:
    ✅ Fixed-size fingerprint (64 bits) — O(1) comparison
    ✅ Tolerant of minor rewording (Hamming distance ≤ 3)
    ✅ Efficient lookup via bit-sampling tables
    ❌ False positives possible (mitigated by tight threshold)
    ❌ Doesn't capture semantic similarity (only lexical)
  
  Alternative rejected: MinHash + LSH
    Higher accuracy for near-duplicate detection
    But much more expensive (variable-length signatures)
    Overkill for news titles which are typically very similar

DECISION 6: Multi-Region Active-Active vs Active-Passive
─────────────────────────────────────────────────────────
  Chose: Active-Active (each region serves its own traffic)
  
  Trade-off:
    ✅ Lowest latency (users hit nearest region)
    ✅ Regional fault isolation
    ✅ Independent scaling per region
    ✅ Data sovereignty compliance
    ❌ Cross-region consistency is eventual (100-500ms lag)
    ❌ More infrastructure to manage (3× of single region)
    ❌ Complex deployment coordination
  
  Alternative rejected: Active-Passive
    Higher latency for non-primary region users
    Failover is disruptive and slow
    Wastes passive region resources

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

FINAL NOTES:

  This design handles the stated requirements with significant headroom.
  
  The key insight is that news aggregation is an EXTREME read-heavy workload
  (38,000:1 read-to-write ratio) where the content is NATURALLY regional.
  
  By pre-computing regional feeds into Redis sorted sets and layering
  a 4-tier cache hierarchy, we push 99% of reads to cache and
  only ~2,300 QPS ever reaches PostgreSQL.
  
  The deduplication pipeline is the most under-appreciated component:
  without it, breaking news would show 200 identical stories.
  SimHash makes this efficient at scale.
  
  The event-driven Kafka backbone provides durability, decoupling,
  and cross-region replication — three critical properties for a
  globally distributed system.
```

---

*End of HLD Document*
