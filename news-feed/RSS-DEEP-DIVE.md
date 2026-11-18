# RSS in the News Feed Aggregator — How It Works End-to-End

---

## 1. What Is RSS?

RSS (Really Simple Syndication) is an **XML-based format** that publishers use to broadcast their latest content. Think of it as a **machine-readable table of contents** that a publisher keeps updated — whenever they publish a new article, they add an entry to their RSS feed.

It's just a URL that returns XML. That's it. No authentication, no API keys, no SDK — just a plain HTTP GET request.

```
You (aggregator)                        Publisher (e.g., BBC)
      │                                        │
      │   GET https://feeds.bbci.co.uk/        │
      │       news/rss.xml                     │
      │───────────────────────────────────────▶│
      │                                        │
      │   200 OK                               │
      │   Content-Type: application/rss+xml    │
      │   Body: <rss> ... XML ... </rss>       │
      │◀───────────────────────────────────────│
      │                                        │
      │   You parse the XML, extract articles  │
      │   Store them in your database          │
```

**Publishers don't need to do anything special for us.** Most news sites already have RSS feeds — they've been around since 1999. We simply discover their feed URLs and poll them.

---

## 2. What an RSS Feed Looks Like

Here's a real-world RSS feed response (simplified):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:media="http://search.yahoo.com/mrss/">
  
  <!-- CHANNEL: Metadata about the publisher/feed -->
  <channel>
    <title>BBC News - World</title>
    <link>https://www.bbc.co.uk/news/world</link>
    <description>BBC News - World</description>
    <language>en-gb</language>
    <lastBuildDate>Mon, 18 Nov 2026 16:30:00 GMT</lastBuildDate>
    <ttl>15</ttl>  <!-- Publisher's suggested poll interval in minutes -->
    
    <image>
      <url>https://news.bbci.co.uk/nol/shared/img/bbc_news_120x60.gif</url>
      <title>BBC News</title>
      <link>https://www.bbc.co.uk/news</link>
    </image>

    <!-- ITEM: Each <item> is one article -->
    <item>
      <title>Global Climate Summit Reaches Historic Agreement</title>
      <description>
        World leaders have agreed to a landmark deal that sets 
        binding emissions targets for the first time since...
      </description>
      <link>https://www.bbc.co.uk/news/world-67890123</link>
      <guid isPermaLink="true">https://www.bbc.co.uk/news/world-67890123</guid>
      <pubDate>Mon, 18 Nov 2026 14:22:00 GMT</pubDate>
      <category>Environment</category>
      <category>Politics</category>
      <media:thumbnail url="https://ichef.bbci.co.uk/news/1024/cpsprodpb/climate.jpg" 
                       width="1024" height="576"/>
    </item>

    <item>
      <title>Tech Giant Announces Revolutionary AI Chip</title>
      <description>
        The new processor promises 10x performance improvement 
        for machine learning workloads...
      </description>
      <link>https://www.bbc.co.uk/news/technology-67890456</link>
      <guid isPermaLink="true">https://www.bbc.co.uk/news/technology-67890456</guid>
      <pubDate>Mon, 18 Nov 2026 12:15:00 GMT</pubDate>
      <category>Technology</category>
      <media:thumbnail url="https://ichef.bbci.co.uk/news/1024/cpsprodpb/chip.jpg" 
                       width="1024" height="576"/>
    </item>

    <!-- ... 20-50 more <item> entries ... -->
    
  </channel>
</rss>
```

### What we extract from each `<item>`:

```
┌──────────────────┬──────────────────────────────────────────────┐
│ RSS Field        │ What We Store                                │
├──────────────────┼──────────────────────────────────────────────┤
│ <title>          │ article.title                                │
│ <description>    │ article.summary (truncated to 200 chars)     │
│ <link>           │ article.original_url (where user clicks to)  │
│ <guid>           │ Unique ID for dedup (avoid re-ingesting)     │
│ <pubDate>        │ article.published_at                         │
│ <category>       │ Hint for our NLP categorization              │
│ <media:thumbnail>│ Source image URL → we download & reprocess   │
└──────────────────┴──────────────────────────────────────────────┘
```

---

## 3. RSS vs Atom (the other format)

Some publishers use **Atom** instead of RSS. It's a newer, cleaner XML format but serves the same purpose. Our crawler handles both.

```xml
<!-- ATOM format example (e.g., used by many Google services, some blogs) -->
<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>The New York Times</title>
  <link href="https://www.nytimes.com" />
  <updated>2026-11-18T16:30:00Z</updated>
  
  <entry>
    <title>Climate Summit Agreement</title>
    <link href="https://www.nytimes.com/2026/11/18/world/climate.html" />
    <id>urn:uuid:1225c695-cfb8-4ebb-aaaa-80da344efa6a</id>
    <updated>2026-11-18T14:22:00Z</updated>
    <summary>World leaders have agreed...</summary>
    <author><name>Jane Smith</name></author>
  </entry>
</feed>
```

```
Differences that matter to us:

  RSS                              Atom
  ───────────────────────          ───────────────────────
  <item>                           <entry>
  <title>                          <title>
  <description>                    <summary> or <content>
  <link>text</link>                <link href="..." />
  <guid>                           <id>
  <pubDate> (RFC 822)              <updated> (ISO 8601)
  
  Our parser normalizes both into the same internal Article struct.
```

---

## 4. The Complete Setup — What We Build

### 4.1 Publisher Registry (our database of RSS sources)

Before we can crawl anything, we need to know **which publishers exist and where their feeds are**. This is our `publishers` table:

```
HOW WE DISCOVER RSS FEED URLs:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

METHOD 1: Manual Curation (initial seed list)
  - Ops team manually adds top 500 publishers
  - Find RSS URLs from publisher websites
  - Most have /rss, /feed, or /rss.xml paths
  
  Examples:
    BBC:        https://feeds.bbci.co.uk/news/rss.xml
    Reuters:    https://www.reutersagency.com/feed/
    CNN:        http://rss.cnn.com/rss/edition.rss
    NYT:        https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml
    TechCrunch: https://techcrunch.com/feed/
    ESPN:       https://www.espn.com/espn/rss/news

METHOD 2: Auto-Discovery (HTML <link> tag)
  - Crawl publisher homepage
  - Look for RSS autodiscovery tag in HTML <head>:
  
    <link rel="alternate" type="application/rss+xml" 
          title="BBC News" 
          href="https://feeds.bbci.co.uk/news/rss.xml" />
  
  - Most news websites include this tag
  - Our discovery crawler checks the top 100K news domains

METHOD 3: Common URL Patterns (brute force)
  - Try common RSS URL patterns for each domain:
    {domain}/rss
    {domain}/feed
    {domain}/rss.xml
    {domain}/feed.xml
    {domain}/atom.xml
    {domain}/index.xml
    {domain}/feeds/posts/default  (Blogger)
    {domain}/?feed=rss2            (WordPress)
  
  - If any returns valid XML → found it

METHOD 4: Publisher Registration (self-service)
  - Publishers sign up via our portal
  - Provide their RSS feed URL
  - We validate and add to registry
```

### 4.2 Publisher Table Setup

```sql
-- What we store for each publisher
INSERT INTO publishers (
  id, name, domain, rss_feed_url, 
  scrape_type, priority, poll_interval_sec,
  region, language, is_active, config
) VALUES (
  'pub-bbc-001',
  'BBC News',
  'bbc.co.uk',
  'https://feeds.bbci.co.uk/news/rss.xml',
  'RSS',           -- RSS | ATOM | WEB | WEBHOOK
  'CRITICAL',      -- CRITICAL | HIGH | MEDIUM | LOW
  300,             -- 5 minutes (we'll override with adaptive algorithm)
  'UK',
  'en',
  true,
  '{
    "sub_feeds": [
      {"category": "world",   "url": "https://feeds.bbci.co.uk/news/world/rss.xml"},
      {"category": "tech",    "url": "https://feeds.bbci.co.uk/news/technology/rss.xml"},
      {"category": "sports",  "url": "https://feeds.bbci.co.uk/sport/rss.xml"},
      {"category": "business","url": "https://feeds.bbci.co.uk/news/business/rss.xml"}
    ],
    "charset": "UTF-8",
    "rate_limit_rpm": 10,
    "respect_ttl": true
  }'
);
```

> **Key point**: Many publishers have MULTIPLE RSS feeds — one per category. BBC has 50+ feeds (World, UK, Tech, Sports, Business, etc.). We track sub-feeds in the config.

---

## 5. The Crawl Flow — Step by Step

```
COMPLETE RSS CRAWL FLOW
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

STEP 1: SCHEDULER PICKS PUBLISHER
──────────────────────────────────

  Scheduler checks Redis priority queue every second:
    
    ZRANGEBYSCORE publisher_queue 0 {current_unix_timestamp} LIMIT 0 50
    
  This returns publishers whose "next crawl time" has passed.
  
  Example: at time 1732000000
    publisher_queue = {
      (1731999900, "pub-reuters-001"),    ← 100 sec overdue, pick this
      (1731999950, "pub-bbc-001"),        ← 50 sec overdue, pick this
      (1732000100, "pub-nyt-001"),        ← not yet due, skip
      (1732003600, "pub-smallblog-001"),  ← not due for 1 hour, skip
    }
  
  Scheduler sends picked publishers to crawler work queue.


STEP 2: CRAWLER CHECKS robots.txt (first time only, cached)
─────────────────────────────────────────────────────────────

  GET https://www.bbc.co.uk/robots.txt
  
  Response:
    User-agent: *
    Crawl-delay: 1          ← wait 1 second between requests
    Disallow: /search       ← don't crawl search pages
    Allow: /news/           ← news pages are OK
    Sitemap: https://www.bbc.co.uk/sitemap.xml
  
  We parse and respect:
    - Crawl-delay: add to our rate limiter
    - Disallow: skip URLs matching these patterns
  
  Cache robots.txt for 24 hours per domain.


STEP 3: SEND CONDITIONAL HTTP REQUEST
──────────────────────────────────────

  This is the key optimization that saves bandwidth.
  We store the ETag and Last-Modified from the previous crawl.
  
  Request:
    GET https://feeds.bbci.co.uk/news/rss.xml
    Headers:
      User-Agent: NewsFeedBot/1.0 (+https://newsfeed.com/bot)
      If-None-Match: "abc123-etag-from-last-crawl"
      If-Modified-Since: Mon, 18 Nov 2026 16:00:00 GMT
      Accept: application/rss+xml, application/atom+xml, application/xml
      Accept-Encoding: gzip, deflate
  
  CASE A — Feed hasn't changed (saves bandwidth):
    Response: 304 Not Modified
    Body: (empty)
    Action: Skip parsing. Reschedule next crawl. Done.
    
  CASE B — Feed has new content:
    Response: 200 OK
    Headers:
      ETag: "def456-new-etag"
      Last-Modified: Mon, 18 Nov 2026 16:30:00 GMT
      Content-Type: application/rss+xml; charset=UTF-8
      Content-Encoding: gzip
    Body: <rss> ... full XML feed ... </rss>
    Action: Continue to Step 4.
    
  CASE C — Feed is gone or broken:
    Response: 404 / 500 / timeout
    Action: Increment error counter. Back off exponentially.
            If 10+ consecutive failures → mark publisher inactive.


STEP 4: PARSE THE XML
─────────────────────

  Using a battle-tested XML parser (Go: gofeed, Python: feedparser)
  
  parser = gofeed.NewParser()
  feed, err = parser.ParseString(responseBody)
  
  The parser handles:
    ✅ RSS 2.0 and Atom 1.0 formats
    ✅ RSS 1.0 (RDF) legacy format
    ✅ Namespace extensions (media:, dc:, content:)
    ✅ CDATA sections in descriptions
    ✅ HTML entities in text
    ✅ Malformed XML (common with smaller publishers)
    ✅ Different date formats (RFC 822, ISO 8601, custom)
    ✅ Character encoding conversion (ISO-8859-1 → UTF-8)
  
  Output: list of normalized Article structs:
  
    type RawArticle struct {
      Title           string
      Summary         string     // from <description> or <summary>
      OriginalURL     string     // from <link>
      GUID            string     // from <guid> or <id>
      PublishedAt     time.Time  // from <pubDate> or <updated>
      Categories      []string   // from <category> tags
      ThumbnailURL    string     // from <media:thumbnail> or <enclosure>
      AuthorName      string     // from <author> or <dc:creator>
      PublisherID     string     // which publisher this came from
      DiscoveredAt    time.Time  // when we found it (now)
    }


STEP 5: FILTER OUT ALREADY-SEEN ARTICLES
─────────────────────────────────────────

  For each parsed article, check if we've already ingested it:
  
  Method 1: GUID check (fast, in-memory bloom filter)
    
    contentHash = SHA256(article.OriginalURL)
    
    if BloomFilter.Exists(contentHash):
      // Probably already seen (0.01% false positive)
      // Double-check in DB if needed
      skip this article
    else:
      BloomFilter.Add(contentHash)
      proceed to Step 6
  
  Method 2: URL uniqueness check (database, for bloom filter misses)
    
    SELECT EXISTS(
      SELECT 1 FROM articles WHERE original_url = {url}
    )
  
  WHY GUID/URL and not title?
    Different articles can have similar titles.
    URLs are guaranteed unique per article.
    The <guid> tag is specifically designed for this — it's the
    publisher's own unique identifier for the article.


STEP 6: PRODUCE TO KAFKA
─────────────────────────

  For each NEW article (not already seen):
    
    kafka.Produce(
      Topic: "raw-articles",
      Key:   publisher_id,       // ensures ordering per publisher
      Value: RawArticle{
        Title:        "Global Climate Summit...",
        Summary:      "World leaders have agreed...",
        OriginalURL:  "https://www.bbc.co.uk/news/world-67890123",
        GUID:         "https://www.bbc.co.uk/news/world-67890123",
        PublishedAt:  "2026-11-18T14:22:00Z",
        Categories:   ["Environment", "Politics"],
        ThumbnailURL: "https://ichef.bbci.co.uk/.../climate.jpg",
        PublisherID:  "pub-bbc-001",
        DiscoveredAt: "2026-11-18T16:30:05Z",
      }
    )
  
  From here, the article enters the Content Processing Pipeline
  (dedup → NLP → quality scoring → DB write → feed update)


STEP 7: UPDATE PUBLISHER STATE & RESCHEDULE
────────────────────────────────────────────

  // Update publisher metadata
  UPDATE publishers SET
    last_scraped_at = NOW(),
    last_etag = 'def456-new-etag',
    last_modified = '2026-11-18T16:30:00Z',
    consecutive_errors = 0,
    articles_found_last_crawl = 3
  WHERE id = 'pub-bbc-001';
  
  // Compute next crawl time (adaptive algorithm)
  next_interval = computeAdaptiveInterval(publisher)
  // For BBC (CRITICAL priority, active publisher): ~5 minutes
  
  // Schedule next crawl
  ZADD publisher_queue {now + next_interval} "pub-bbc-001"
  // e.g., ZADD publisher_queue 1732000305 "pub-bbc-001"
  
  Done! Crawler picks up next publisher from queue.
```

---

## 6. HTTP Optimization Details

### 6.1 Conditional Requests (save bandwidth)

```
WHY CONDITIONAL REQUESTS MATTER:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

We crawl 50,000 publishers. If each feed is ~100 KB:
  Without conditional requests: 50,000 × 100 KB = 5 GB per crawl cycle
  With conditional requests:    ~70% return 304 (no change)
                                50,000 × 30% × 100 KB = 1.5 GB per cycle
                                (304 responses are ~200 bytes each)
  
  Savings: ~70% bandwidth reduction

HOW IT WORKS:

  First request (no cached headers):
    GET /rss.xml
    → 200 OK
    → ETag: "abc123"
    → Last-Modified: Mon, 18 Nov 2026 14:00:00 GMT
    → Body: full XML (100 KB)
    
    We store: etag="abc123", lastModified="2026-11-18T14:00:00Z"

  Second request (5 minutes later):
    GET /rss.xml
    If-None-Match: "abc123"
    If-Modified-Since: Mon, 18 Nov 2026 14:00:00 GMT
    → 304 Not Modified      ← feed hasn't changed
    → Body: (empty)          ← saved 100 KB
    
  Third request (10 minutes later):
    GET /rss.xml
    If-None-Match: "abc123"
    If-Modified-Since: Mon, 18 Nov 2026 14:00:00 GMT
    → 200 OK                 ← new articles published!
    → ETag: "def456"         ← new etag
    → Body: full XML with new articles
```

### 6.2 Rate Limiting Per Domain

```
WHY WE RATE LIMIT OURSELVES:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

We're a good internet citizen. If we hammer a publisher's server
with 100 requests/second, they'll block us and might even take
legal action. 

We implement per-domain rate limiting:

  Token bucket per publisher domain:
  
    ┌──────────────────┬────────────────┬────────────────────┐
    │ Publisher Tier    │ Max Req/Min    │ Why                │
    ├──────────────────┼────────────────┼────────────────────┤
    │ CRITICAL (Reuters)│ 10 req/min    │ Big CDN, can handle│
    │ HIGH (NYT)        │ 5 req/min     │ Moderate capacity  │
    │ MEDIUM (regional) │ 2 req/min     │ Smaller servers    │
    │ LOW (blogs)       │ 1 req/min     │ Shared hosting     │
    └──────────────────┴────────────────┴────────────────────┘
  
  Also respect:
    - robots.txt Crawl-delay directive
    - HTTP 429 Too Many Requests (back off immediately)
    - HTTP Retry-After header
  
  Implementation (Redis token bucket):
    key = "ratelimit:crawl:{domain}"
    
    allowed = redis.eval("""
      local tokens = redis.call('GET', KEYS[1]) or MAX_TOKENS
      if tonumber(tokens) > 0 then
        redis.call('DECR', KEYS[1])
        return 1  -- allowed
      else
        return 0  -- rate limited, wait
      end
    """)
    
    if not allowed:
      sleep(60 / max_requests_per_minute)  -- wait for token refill
```

---

## 7. Handling Edge Cases

```
EDGE CASES IN RSS CRAWLING:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. MALFORMED XML
   Problem: Many small publishers have broken XML (unclosed tags,
            invalid characters, wrong encoding)
   Solution: Use a lenient parser (gofeed handles most cases)
             Fall back to regex extraction for severely broken feeds
             Log and alert if a previously-working feed starts failing

2. FEED URL CHANGES
   Problem: Publisher redesigns site, RSS URL changes
   Solution: 
     - If old URL returns 301 Redirect → update stored URL automatically
     - If old URL returns 404 → try auto-discovery on homepage
     - If that fails → mark publisher for manual review

3. DUPLICATE GUID / NO GUID
   Problem: Some feeds reuse GUIDs or don't include them
   Solution:
     - If no <guid>: use <link> as GUID (usually unique per article)
     - If <link> also missing: use SHA256(title + pubDate)
     - Track per-publisher GUID behavior in config

4. TIMEZONE CHAOS IN pubDate
   Problem: Different publishers use different date formats:
     "Mon, 18 Nov 2026 14:22:00 GMT"          (RFC 822)
     "2026-11-18T14:22:00Z"                    (ISO 8601)
     "Nov 18, 2026 2:22 PM EST"                (custom US)
     "18/11/2026 14:22"                        (European)
     "1732000920"                               (Unix timestamp)
   Solution: 
     dateutil parser with fallback chain
     Store discovered format per publisher
     Always normalize to UTC before storing

5. FEED CONTAINS OLD ARTICLES ONLY
   Problem: Some feeds keep the same 10 articles for weeks
   Solution:
     Compare article GUIDs against last crawl
     If 100% overlap → no new content, extend poll interval
     If 0% overlap → feed was rebuilt, process all as new

6. PUBLISHER SERVES HTML INSTEAD OF XML
   Problem: Misconfigured server returns HTML page for RSS URL
   Solution:
     Check Content-Type header:
       Expected: application/rss+xml, application/atom+xml, text/xml
       If: text/html → try to find RSS link in the HTML
     Check first few bytes: should start with <?xml or <rss or <feed

7. VERY LARGE FEEDS (> 1 MB)
   Problem: Some aggregator feeds contain 1000+ items
   Solution:
     Set max response size: 5 MB
     Only process first 100 items (most recent)
     Stream-parse XML instead of loading entire document into memory

8. CHARACTER ENCODING ISSUES
   Problem: Feed declares UTF-8 but contains ISO-8859-1 characters
   Solution:
     Detect actual encoding using chardet library
     Transcode to UTF-8 before parsing
     Strip invalid UTF-8 sequences

9. RELATIVE URLs
   Problem: <link>news/article-123</link> (missing domain)
   Solution:
     Resolve against feed URL base:
     base = "https://www.bbc.co.uk/news/rss.xml"
     resolved = urljoin(base, "news/article-123")
     → "https://www.bbc.co.uk/news/article-123"
```

---

## 8. Crawler Implementation (Go Pseudocode)

```go
// crawler/worker.go — Core RSS Crawler Logic

package crawler

import (
    "github.com/mmcdole/gofeed"
    "time"
)

type CrawlResult struct {
    PublisherID    string
    ArticlesFound  int
    NewArticles    int
    Duration       time.Duration
    Error          error
    NextCrawlAt    time.Time
    NewETag        string
    NewLastModified string
}

func (w *Worker) CrawlPublisher(pub Publisher) CrawlResult {
    start := time.Now()
    result := CrawlResult{PublisherID: pub.ID}

    // ── Step 1: Check rate limit ──
    if !w.rateLimiter.Allow(pub.Domain) {
        result.NextCrawlAt = time.Now().Add(30 * time.Second)
        return result // try again later
    }

    // ── Step 2: Build HTTP request with conditional headers ──
    req := buildRequest(pub.RSSFeedURL)
    if pub.LastETag != "" {
        req.Header.Set("If-None-Match", pub.LastETag)
    }
    if pub.LastModified != "" {
        req.Header.Set("If-Modified-Since", pub.LastModified)
    }
    req.Header.Set("User-Agent", "NewsFeedBot/1.0 (+https://newsfeed.com/bot)")
    req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml")
    req.Header.Set("Accept-Encoding", "gzip")

    // ── Step 3: Execute HTTP request (10s timeout) ──
    resp, err := w.httpClient.Do(req)  // timeout: 10s
    if err != nil {
        result.Error = err
        w.handleCrawlError(pub, err)
        result.NextCrawlAt = w.computeBackoff(pub)
        return result
    }
    defer resp.Body.Close()

    // ── Step 4: Handle response status ──
    switch {
    case resp.StatusCode == 304:
        // Feed not modified since last crawl — nothing to do
        result.NextCrawlAt = w.computeNextCrawl(pub, 0)
        return result

    case resp.StatusCode == 301 || resp.StatusCode == 308:
        // Feed URL permanently moved — update our records
        newURL := resp.Header.Get("Location")
        w.updatePublisherURL(pub.ID, newURL)
        // Re-crawl with new URL
        pub.RSSFeedURL = newURL
        return w.CrawlPublisher(pub)

    case resp.StatusCode == 429:
        // Rate limited by publisher — back off
        retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
        result.NextCrawlAt = time.Now().Add(retryAfter)
        return result

    case resp.StatusCode != 200:
        // Other error (404, 500, etc.)
        result.Error = fmt.Errorf("HTTP %d from %s", resp.StatusCode, pub.Domain)
        w.handleCrawlError(pub, result.Error)
        result.NextCrawlAt = w.computeBackoff(pub)
        return result
    }

    // ── Step 5: Parse RSS/Atom XML ──
    // gofeed automatically detects RSS vs Atom and normalizes
    parser := gofeed.NewParser()
    feed, err := parser.Parse(resp.Body)
    if err != nil {
        result.Error = fmt.Errorf("XML parse error: %w", err)
        w.handleCrawlError(pub, result.Error)
        result.NextCrawlAt = w.computeBackoff(pub)
        return result
    }

    result.ArticlesFound = len(feed.Items)

    // ── Step 6: Save new ETag and Last-Modified for next crawl ──
    result.NewETag = resp.Header.Get("ETag")
    result.NewLastModified = resp.Header.Get("Last-Modified")

    // ── Step 7: Process each article ──
    for _, item := range feed.Items {
        // Normalize the parsed item into our internal struct
        article := normalizeArticle(item, pub)

        // Check bloom filter: have we seen this URL before?
        urlHash := sha256(article.OriginalURL)
        if w.bloomFilter.Exists(urlHash) {
            continue // already ingested, skip
        }

        // New article! Add to bloom filter and send to Kafka
        w.bloomFilter.Add(urlHash)
        w.kafkaProducer.Produce("raw-articles", pub.ID, article)
        result.NewArticles++
    }

    // ── Step 8: Compute next crawl time ──
    result.NextCrawlAt = w.computeNextCrawl(pub, result.NewArticles)
    result.Duration = time.Since(start)

    // ── Step 9: Update publisher state in DB ──
    w.updatePublisherState(pub.ID, UpdatePayload{
        LastScrapedAt:          time.Now(),
        LastETag:               result.NewETag,
        LastModified:           result.NewLastModified,
        ArticlesFoundLastCrawl: result.NewArticles,
        ConsecutiveErrors:      0,
    })

    // ── Step 10: Reschedule in priority queue ──
    w.redis.ZAdd("publisher_queue", result.NextCrawlAt.Unix(), pub.ID)

    return result
}

// normalizeArticle converts gofeed.Item → our internal RawArticle
func normalizeArticle(item *gofeed.Item, pub Publisher) RawArticle {
    article := RawArticle{
        PublisherID:  pub.ID,
        DiscoveredAt: time.Now(),
    }

    // Title
    article.Title = strings.TrimSpace(item.Title)

    // Summary: strip HTML tags, truncate
    article.Summary = stripHTML(item.Description)
    if len(article.Summary) > 500 {
        article.Summary = article.Summary[:500]
    }

    // URL: resolve relative URLs
    article.OriginalURL = resolveURL(pub.RSSFeedURL, item.Link)

    // GUID: use item's GUID, fall back to URL
    if item.GUID != "" {
        article.GUID = item.GUID
    } else {
        article.GUID = article.OriginalURL
    }

    // Published date: parse various formats, normalize to UTC
    article.PublishedAt = parseDate(item.Published, item.Updated)
    if article.PublishedAt.IsZero() {
        article.PublishedAt = time.Now() // fallback
    }

    // Categories from RSS tags
    article.Categories = item.Categories

    // Thumbnail: check multiple sources
    article.ThumbnailURL = extractThumbnail(item)

    return article
}

// extractThumbnail tries multiple RSS extensions to find an image
func extractThumbnail(item *gofeed.Item) string {
    // 1. Check media:thumbnail (Media RSS extension)
    if item.Extensions["media"] != nil {
        if thumbs, ok := item.Extensions["media"]["thumbnail"]; ok {
            return thumbs[0].Attrs["url"]
        }
    }

    // 2. Check media:content
    if item.Extensions["media"] != nil {
        if content, ok := item.Extensions["media"]["content"]; ok {
            for _, c := range content {
                if strings.HasPrefix(c.Attrs["type"], "image/") {
                    return c.Attrs["url"]
                }
            }
        }
    }

    // 3. Check enclosure (RSS 2.0 media attachment)
    if item.Enclosures != nil {
        for _, enc := range item.Enclosures {
            if strings.HasPrefix(enc.Type, "image/") {
                return enc.URL
            }
        }
    }

    // 4. Extract first <img> from description HTML
    if imgURL := extractFirstImage(item.Description); imgURL != "" {
        return imgURL
    }

    // 5. No thumbnail found
    return ""
}
```

---

## 9. What Publishers Need To Do (Answer: Almost Nothing)

```
FROM THE PUBLISHER'S PERSPECTIVE:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Most publishers ALREADY have RSS feeds. No setup needed from them.

If a publisher WANTS to optimize for us:

BASIC (most publishers already do this):
  ✅ Maintain an RSS/Atom feed with their latest articles
  ✅ Include <guid> for each article (unique, permanent ID)
  ✅ Include <pubDate> in a standard format
  ✅ Support ETag / Last-Modified headers (web server usually does this)
  ✅ Allow our User-Agent in robots.txt

OPTIONAL (for faster ingestion):
  ⭐ Implement WebSub/PubSubHubbub webhooks
     - Publisher notifies us INSTANTLY when new article is published
     - Reduces latency from minutes to seconds
     - Publisher adds to their feed:
       <atom:link rel="hub" href="https://pubsubhubbub.appspot.com" />
     - When new article published, publisher POSTs to hub
     - Hub notifies all subscribers (including us)

  ⭐ Register with our publisher portal
     - Get API key for webhook push
     - Choose notification preferences
     - See analytics on their articles' performance

  ⭐ Include media:thumbnail in RSS
     - Helps us display better thumbnails
     - Otherwise we try to extract from article page (slower)

The beauty of RSS: it's a PULL model. Publishers don't need to know
we exist. We just fetch their public feed URL like any RSS reader.
```

---

## 10. Architecture Diagram — RSS Crawl System

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       RSS CRAWL SUBSYSTEM                               │
│                                                                         │
│  ┌────────────────────────────────────────────────┐                     │
│  │            ADAPTIVE SCHEDULER                   │                    │
│  │                                                │                    │
│  │  Runs every 1 second:                          │                    │
│  │    1. ZRANGEBYSCORE publisher_queue 0 {now}     │                    │
│  │       → returns overdue publishers              │                    │
│  │    2. Distribute to worker queues by priority   │                    │
│  │                                                │                    │
│  │  ┌──────────────────────────────────────────┐  │                    │
│  │  │  Redis Sorted Set: publisher_queue       │  │                    │
│  │  │                                          │  │                    │
│  │  │  Score (unix ts)    Member               │  │                    │
│  │  │  ──────────────    ──────────────         │  │                    │
│  │  │  1732000000        pub-reuters-001  ← due│  │                    │
│  │  │  1732000050        pub-bbc-001      ← due│  │                    │
│  │  │  1732000300        pub-nyt-001           │  │                    │
│  │  │  1732003600        pub-blog-042          │  │                    │
│  │  │  ...               ...                   │  │                    │
│  │  │  (50,000 entries)                        │  │                    │
│  │  └──────────────────────────────────────────┘  │                    │
│  └────────────────────────┬───────────────────────┘                    │
│                           │                                             │
│            ┌──────────────┼──────────────┐                              │
│            │              │              │                               │
│            ▼              ▼              ▼                               │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                    │
│  │ HI-PRI QUEUE │ │ MID-PRI QUEUE│ │ LOW-PRI QUEUE│                    │
│  │              │ │              │ │              │                     │
│  │ 20 workers   │ │ 50 workers   │ │ 100 workers  │                    │
│  │ Goroutines   │ │ Goroutines   │ │ Goroutines   │                    │
│  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘                    │
│         │                │                │                             │
│         └────────────────┼────────────────┘                             │
│                          │                                              │
│                          │  For each publisher:                         │
│                          │                                              │
│                          ▼                                              │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │                     CRAWLER WORKER                            │      │
│  │                                                              │      │
│  │  1. Rate limit check (Redis token bucket per domain)         │      │
│  │                │                                              │      │
│  │                ▼                                              │      │
│  │  2. HTTP GET {rss_feed_url}                                  │      │
│  │     If-None-Match: {stored_etag}                             │      │
│  │     If-Modified-Since: {stored_last_modified}                │      │
│  │                │                                              │      │
│  │        ┌───────┼───────┐                                     │      │
│  │        │       │       │                                     │      │
│  │      304     200     4xx/5xx                                 │      │
│  │   (no change) │   (error)                                    │      │
│  │        │      │       │                                      │      │
│  │   reschedule  │    backoff                                   │      │
│  │        │      │    reschedule                                │      │
│  │        │      ▼       │                                      │      │
│  │        │  3. Parse XML (gofeed library)                      │      │
│  │        │     Handle RSS 2.0, Atom 1.0, RSS 1.0               │      │
│  │        │              │                                      │      │
│  │        │              ▼                                      │      │
│  │        │  4. For each <item>:                                │      │
│  │        │       url_hash = SHA256(article.url)                │      │
│  │        │       if BloomFilter.Exists(url_hash) → skip        │      │
│  │        │       else → BloomFilter.Add(url_hash)              │      │
│  │        │            → Produce to Kafka "raw-articles"        │      │
│  │        │              │                                      │      │
│  │        │              ▼                                      │      │
│  │        │  5. Update publisher state in DB                    │      │
│  │        │     (last_scraped_at, etag, errors, article count)  │      │
│  │        │              │                                      │      │
│  │        │              ▼                                      │      │
│  │        └──▶ 6. ZADD publisher_queue {next_crawl_time} {id}  │      │
│  │                                                              │      │
│  └──────────────────────────────────────────────────────────────┘      │
│                          │                                              │
│                          ▼                                              │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  KAFKA TOPIC: raw-articles                                    │      │
│  │  → Consumed by Content Processing Pipeline                   │      │
│  │  → (dedup → NLP → quality scoring → DB write → feed update)  │      │
│  └──────────────────────────────────────────────────────────────┘      │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │  BLOOM FILTER (Redis)                                        │      │
│  │  Key: article_urls                                           │      │
│  │  Size: ~60 MB (50M items, 0.01% FP rate)                    │      │
│  │  Purpose: Fast check if URL already ingested                 │      │
│  │  Avoids: duplicate Kafka messages, duplicate DB queries      │      │
│  └──────────────────────────────────────────────────────────────┘      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘

EXTERNAL:

  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
  │ Reuters        │  │ BBC            │  │ Small Blog     │
  │ /rss/topnews   │  │ /news/rss.xml  │  │ /feed.xml      │
  │                │  │                │  │                │
  │ Updates: every │  │ Updates: every │  │ Updates: every │
  │ few minutes    │  │ 15-30 min      │  │ few days       │
  └────────────────┘  └────────────────┘  └────────────────┘
     We poll: 5 min      We poll: 10 min    We poll: 4 hours
```

---

## Summary

```
RSS IN OUR SYSTEM — KEY TAKEAWAYS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. RSS is just XML served over HTTP — no special setup needed
   from publishers. We GET their feed URL and parse the response.

2. We maintain a PUBLISHER REGISTRY of 50K+ feed URLs,
   discovered via manual curation, HTML auto-discovery,
   and common URL patterns.

3. An ADAPTIVE SCHEDULER uses a Redis sorted set as a priority
   queue to decide which publisher to crawl next, based on
   priority tier, publishing frequency, and time-of-day.

4. CONDITIONAL REQUESTS (ETag / If-Modified-Since) save ~70%
   bandwidth by skipping unchanged feeds.

5. A BLOOM FILTER provides O(1) URL dedup at the crawler level,
   preventing duplicate articles from even reaching Kafka.

6. PARSED ARTICLES go to Kafka "raw-articles" topic, then through
   the content processing pipeline (dedup → NLP → quality → DB).

7. PER-DOMAIN RATE LIMITING + robots.txt compliance ensures we
   don't overwhelm publisher servers.

8. WEBHOOKS (optional) provide instant push from publishers who
   want faster indexing — bypasses the polling delay entirely.

9. The whole system is PULL-BASED by default. Publishers don't
   need to know we exist. This is the fundamental advantage
   of RSS as a syndication protocol.
```
