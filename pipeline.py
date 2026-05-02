import os
import json
import requests
import feedparser
from anthropic import Anthropic
from supabase import create_client
from dateutil import parser as dateparser
from datetime import datetime, timezone

# ── clients ──────────────────────────────────────────
client   = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
NEWS_KEY = os.environ["NEWS_API_KEY"]

# ── RSS sources tailored to your interests ───────────
RSS_FEEDS = {
    "genai": [
        "https://feeds.feedburner.com/oreilly/radar",
        "https://huggingface.co/blog/feed.xml",
        "https://bair.berkeley.edu/blog/feed.xml",
    ],
    "tech": [
        "https://feeds.feedburner.com/TechCrunch",
        "https://www.theverge.com/rss/index.xml",
        "https://www.wired.com/feed/rss",
    ],
    "insurance": [
        "https://www.insurancejournal.com/feed/",
        "https://www.carriermanagement.com/feed/",
        "https://insuranceblog.accenture.com/feed",
    ],
    "life_insurance": [
        "https://www.lifehealthpro.com/feed",
        "https://www.ThinkAdvisor.com/feed/",
    ],
    "marketing_branding": [
        "https://feeds.feedburner.com/MarketingWeekNews",
        "https://adage.com/rss.xml",
        "https://www.adweek.com/feed/",
        "https://feeds.hbr.org/harvardbusiness",
    ],
    "content_creation": [
        "https://contentmarketinginstitute.com/feed/",
        "https://www.creativebloq.com/rss",
        "https://copyblogger.com/feed/",
    ],
    "nyc_events": [
        "https://www.timeout.com/newyork/feed.xml",
        "https://gothamist.com/feed",
        "https://www.nytimes.com/services/xml/rss/nyt/Arts.xml",
        "https://www.nytimes.com/services/xml/rss/nyt/NYRegion.xml",
    ],
}

# ── NewsAPI queries per topic ─────────────────────────
NEWSAPI_QUERIES = [
    ("genai",              "generative AI OR large language models OR OpenAI OR Anthropic"),
    ("tech",               "technology startups OR silicon valley OR big tech"),
    ("insurance",          "insurance industry OR insurtech OR underwriting"),
    ("life_insurance",     "life insurance OR annuities OR life coverage"),
    ("marketing_branding", "brand strategy OR product marketing OR brand campaign"),
    ("content_creation",   "content strategy OR creator economy OR content marketing"),
    ("nyc_events",         "NYC events OR New York City arts OR Manhattan culture"),
]

# ── your personal profile (Claude reads this) ────────
PERSONAL_PROFILE = """
You are the personal research editor for someone with this profile:
- Works in tech and insurance/life insurance
- Passionate about product marketing, branding, and content strategy
- Lives in New York City - wants to know about arts, culture, and events happening there
- Follows GenAI closely - both technical developments and business implications
- Defines URGENT as: (1) career impact - directly affects their work,
  (2) NYC-specific - happening in their city, OR
  (3) industry-moving - major shifts in tech, GenAI, or insurance

When scoring relevance and urgency, think specifically about this person -
not whether something is generally important news, but whether it matters TO THEM.
Always write a why_matters line that connects the article to their life or work.
"""

# ── fetch RSS articles ────────────────────────────────
def fetch_rss(category, urls):
    articles = []
    for url in urls:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                articles.append({
                    "title":    entry.get("title", ""),
                    "url":      entry.get("link", ""),
                    "text":     entry.get("summary", "")[:500],
                    "category": category,
                })
        except Exception as e:
            print(f"RSS error {url}: {e}")
    return articles

# ── fetch NewsAPI articles ────────────────────────────
def fetch_newsapi(category, query):
    try:
        r = requests.get("https://newsapi.org/v2/everything", params={
            "q":        query,
            "sortBy":   "publishedAt",
            "pageSize": 10,
            "language": "en",
            "apiKey":   NEWS_KEY,
        })
        data = r.json()
        articles = []
        for a in data.get("articles", []):
            articles.append({
                "title":    a.get("title", ""),
                "url":      a.get("url", ""),
                "text":     (a.get("description") or "")[:500],
                "category": category,
            })
        return articles
    except Exception as e:
        print(f"NewsAPI error {query}: {e}")
        return []

# ── deduplicate by title similarity ──────────────────
def deduplicate(articles):
    seen, unique = set(), []
    for a in articles:
        key = a["title"].lower()[:60]
        if key not in seen and a["title"]:
            seen.add(key)
            unique.append(a)
    return unique

# ── send batch to Claude, get structured data back ───
def process_with_claude(articles):
    if not articles:
        return []

    batch_text = "\n\n".join([
        f"[{i+1}] TITLE: {a['title']}\nCATEGORY: {a['category']}\nTEXT: {a['text']}\nURL: {a['url']}"
        for i, a in enumerate(articles)
    ])

    prompt = f"""{PERSONAL_PROFILE}

Here are today's articles. Return ONLY a JSON array, no markdown, no code fences, no explanation.
Start your response with [ and end with ].

Each item in the array must have exactly these fields:
[
  {{
    "title": "original title",
    "summary": "2 sentence summary in plain conversational english",
    "category": "genai|tech|insurance|life_insurance|marketing_branding|content_creation|nyc_events",
    "urgency": ,
    "sentiment": "positive|negative|neutral",
    "source_url": "original url",
    "why_matters": "one sentence explaining why this matters specifically to this person"
  }}
]

ARTICLES:
{batch_text}"""

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4000,
        messages=[{"role": "user", "content": prompt}]
    )

    raw = response.content[0].text.strip()
    raw = raw.replace("```json", "").replace("```", "").strip()

    if not raw.startswith("["):
        idx = raw.find("[")
        if idx != -1:
            raw = raw[idx:]

    print(f"  Claude preview: {raw[:80]}")

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"  JSON error: {e}")
        print(f"  Raw: {raw[:300]}")
        return []

# ── write results to Supabase ─────────────────────────
def save_to_supabase(processed):
    if not processed:
        print("Nothing to save.")
        return

    rows = []
    for a in processed:
        rows.append({
            "title":       a.get("title", ""),
            "summary":     a.get("summary", ""),
            "category":    a.get("category", ""),
            "urgency":     a.get("urgency", 5),
            "sentiment":   a.get("sentiment", "neutral"),
            "source_url":  a.get("source_url", ""),
            "why_matters": a.get("why_matters", ""),
            "created_at":  datetime.now(timezone.utc).isoformat(),
        })

    result = supabase.table("articles").insert(rows).execute()
    print(f"Saved {len(rows)} articles to Supabase.")
    return result

# ── main ──────────────────────────────────────────────
def main():
    print("Starting daily briefing pipeline...")
    all_articles = []

    for category, urls in RSS_FEEDS.items():
        articles = fetch_rss(category, urls)
        print(f"  RSS [{category}]: {len(articles)} articles")
        all_articles.extend(articles)

    for category, query in NEWSAPI_QUERIES:
        articles = fetch_newsapi(category, query)
        print(f"  NewsAPI [{category}]: {len(articles)} articles")
        all_articles.extend(articles)

    unique = deduplicate(all_articles)
    print(f"  Total after dedup: {len(unique)} articles")

    batch_size = 20
    all_processed = []
    for i in range(0, len(unique), batch_size):
        batch = unique[i:i+batch_size]
        print(f"  Processing batch {i//batch_size + 1}...")
        processed = process_with_claude(batch)
        all_processed.extend(processed)

    save_to_supabase(all_processed)
    print("Pipeline complete.")

if __name__ == "__main__":
    main()
