import os
import json
import requests
import feedparser
from anthropic import Anthropic
from supabase import create_client
from datetime import datetime, timezone

client   = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
NEWS_KEY = os.environ["NEWS_API_KEY"]

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
    ],
    "life_insurance": [
        "https://www.lifehealthpro.com/feed",
        "https://www.ThinkAdvisor.com/feed/",
    ],
    "marketing_branding": [
        "https://adage.com/rss.xml",
        "https://www.adweek.com/feed/",
        "https://feeds.hbr.org/harvardbusiness",
    ],
    "content_creation": [
        "https://contentmarketinginstitute.com/feed/",
        "https://copyblogger.com/feed/",
    ],
    "nyc_events": [
        "https://gothamist.com/feed",
        "https://www.nytimes.com/services/xml/rss/nyt/Arts.xml",
        "https://www.timeout.com/newyork/feed.xml",
    ],
}

NEWSAPI_QUERIES = [
    ("genai",              "generative AI OR large language models OR OpenAI OR Anthropic"),
    ("tech",               "technology startups OR silicon valley OR big tech"),
    ("insurance",          "insurance industry OR insurtech OR underwriting"),
    ("life_insurance",     "life insurance OR annuities OR life coverage"),
    ("marketing_branding", "brand strategy OR product marketing OR brand campaign"),
    ("content_creation",   "content strategy OR creator economy OR content marketing"),
    ("nyc_events",         "NYC events OR New York City arts OR Manhattan culture"),
]

PERSONAL_PROFILE = """You are the personal research editor for someone who:
- Works in tech and insurance/life insurance
- Is passionate about product marketing, branding, and content strategy
- Lives in NYC and wants to know about arts, culture, and local events
- Follows GenAI closely for both technical and business implications
- Considers something urgent if it affects their career, is NYC-specific, or is industry-moving
"""

def fetch_rss(category, urls):
    articles = []
    for url in urls:
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries[:5]:
                articles.append({
                    "title":    entry.get("title", ""),
                    "url":      entry.get("link", ""),
                    "text":     entry.get("summary", "")[:400],
                    "category": category,
                })
        except Exception as e:
            print(f"RSS error {url}: {e}")
    return articles

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
                "text":     (a.get("description") or "")[:400],
                "category": category,
            })
        return articles
    except Exception as e:
        print(f"NewsAPI error {query}: {e}")
        return []

def deduplicate(articles):
    seen, unique = set(), []
    for a in articles:
        key = a["title"].lower()[:60]
        if key not in seen and a["title"]:
            seen.add(key)
            unique.append(a)
    return unique

def process_with_claude(articles):
    if not articles:
        return []

    results = []
    for a in articles:
        try:
            prompt = f"""{PERSONAL_PROFILE}

Analyze this article and respond with a single JSON object only.
No markdown. No code fences. No explanation. Just the raw JSON object.

Article title: {a['title']}
Article category: {a['category']}
Article text: {a['text']}
Article url: {a['url']}

Respond with exactly this structure:
{{"title": "the article title", "summary": "2 sentence plain english summary", "category": "{a['category']}", "urgency": 7, "sentiment": "positive", "source_url": "{a['url']}", "why_matters": "one sentence on why this matters to this person"}}"""

            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}]
            )

            raw = response.content[0].text.strip()
            raw = raw.replace("```json", "").replace("```", "").strip()

            if not raw.startswith("{"):
                idx = raw.find("{")
                if idx != -1:
                    raw = raw[idx:]

            end = raw.rfind("}") + 1
            if end > 0:
                raw = raw[:end]

            print(f"  Preview: {raw[:60]}")
            obj = json.loads(raw)
            results.append(obj)

        except Exception as e:
            print(f"  Error on article '{a['title'][:40]}': {e}")
            continue

    return results

def save_to_supabase(processed):
    if not processed:
        print("Nothing to save.")
        return

    rows = []
    for a in processed:
        rows.append({
            "title":       str(a.get("title", "")),
            "summary":     str(a.get("summary", "")),
            "category":    str(a.get("category", "")),
            "urgency":     int(a.get("urgency", 5)),
            "sentiment":   str(a.get("sentiment", "neutral")),
            "source_url":  str(a.get("source_url", "")),
            "why_matters": str(a.get("why_matters", "")),
            "created_at":  datetime.now(timezone.utc).isoformat(),
        })

    supabase.table("articles").insert(rows).execute()
    print(f"Saved {len(rows)} articles to Supabase.")

def main():
    print("Starting pipeline...")
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
    print(f"  Processing {len(unique)} articles one by one...")

    processed = process_with_claude(unique)
    print(f"  Successfully processed: {len(processed)} articles")

    save_to_supabase(processed)
    print("Pipeline complete.")

if __name__ == "__main__":
    main()
