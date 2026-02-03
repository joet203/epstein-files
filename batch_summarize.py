import sqlite3, json, os, time, urllib.request

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")
API_KEY = os.environ.get("GEMINI_API_KEY")
URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"

PROMPT = """You are analyzing a declassified DOJ document from the Jeffrey Epstein case.

Provide a concise summary with these sections:
- **What this is**: Document type (email, deposition, FBI report, etc) and date if available
- **Key people**: Names mentioned and their roles
- **Key facts**: The most important revelations, allegations, or facts (bullet points)
- **Notable**: Anything particularly significant or surprising

Be specific. Use names, dates, and direct references. Skip boilerplate and focus on substance. If the text is too garbled to extract meaning, say so briefly.

Document text:
"""

def summarize(text):
    payload = json.dumps({
        "contents": [{"parts": [{"text": PROMPT + text[:7000]}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 512}
    })
    req = urllib.request.Request(URL, data=payload.encode(), headers={"Content-Type": "application/json"})
    resp = urllib.request.urlopen(req)
    result = json.loads(resp.read())
    return result["candidates"][0]["content"]["parts"][0]["text"]

def main():
    if not API_KEY:
        print("ERROR: GEMINI_API_KEY not set. Run: source ~/.zshrc")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("ALTER TABLE documents ADD COLUMN ai_summary TEXT DEFAULT ''")
    except:
        pass

    rows = conn.execute("""
        SELECT id, filename, condensed, full_text
        FROM documents
        WHERE interest_score >= 40 AND (ai_summary IS NULL OR ai_summary = '')
        ORDER BY interest_score DESC
    """).fetchall()

    print(f"Summarizing {len(rows)} documents...")

    for i, (doc_id, fname, condensed, full_text) in enumerate(rows):
        text = condensed if condensed and len(condensed) > 50 else full_text
        if not text or len(text.strip()) < 30:
            conn.execute("UPDATE documents SET ai_summary='[No extractable text]' WHERE id=?", (doc_id,))
            continue

        try:
            summary = summarize(text)
            conn.execute("UPDATE documents SET ai_summary=? WHERE id=?", (summary, doc_id))
            conn.commit()
            print(f"  [{i+1}/{len(rows)}] {fname} ✓")
        except Exception as e:
            print(f"  [{i+1}/{len(rows)}] {fname} ERROR: {e}")
            time.sleep(2)
            continue

        # rate limit: ~15 req/min for free tier
        if (i + 1) % 14 == 0:
            print("  (rate limit pause)")
            time.sleep(62)

    print("\nDone!")
    conn.close()

if __name__ == "__main__":
    main()
