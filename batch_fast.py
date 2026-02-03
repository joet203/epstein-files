import sqlite3, json, os, urllib.request, concurrent.futures, time

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")
API_KEY = os.environ.get("GEMINI_API_KEY")
URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"

SUMMARY_PROMPT = """You are analyzing a declassified DOJ document from the Jeffrey Epstein case.

Provide a concise summary with these sections:
- **What this is**: Document type (email, deposition, FBI report, etc) and date if available
- **Key people**: Names mentioned and their roles
- **Key facts**: The most important revelations, allegations, or facts (bullet points)
- **Notable**: Anything particularly significant or surprising

Be specific. Use names, dates, and direct references. Skip boilerplate and focus on substance. If the text is too garbled to extract meaning, say so briefly.

Document text:
"""

RANK_PROMPT = """Rate this document summary's newsworthiness on a scale of 1-100 for someone investigating the Epstein case. Consider:
- Does it contain specific allegations of crimes?
- Does it name powerful/famous people in compromising situations?
- Does it reveal cover-ups, obstruction, or corruption?
- Does it contain firsthand witness testimony about abuse?
- Does it show connections between Epstein and institutions?

Respond with ONLY a JSON object: {"score": <number>, "reason": "<one sentence>"}

Summary:
"""

def call_gemini(prompt, max_tokens=512):
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": max_tokens}
    })
    req = urllib.request.Request(URL, data=payload.encode(), headers={"Content-Type": "application/json"})
    for attempt in range(3):
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            result = json.loads(resp.read())
            return result["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                time.sleep(5 * (attempt + 1))
            else:
                time.sleep(2)
    return None

def summarize_doc(row):
    doc_id, fname, condensed, full_text = row
    text = condensed if condensed and len(condensed) > 50 else full_text
    if not text or len(text.strip()) < 30:
        return doc_id, fname, "[No extractable text]", 0, ""

    summary = call_gemini(SUMMARY_PROMPT + text[:7000])
    if not summary:
        return doc_id, fname, None, 0, ""

    # now rank it
    rank_resp = call_gemini(RANK_PROMPT + summary, max_tokens=100)
    news_score = 0
    reason = ""
    if rank_resp:
        try:
            # extract JSON from response
            j = rank_resp.strip()
            if j.startswith("```"):
                j = j.split("\n", 1)[1].rsplit("```", 1)[0]
            data = json.loads(j)
            news_score = int(data.get("score", 0))
            reason = data.get("reason", "")
        except:
            pass

    return doc_id, fname, summary, news_score, reason

def main():
    if not API_KEY:
        print("ERROR: GEMINI_API_KEY not set")
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("ALTER TABLE documents ADD COLUMN ai_summary TEXT DEFAULT ''")
    except:
        pass
    try:
        conn.execute("ALTER TABLE documents ADD COLUMN news_score INTEGER DEFAULT 0")
    except:
        pass
    try:
        conn.execute("ALTER TABLE documents ADD COLUMN news_reason TEXT DEFAULT ''")
    except:
        pass

    # get unsummarized docs
    remaining = conn.execute("""
        SELECT id, filename, condensed, full_text
        FROM documents WHERE interest_score >= 40 AND (ai_summary IS NULL OR ai_summary = '')
        ORDER BY interest_score DESC
    """).fetchall()

    # get already summarized but unranked
    unranked = conn.execute("""
        SELECT id, filename, ai_summary
        FROM documents WHERE ai_summary != '' AND (news_score IS NULL OR news_score = 0)
    """).fetchall()

    print(f"Remaining to summarize: {len(remaining)}")
    print(f"Already summarized, need ranking: {len(unranked)}")

    # summarize remaining (5 concurrent to stay under rate limits)
    if remaining:
        print("\n--- Summarizing ---")
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(summarize_doc, row): row for row in remaining}
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                doc_id, fname, summary, news_score, reason = future.result()
                if summary:
                    conn.execute("UPDATE documents SET ai_summary=?, news_score=?, news_reason=? WHERE id=?",
                                 (summary, news_score, reason, doc_id))
                    conn.commit()
                    print(f"  [{i+1}/{len(remaining)}] {fname} (news:{news_score})")

    # rank already-summarized docs
    if unranked:
        print("\n--- Ranking existing summaries ---")
        def rank_existing(row):
            doc_id, fname, summary = row
            resp = call_gemini(RANK_PROMPT + summary, max_tokens=100)
            score, reason = 0, ""
            if resp:
                try:
                    j = resp.strip()
                    if j.startswith("```"):
                        j = j.split("\n", 1)[1].rsplit("```", 1)[0]
                    data = json.loads(j)
                    score = int(data.get("score", 0))
                    reason = data.get("reason", "")
                except:
                    pass
            return doc_id, fname, score, reason

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(rank_existing, row): row for row in unranked}
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                doc_id, fname, score, reason = future.result()
                conn.execute("UPDATE documents SET news_score=?, news_reason=? WHERE id=?",
                             (score, reason, doc_id))
                conn.commit()
                print(f"  [{i+1}/{len(unranked)}] {fname} (news:{score})")

    # final stats
    print("\n--- Top 15 most newsworthy ---")
    top = conn.execute("""
        SELECT filename, news_score, news_reason, substr(ai_summary, 1, 150)
        FROM documents WHERE news_score > 0 ORDER BY news_score DESC LIMIT 15
    """).fetchall()
    for fname, score, reason, preview in top:
        print(f"  [{score}] {fname}: {reason}")

    conn.close()
    print("\nDone!")

if __name__ == "__main__":
    main()
