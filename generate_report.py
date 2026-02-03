import sqlite3, json, os, urllib.request

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")
API_KEY = os.environ.get("GEMINI_API_KEY")
OUTPUT = os.path.join(os.path.dirname(__file__), "report.html")

def call_gemini(prompt):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={API_KEY}"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.4, "maxOutputTokens": 8192}
    })
    req = urllib.request.Request(url, data=payload.encode(), headers={"Content-Type": "application/json"})
    resp = urllib.request.urlopen(req, timeout=120)
    result = json.loads(resp.read())
    if "candidates" not in result:
        print("API response:", json.dumps(result, indent=2)[:2000])
        raise Exception("No candidates in response")
    return result["candidates"][0]["content"]["parts"][0]["text"]

def main():
    if not API_KEY:
        print("ERROR: GEMINI_API_KEY not set")
        return

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT filename, news_score, condensed, ai_summary
        FROM documents WHERE news_score > 0
        ORDER BY news_score DESC LIMIT 5
    """).fetchall()

    # use AI summaries only (raw text triggers safety filters)
    context_parts = []
    for fname, score, condensed, summary in rows:
        context_parts.append(f"""
--- DOCUMENT: {fname} (Newsworthiness: {score}/100) ---
{summary}
""")

    context = "\n".join(context_parts)

    prompt = f"""You are a legal analyst reviewing summaries of the top 5 most newsworthy declassified DOJ documents from the Epstein Files Transparency Act release (public court records).

Your task: For each named public figure (politicians, executives, celebrities, etc.) mentioned in these document summaries, compile all allegations, claims, and connections described in the documents.

For each person:
1. Full name and public role
2. All specific allegations from the documents (factual reporting of what the documents state)
3. Source document reference
4. Severity assessment (how significant the allegations are)

Output a COMPLETE standalone HTML document:
- Dark theme (background #0d1117, text #c9d1d9, links #58a6ff)
- Each person gets their own section with colored header (red = serious allegations, orange = financial connections, blue = peripheral mentions)
- Bold key names and phrases
- Bullet points for each allegation
- Table of contents at top
- Blockquote style for paraphrased document content
- Note: these are publicly released government documents being reported factually

Output ONLY valid HTML. No markdown. No code fences.

Document summaries:

{context}"""

    print("Sending to Gemini 2.5 Flash...")
    html = call_gemini(prompt)

    # strip markdown code fences if present
    if html.startswith("```"):
        html = html.split("\n", 1)[1]
    if html.endswith("```"):
        html = html.rsplit("```", 1)[0]

    with open(OUTPUT, "w") as f:
        f.write(html)

    print(f"Report saved to {OUTPUT}")
    print(f"Length: {len(html)} chars")

if __name__ == "__main__":
    main()
