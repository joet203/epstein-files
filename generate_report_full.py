import sqlite3, json, os, urllib.request, time

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")
API_KEY = os.environ.get("GEMINI_API_KEY")
OUTPUT = os.path.join(os.path.dirname(__file__), "report.html")

def call_gemini(prompt, retries=3):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={API_KEY}"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.4, "maxOutputTokens": 16384}
    })
    req = urllib.request.Request(url, data=payload.encode(), headers={"Content-Type": "application/json"})
    for attempt in range(retries):
        try:
            resp = urllib.request.urlopen(req, timeout=120)
            result = json.loads(resp.read())
            if "candidates" not in result:
                reason = result.get("promptFeedback", {}).get("blockReason", "unknown")
                print(f"  Blocked: {reason}")
                return None
            return result["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            if "429" in str(e):
                time.sleep(10)
            else:
                print(f"  Error: {e}")
                time.sleep(3)
    return None

BATCH_PROMPT = """You are a legal analyst reviewing summaries of declassified DOJ Epstein case documents (public court records released under the Epstein Files Transparency Act).

Extract all allegations, claims, and connections involving named PUBLIC FIGURES (politicians, billionaires, celebrities, executives, royalty, lawyers, etc).

For each person, output a JSON object with:
- "name": full name
- "role": their public role/title
- "allegations": list of specific factual claims from the documents
- "sources": list of source document filenames
- "severity": "critical" | "high" | "medium" | "low"

Output ONLY a JSON array. No markdown, no explanation.

Document summaries:

"""

RENDER_PROMPT = """You are generating a report from structured data about public figures named in the DOJ Epstein files (publicly released court documents).

Generate a COMPLETE standalone HTML document:
- Dark theme: background #0d1117, text #c9d1d9, links #58a6ff
- Title: "Epstein Files — Public Figures Named in DOJ Documents"
- Subtitle with doc count and date
- Table of contents at top linking to each person (sorted by severity)
- Each person gets their own section with:
  - Colored header: #da3633 for critical, #f0883e for high, #8b949e for medium/low
  - Their name, role, severity badge
  - Bullet points for each allegation
  - Source document references in smaller gray text
- Blockquote style for key allegations (dark background, left border)
- Professional investigative journalism style
- Note at bottom: "Based on publicly released DOJ documents under the Epstein Files Transparency Act"

Output ONLY valid HTML. No markdown code fences.

Data:

"""

def main():
    if not API_KEY:
        print("ERROR: GEMINI_API_KEY not set")
        return

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT filename, news_score, ai_summary
        FROM documents WHERE ai_summary != '' AND news_score >= 50
        ORDER BY news_score DESC
    """).fetchall()
    print(f"Processing {len(rows)} documents in batches...")

    # batch into groups of 20
    all_people = []
    batch_size = 10
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        context = "\n".join(f"--- {fname} (score:{score}) ---\n{summary}\n" for fname, score, summary in batch)

        print(f"  Batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1} ({len(batch)} docs)...")
        result = call_gemini(BATCH_PROMPT + context)
        if not result:
            continue

        # parse JSON — try to fix truncated output
        try:
            text = result.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0]
            # try as-is first
            try:
                people = json.loads(text)
            except:
                # try closing truncated JSON
                for fix in [']', '"}]', '"}]}]', '"]}]']:
                    try:
                        people = json.loads(text + fix)
                        break
                    except:
                        continue
                else:
                    # extract individual objects with regex
                    import re
                    objects = re.findall(r'\{[^{}]+\}', text)
                    people = []
                    for obj in objects:
                        try:
                            p = json.loads(obj)
                            if "name" in p:
                                people.append(p)
                        except:
                            pass
            all_people.extend(people)
            print(f"    Found {len(people)} people")
        except Exception as e:
            print(f"    Parse error: {e}")

        time.sleep(2)

    # deduplicate by name
    merged = {}
    for p in all_people:
        name = p.get("name", "").strip()
        if not name:
            continue
        key = name.lower()
        if key in merged:
            merged[key]["allegations"] = list(set(merged[key]["allegations"] + p.get("allegations", [])))
            merged[key]["sources"] = list(set(merged[key]["sources"] + p.get("sources", [])))
            # keep highest severity
            sev_order = {"critical": 4, "high": 3, "medium": 2, "low": 1}
            if sev_order.get(p.get("severity", "low"), 0) > sev_order.get(merged[key]["severity"], 0):
                merged[key]["severity"] = p["severity"]
        else:
            merged[key] = p

    people_list = sorted(merged.values(), key=lambda p: {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(p.get("severity", "low"), 4))
    print(f"\nTotal unique people: {len(people_list)}")

    # render HTML
    print("Generating HTML report...")
    data_str = json.dumps(people_list, indent=2)
    html = call_gemini(RENDER_PROMPT + data_str)
    if not html:
        # fallback: save raw JSON
        print("Render blocked, saving raw data")
        html = f"<pre>{data_str}</pre>"

    if html.startswith("```"):
        html = html.split("\n", 1)[1]
    if html.endswith("```"):
        html = html.rsplit("```", 1)[0]

    with open(OUTPUT, "w") as f:
        f.write(html)

    print(f"Report saved to {OUTPUT} ({len(html)} chars)")
    print(f"People profiled: {len(people_list)}")
    for p in people_list[:10]:
        print(f"  [{p.get('severity','?')}] {p.get('name','')} — {len(p.get('allegations',[]))} allegations")

if __name__ == "__main__":
    main()
