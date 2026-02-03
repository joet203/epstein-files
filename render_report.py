"""Render the report HTML directly from DB data — no Gemini needed for this step."""
import sqlite3, json, os, urllib.request, time, re

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")
API_KEY = os.environ.get("GEMINI_API_KEY")
OUTPUT = os.path.join(os.path.dirname(__file__), "report.html")

def call_gemini(prompt):
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={API_KEY}"
    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 16384}
    })
    req = urllib.request.Request(url, data=payload.encode(), headers={"Content-Type": "application/json"})
    for attempt in range(3):
        try:
            resp = urllib.request.urlopen(req, timeout=120)
            result = json.loads(resp.read())
            if "candidates" not in result:
                flush(f"  Blocked: {result.get('promptFeedback', {}).get('blockReason', '?')}")
                return None
            return result["candidates"][0]["content"]["parts"][0]["text"]
        except Exception as e:
            flush(f"  Retry {attempt+1}: {e}")
            time.sleep(5)
    return None

def extract_people_from_batch(summaries_text):
    prompt = """From these DOJ document summaries, extract named PUBLIC FIGURES only (politicians, billionaires, celebrities, executives — NOT lawyers, agents, or unnamed victims).

For each, return JSON: {"name": "...", "role": "...", "severity": "critical|high|medium|low", "allegations": ["..."], "sources": ["..."]}

Rules:
- Only include people who are subjects of allegations or noteworthy connections
- Skip lawyers (Wigdor, Christensen, Edwards) unless they're accused of wrongdoing
- Skip unnamed victims
- Be specific in allegations
- Output ONLY a JSON array, nothing else

Summaries:
""" + summaries_text
    return call_gemini(prompt)

def parse_json_loose(text):
    if not text:
        return []
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0]
    # try full parse
    try:
        return json.loads(text)
    except:
        pass
    # try closing brackets
    for fix in [']', '"}]', '"]}]', '"]}}]']:
        try:
            return json.loads(text + fix)
        except:
            pass
    # extract objects
    results = []
    for m in re.finditer(r'\{[^{}]{20,}\}', text):
        try:
            obj = json.loads(m.group())
            if "name" in obj:
                results.append(obj)
        except:
            pass
    return results

def merge_people(all_people):
    merged = {}
    for p in all_people:
        name = p.get("name", "").strip()
        if not name or len(name) < 3:
            continue
        key = name.lower()
        if key in merged:
            merged[key]["allegations"] = list(set(merged[key]["allegations"] + p.get("allegations", [])))
            merged[key]["sources"] = list(set(merged[key]["sources"] + p.get("sources", [])))
            sev = {"critical": 4, "high": 3, "medium": 2, "low": 1}
            if sev.get(p.get("severity", "low"), 0) > sev.get(merged[key]["severity"], 0):
                merged[key]["severity"] = p["severity"]
                merged[key]["role"] = p.get("role", merged[key].get("role", ""))
        else:
            merged[key] = {
                "name": name,
                "role": p.get("role", ""),
                "severity": p.get("severity", "low"),
                "allegations": p.get("allegations", []),
                "sources": p.get("sources", [])
            }
    return sorted(merged.values(), key=lambda x: {"critical": 0, "high": 1, "medium": 2, "low": 3}.get(x["severity"], 4))

def render_html(people):
    sev_colors = {"critical": "#da3633", "high": "#f0883e", "medium": "#8b949e", "low": "#484f58"}
    sev_labels = {"critical": "CRITICAL", "high": "HIGH", "medium": "MEDIUM", "low": "LOW"}

    toc = ""
    sections = ""
    for i, p in enumerate(people):
        color = sev_colors.get(p["severity"], "#484f58")
        label = sev_labels.get(p["severity"], "LOW")
        pid = f"person-{i}"

        toc += f'<li><a href="#{pid}" style="color:{color}">{p["name"]}</a> <span style="color:{color};font-size:11px">({label})</span></li>\n'

        allegations_html = ""
        for a in p["allegations"]:
            allegations_html += f'<li>{a}</li>\n'

        sources_html = ", ".join(p.get("sources", []))

        sections += f"""
<div class="person-section" id="{pid}">
    <div class="person-header" style="border-left: 4px solid {color}; padding-left: 16px; margin-bottom: 16px;">
        <h3 style="color:{color}; margin:0; font-size:20px;">{p["name"]}
            <span style="background:{color}; color:#fff; padding:2px 8px; border-radius:10px; font-size:11px; margin-left:8px; vertical-align:middle;">{label}</span>
        </h3>
        <div style="color:#8b949e; font-size:13px; margin-top:4px;">{p["role"]}</div>
    </div>
    <ul style="line-height:1.8; margin-bottom:12px;">
        {allegations_html}
    </ul>
    <div style="color:#484f58; font-size:12px;">Sources: {sources_html}</div>
</div>
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Epstein Files — Public Figures Report</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ background: #0d1117; color: #c9d1d9; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.6; padding: 40px 20px; }}
a {{ color: #58a6ff; text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
.container {{ max-width: 900px; margin: 0 auto; }}
h1 {{ font-size: 28px; color: #f0f6fc; margin-bottom: 8px; }}
.subtitle {{ color: #8b949e; font-size: 14px; margin-bottom: 30px; }}
.toc {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 20px; margin-bottom: 30px; }}
.toc h2 {{ font-size: 16px; color: #f0f6fc; margin-bottom: 12px; }}
.toc ul {{ list-style: none; columns: 2; }}
.toc li {{ margin-bottom: 6px; font-size: 14px; }}
.person-section {{ background: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 20px; margin-bottom: 16px; }}
.person-section ul {{ padding-left: 20px; }}
.person-section li {{ margin-bottom: 8px; font-size: 14px; }}
.footer {{ text-align: center; color: #484f58; font-size: 12px; margin-top: 40px; padding-top: 20px; border-top: 1px solid #21262d; }}
</style>
</head>
<body>
<div class="container">
    <h1>Epstein Files — Public Figures Report</h1>
    <div class="subtitle">{len(people)} public figures identified across 117 declassified DOJ documents</div>

    <div class="toc">
        <h2>Table of Contents</h2>
        <ul>{toc}</ul>
    </div>

    {sections}

    <div class="footer">
        Based on publicly released DOJ documents under the Epstein Files Transparency Act (January 2026)<br>
        Generated from automated analysis of Datasets 2, 3, 4, 5, 6, 7, and 12
    </div>
</div>
</body>
</html>"""

def flush(*args):
    print(*args, flush=True)

def main():
    if not API_KEY:
        flush("ERROR: GEMINI_API_KEY not set")
        return

    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT filename, news_score, ai_summary
        FROM documents WHERE ai_summary != '' AND news_score >= 50
        ORDER BY news_score DESC
    """).fetchall()
    flush(f"Processing {len(rows)} documents...")

    all_people = []
    batch_size = 10
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        context = "\n".join(f"--- {f} (score:{s}) ---\n{summary}\n" for f, s, summary in batch)
        flush(f"  Batch {i//batch_size + 1}/{(len(rows)-1)//batch_size + 1}...")

        result = extract_people_from_batch(context)
        people = parse_json_loose(result)
        all_people.extend(people)
        flush(f"    Extracted {len(people)} people")
        time.sleep(2)

    people = merge_people(all_people)
    flush(f"\nTotal unique public figures: {len(people)}")

    html = render_html(people)
    with open(OUTPUT, "w") as f:
        f.write(html)

    flush(f"Report saved: {OUTPUT} ({len(html)} chars)")
    for p in people[:10]:
        flush(f"  [{p['severity']}] {p['name']} — {len(p['allegations'])} allegations")

if __name__ == "__main__":
    main()
