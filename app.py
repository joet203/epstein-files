import os, sys, sqlite3, json, re, random
from flask import Flask, request, jsonify, send_from_directory, redirect

app = Flask(__name__, static_folder="static")
DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")
DS9_IDS_PATH = os.path.join(os.path.dirname(__file__), "dataset9_ids.txt")

# load dataset 9 IDs into memory
DS9_IDS = []
if os.path.exists(DS9_IDS_PATH):
    with open(DS9_IDS_PATH) as f:
        DS9_IDS = [line.strip() for line in f if line.strip()]

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/stats")
def stats():
    conn = get_db()
    docs = conn.execute("SELECT COUNT(*) c FROM documents").fetchone()["c"]
    pages = conn.execute("SELECT COUNT(*) c FROM pages").fetchone()["c"]
    conn.close()
    return jsonify({"documents": docs, "pages": pages})

@app.route("/api/documents")
def documents():
    conn = get_db()
    doc_type = request.args.get("type", "")
    min_score = int(request.args.get("min_score", 0))
    q = "SELECT id, filename, page_count, bates_start, bates_end, doc_type, interest_score FROM documents WHERE interest_score >= ?"
    params = [min_score]
    if doc_type:
        q += " AND doc_type = ?"
        params.append(doc_type)
    q += " ORDER BY interest_score DESC, filename"
    rows = conn.execute(q, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/highlights")
def highlights():
    conn = get_db()
    cur = conn.cursor()
    cur.row_factory = None
    rows = cur.execute("""
        SELECT d.id, d.filename, d.page_count, d.doc_type, d.interest_score,
               COALESCE(d.ai_summary, substr(COALESCE(d.condensed, d.full_text), 1, 300)),
               COALESCE(d.news_score, 0), COALESCE(d.news_reason, '')
        FROM documents d
        WHERE d.interest_score >= 40
        ORDER BY COALESCE(d.news_score, 0) DESC, d.interest_score DESC
    """).fetchall()
    conn.close()
    return jsonify([{"id": r[0], "filename": r[1], "page_count": r[2], "doc_type": r[3],
                     "interest_score": r[4], "preview": r[5],
                     "news_score": r[6], "news_reason": r[7]} for r in rows])

@app.route("/api/doc_types")
def doc_types():
    conn = get_db()
    rows = conn.execute("SELECT doc_type, COUNT(*) c FROM documents GROUP BY doc_type ORDER BY c DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/document/<int:doc_id>")
def document(doc_id):
    conn = get_db()
    doc = conn.execute("SELECT * FROM documents WHERE id=?", (doc_id,)).fetchone()
    if not doc:
        return jsonify({"error": "not found"}), 404
    pages = conn.execute("SELECT page_num, text FROM pages WHERE doc_id=? ORDER BY page_num", (doc_id,)).fetchall()
    conn.close()
    d = dict(doc)
    d["condensed"] = d.get("condensed", "") or ""
    return jsonify({"doc": d, "pages": [dict(p) for p in pages]})

def _build_fts_query(q):
    tokens = q.split()
    parts = []
    for t in tokens:
        clean = re.sub(r'[^\w]', '', t)
        if clean:
            parts.append(clean + "*")
    return " AND ".join(parts)

@app.route("/api/search")
def search():
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    conn = get_db()
    cur = conn.cursor()
    cur.row_factory = None

    # try FTS prefix match first
    fts_q = _build_fts_query(q)
    rows = []
    if fts_q:
        try:
            rows = cur.execute("""
                SELECT p.doc_id, s.filename, p.page_num,
                       snippet(search, 2, '<mark>', '</mark>', '...', 40)
                FROM search s
                JOIN pages p ON p.id = s.rowid
                WHERE search MATCH ?
                ORDER BY rank
                LIMIT 100
            """, (fts_q,)).fetchall()
        except Exception:
            rows = []

    # fallback: LIKE-based fuzzy search
    if not rows:
        like_pattern = "%" + "%".join(q.split()) + "%"
        rows = cur.execute("""
            SELECT p.doc_id, d.filename, p.page_num, substr(p.text, max(1, instr(lower(p.text), lower(?)) - 80), 200)
            FROM pages p
            JOIN documents d ON d.id = p.doc_id
            WHERE lower(p.text) LIKE lower(?)
            ORDER BY p.doc_id, p.page_num
            LIMIT 100
        """, (q.split()[0], like_pattern)).fetchall()

    conn.close()
    return jsonify([{"doc_id": r[0], "filename": r[1], "page_num": r[2], "snippet": r[3]} for r in rows])

@app.route("/api/summarize", methods=["POST"])
def summarize():
    data = request.json
    text = data.get("text", "")[:8000]
    query = data.get("query", "")

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return jsonify({"error": "GEMINI_API_KEY not set"}), 500

    import urllib.request, urllib.parse
    prompt = f"You are analyzing declassified DOJ documents from the Epstein case. The user searched for: \"{query}\"\n\nHere is the relevant document text:\n\n{text}\n\nProvide a clear, factual summary of what this document contains and how it relates to the search query. Be specific about names, dates, and events mentioned."

    payload = json.dumps({
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.3, "maxOutputTokens": 1024}
    })

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"
    req = urllib.request.Request(url, data=payload.encode(), headers={"Content-Type": "application/json"})
    try:
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read())
        summary = result["candidates"][0]["content"]["parts"][0]["text"]
        return jsonify({"summary": summary})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/pdf/<int:doc_id>")
def serve_pdf(doc_id):
    conn = get_db()
    doc = conn.execute("SELECT filepath FROM documents WHERE id=?", (doc_id,)).fetchone()
    conn.close()
    if not doc:
        return "not found", 404
    from flask import send_file
    return send_file(doc["filepath"], mimetype="application/pdf")

@app.route("/browser")
def browser():
    return send_from_directory("static", "browser.html")

@app.route("/api/ds9/random")
def ds9_random():
    if not DS9_IDS:
        return jsonify({"error": "Dataset 9 IDs not loaded"}), 500
    file_id = random.choice(DS9_IDS)
    url = f"https://www.justice.gov/epstein/files/DataSet%209/{file_id}.pdf"
    return jsonify({"id": file_id, "url": url})

@app.route("/api/ds9/stats")
def ds9_stats():
    return jsonify({"count": len(DS9_IDS), "sample": DS9_IDS[:10] if DS9_IDS else []})

@app.route("/api/ds9/search")
def ds9_search():
    q = request.args.get("q", "").strip().upper()
    if not q:
        return jsonify([])
    matches = [fid for fid in DS9_IDS if q in fid][:100]
    return jsonify([{"id": fid, "url": f"https://www.justice.gov/epstein/files/DataSet%209/{fid}.pdf"} for fid in matches])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001, debug=False)
