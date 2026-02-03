import re, sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")

def clean_text(text):
    if not text or len(text.strip()) < 10:
        return text

    lines = text.split("\n")
    cleaned = []
    for line in lines:
        s = line.strip()
        if not s:
            continue
        # skip standalone line numbers (deposition format)
        if re.match(r'^\d{1,3}$', s):
            continue
        # skip bates stamps
        if re.match(r'^EFTA\d+$', s):
            continue
        # skip page headers like "Page X of Y"
        if re.match(r'^Page \d+ of \d+', s):
            continue
        # skip "ITEM WAS NOT SCANNED" artifacts
        if 'WAS NOT SCANNED' in s.upper():
            continue
        cleaned.append(s)

    text = "\n".join(cleaned)

    # collapse excessive whitespace / newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    # rejoin lines that are broken mid-sentence (lowercase continuation)
    text = re.sub(r'(?<=[a-z,])\n(?=[a-z])', ' ', text)

    return text.strip()

def main():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT id, text FROM pages").fetchall()
    print(f"Cleaning {len(rows)} pages...")

    updated = 0
    for row_id, text in rows:
        cleaned = clean_text(text)
        if cleaned != text:
            conn.execute("UPDATE pages SET text=? WHERE id=?", (cleaned, row_id))
            updated += 1

    conn.commit()

    # also update full_text on documents
    docs = conn.execute("SELECT id FROM documents").fetchall()
    for (doc_id,) in docs:
        pages = conn.execute("SELECT text FROM pages WHERE doc_id=? ORDER BY page_num", (doc_id,)).fetchall()
        full = "\n\n".join(p[0] for p in pages)
        conn.execute("UPDATE documents SET full_text=? WHERE id=?", (full, doc_id))
    conn.commit()

    # rebuild FTS index
    conn.execute("DROP TABLE IF EXISTS search")
    conn.execute("CREATE VIRTUAL TABLE search USING fts5(filename, page_num, text)")
    idx_rows = conn.execute("SELECT p.id, d.filename, p.page_num, p.text FROM pages p JOIN documents d ON d.id = p.doc_id").fetchall()
    for r in idx_rows:
        conn.execute("INSERT INTO search(rowid, filename, page_num, text) VALUES (?,?,?,?)", r)
    conn.commit()

    print(f"Updated {updated} pages, rebuilt search index ({len(idx_rows)} rows)")
    conn.close()

if __name__ == "__main__":
    main()
