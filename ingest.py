import os, sys, sqlite3, fitz, glob

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")
BASE_DIR = os.path.dirname(__file__)

def init_db(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY,
        filename TEXT UNIQUE,
        filepath TEXT,
        page_count INTEGER,
        full_text TEXT,
        bates_start TEXT,
        bates_end TEXT
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS pages (
        id INTEGER PRIMARY KEY,
        doc_id INTEGER REFERENCES documents(id),
        page_num INTEGER,
        text TEXT
    )""")
    conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(filename, page_num, text)")
    conn.execute("""CREATE TRIGGER IF NOT EXISTS pages_ai AFTER INSERT ON pages BEGIN
        INSERT INTO search(rowid, filename, page_num, text)
        SELECT new.id, d.filename, new.page_num, new.text FROM documents d WHERE d.id = new.doc_id;
    END""")
    conn.commit()

def parse_dat(dat_path):
    bates = {}
    if not os.path.exists(dat_path):
        return bates
    with open(dat_path, "r", encoding="utf-8", errors="replace") as f:
        lines = f.readlines()
    for line in lines[1:]:
        parts = line.strip().split("þ")
        parts = [p for p in parts if p]
        if len(parts) >= 2:
            bates[parts[0]] = parts[1] if len(parts) > 1 else ""
    return bates

def ingest():
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    bates_map = {}
    for dat in glob.glob(os.path.join(BASE_DIR, "**", "*.DAT"), recursive=True):
        bates_map.update(parse_dat(dat))

    pdfs = glob.glob(os.path.join(BASE_DIR, "**", "*.pdf"), recursive=True)
    print(f"Found {len(pdfs)} PDFs")

    for pdf_path in sorted(pdfs):
        filename = os.path.basename(pdf_path)
        existing = conn.execute("SELECT id FROM documents WHERE filename=?", (filename,)).fetchone()
        if existing:
            print(f"  skip {filename} (already indexed)")
            continue

        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            print(f"  ERROR opening {filename}: {e}")
            continue

        pages_text = []
        full_text_parts = []
        for i in range(len(doc)):
            text = doc[i].get_text()
            pages_text.append(text)
            full_text_parts.append(text)

        bates_start = filename.replace(".pdf", "")
        bates_end = bates_map.get(bates_start, "")

        conn.execute(
            "INSERT INTO documents (filename, filepath, page_count, full_text, bates_start, bates_end) VALUES (?,?,?,?,?,?)",
            (filename, pdf_path, len(doc), "\n".join(full_text_parts), bates_start, bates_end)
        )
        doc_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        for i, text in enumerate(pages_text):
            conn.execute("INSERT INTO pages (doc_id, page_num, text) VALUES (?,?,?)", (doc_id, i + 1, text))

        print(f"  indexed {filename} ({len(doc)} pages)")
        doc.close()

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0]
    pages = conn.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    print(f"\nDone: {total} documents, {pages} pages indexed")
    conn.close()

if __name__ == "__main__":
    ingest()
