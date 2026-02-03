import sqlite3, re, os

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")

def classify(text, page_count):
    text = text or ""
    stripped = text.strip()
    char_count = len(stripped)

    # empty / scanned image
    if char_count < 20:
        return "empty", 0

    # OCR garbage: high ratio of symbols/punctuation to letters
    letters = sum(1 for c in stripped if c.isalpha())
    if char_count > 0 and letters / char_count < 0.3:
        return "scan_garbage", 0

    lower = stripped.lower()

    # detect document type and assign interest score
    score = 0
    doc_type = "other"

    # emails / memos
    if re.search(r'^(from:|sent:|to:|subject:|date:)', lower, re.MULTILINE):
        doc_type = "email"
        score = 60

    # depositions / testimony
    if re.search(r'\b(deposition|testimony|grand jury|Q\.\s|A\.\s|WITNESS|sworn)', lower):
        doc_type = "deposition" if doc_type == "other" else doc_type
        score = max(score, 70)

    # FBI / law enforcement reports
    if re.search(r'\b(fbi|case number|case summary|investigation|indicted|arrest|convicted|bureau)', lower):
        doc_type = "law_enforcement"
        score = max(score, 80)

    # legal filings
    if re.search(r'\b(court|motion|order|plaintiff|defendant|docket|filed|judge|verdict|sentence)', lower):
        doc_type = "legal" if doc_type == "other" else doc_type
        score = max(score, 50)

    # phone/fax logs
    if re.search(r'(fax activity|call detail|call log|phone.*record)', lower):
        doc_type = "phone_records"
        score = 20

    # file listings / photo indexes
    if lower.count('.tif') > 3 or lower.count('.jpg') > 3 or lower.count('.pdf') > 5:
        doc_type = "file_listing"
        score = 10

    # evidence/property lists
    if re.search(r'(evidence|property|contents|item quantity)', lower):
        doc_type = "evidence_list"
        score = 30

    # boost for key names
    names = ['epstein', 'maxwell', 'ghislaine', 'prince andrew', 'giuffre', 'roberts',
             'dershowitz', 'clinton', 'trump', 'black', 'wexner', 'brunel', 'dubin']
    name_hits = sum(1 for n in names if n in lower)
    score += name_hits * 10

    # boost for substantive content length
    if char_count > 1000:
        score += 10
    if char_count > 5000:
        score += 10

    score = min(score, 100)

    if doc_type == "other" and score < 20:
        # short docs with no identifiable type
        if char_count < 100:
            return "minimal", score
        doc_type = "document"

    return doc_type, score

def main():
    conn = sqlite3.connect(DB_PATH)

    # add columns if not exist
    try:
        conn.execute("ALTER TABLE documents ADD COLUMN doc_type TEXT DEFAULT ''")
    except:
        pass
    try:
        conn.execute("ALTER TABLE documents ADD COLUMN interest_score INTEGER DEFAULT 0")
    except:
        pass

    rows = conn.execute("SELECT id, full_text, page_count FROM documents").fetchall()
    print(f"Classifying {len(rows)} documents...")

    counts = {}
    for doc_id, text, page_count in rows:
        doc_type, score = classify(text, page_count)
        conn.execute("UPDATE documents SET doc_type=?, interest_score=? WHERE id=?", (doc_type, score, doc_id))
        counts[doc_type] = counts.get(doc_type, 0) + 1

    conn.commit()

    print("\nDocument types:")
    for dtype, count in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {dtype}: {count}")

    interesting = conn.execute("SELECT COUNT(*) FROM documents WHERE interest_score >= 40").fetchone()[0]
    print(f"\n{interesting} documents with interest score >= 40 (out of {len(rows)})")

    print("\nTop 10 most interesting:")
    top = conn.execute("SELECT filename, doc_type, interest_score, substr(full_text, 1, 120) FROM documents ORDER BY interest_score DESC LIMIT 10").fetchall()
    for fname, dtype, score, preview in top:
        print(f"  [{score}] {fname} ({dtype}) — {preview[:80]}...")

    conn.close()

if __name__ == "__main__":
    main()
