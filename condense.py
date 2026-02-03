import sqlite3, re, os

DB_PATH = os.path.join(os.path.dirname(__file__), "epstein.db")

# boilerplate patterns to strip
JUNK_PATTERNS = [
    # evidence envelope / chain of custody
    r'(?i)chain of custody.*?(?=\n\n|\Z)',
    r'(?i)evidence envelope.*?(?=\n\n|\Z)',
    r'(?i)enclosure:.*?(?=\n\n|\Z)',
    r'(?i)(?:original|duplicate|enhanced original)\s*$',
    r'(?i)magnetic tape.*?computer disk.*?printed material',
    r'(?i)court authorized intercept.*?(?=\n\n|\Z)',
    # email boilerplate
    r'(?i)please consider the environment before printing.*',
    r'(?i)this communication may contain confidential.*?(?=\n\n|\Z)',
    r'(?i)this e-?mail (?:and any|is|may).*?(?=\n\n|\Z)',
    r'(?i)if you (?:are not|have received) the intended recipient.*?(?=\n\n|\Z)',
    r'(?i)disclaimer:.*?(?=\n\n|\Z)',
    r'(?i)privileged.*?attorney.*?client.*?(?=\n\n|\Z)',
    # scan artifacts
    r'(?i)item\s+was\s+not\s+scanned\s+description',
    # page headers/footers
    r'(?i)grand jury material.*?criminal procedure',
    r'(?i)this document contains neither recommendations nor conclusions of the fbi.*?(?=\n\n|\Z)',
    r'(?i)it is the property of the fbi.*?(?=\n\n|\Z)',
    # repeated exhibit stamps
    r'(?:GM_[A-Z]+_\d+\s*)+',
    # image file listings
    r'(?:[A-Z]+\d+[._]\w+\s*){3,}',
]

def is_junk_page(text):
    s = text.strip()
    if len(s) < 15:
        return True
    letters = sum(1 for c in s if c.isalpha())
    if len(s) > 0 and letters / len(s) < 0.25:
        return True
    # page is just phone records (mostly digits/dates)
    digits = sum(1 for c in s if c.isdigit())
    if len(s) > 50 and digits / len(s) > 0.4:
        return True
    return False

def condense_text(full_text):
    if not full_text:
        return ""
    # split into pages (we joined with \n\n)
    pages = full_text.split("\n\n")
    kept = []
    for page in pages:
        if is_junk_page(page):
            continue
        cleaned = page
        for pat in JUNK_PATTERNS:
            cleaned = re.sub(pat, '', cleaned, flags=re.MULTILINE | re.DOTALL)
        # collapse whitespace
        cleaned = re.sub(r'\n{3,}', '\n\n', cleaned).strip()
        if len(cleaned) > 15:
            kept.append(cleaned)
    return "\n\n".join(kept)

def reclassify(text, condensed, doc_type, page_count):
    if not condensed or len(condensed.strip()) < 30:
        return "empty", 0

    lower = condensed.lower()
    score = 0

    # substantive content markers
    has_names = False
    key_names = ['epstein', 'maxwell', 'ghislaine', 'prince andrew', 'giuffre', 'roberts',
                 'dershowitz', 'clinton', 'trump', 'black', 'wexner', 'brunel', 'dubin',
                 'victim', 'minor', 'underage', 'abuse', 'trafficking']
    name_hits = sum(1 for n in key_names if n in lower)
    if name_hits:
        has_names = True
        score += name_hits * 8

    # dialogue/testimony (Q&A format)
    qa_count = len(re.findall(r'\b[QA]\.\s', condensed))
    if qa_count > 5:
        doc_type = "deposition"
        score += 30

    # narrative content (sentences, not just data)
    sentences = len(re.findall(r'[.!?]\s+[A-Z]', condensed))
    if sentences > 5:
        score += 20
    elif sentences > 2:
        score += 10

    # FBI/law enforcement specifics
    if re.search(r'(?i)(investigation|arrest|surveillance|interview|witness|statement|allegation)', lower):
        score += 20
        if doc_type not in ('deposition', 'email'):
            doc_type = "law_enforcement"

    # legal substance
    if re.search(r'(?i)(plea agreement|indictment|non-prosecution|immunity|cooperat|sentenc)', lower):
        score += 25

    # emails with actual content (not just headers)
    if re.search(r'^(From:|Subject:)', condensed, re.MULTILINE):
        if sentences > 2 or len(condensed) > 300:
            doc_type = "email"
            score += 15

    # demote if mostly tabular/data
    lines = condensed.split('\n')
    short_lines = sum(1 for l in lines if len(l.strip()) < 20)
    if len(lines) > 10 and short_lines / len(lines) > 0.7:
        score = max(score - 20, 0)

    # length bonus
    if len(condensed) > 2000:
        score += 10
    if len(condensed) > 5000:
        score += 10

    score = min(score, 100)
    return doc_type, score

def main():
    conn = sqlite3.connect(DB_PATH)

    try:
        conn.execute("ALTER TABLE documents ADD COLUMN condensed TEXT DEFAULT ''")
    except:
        pass

    rows = conn.execute("SELECT id, full_text, doc_type, page_count FROM documents").fetchall()
    print(f"Condensing {len(rows)} documents...")

    stats = {"removed": 0, "kept": 0, "upgraded": 0, "downgraded": 0}
    for doc_id, full_text, doc_type, page_count in rows:
        condensed = condense_text(full_text)
        new_type, new_score = reclassify(full_text, condensed, doc_type, page_count)
        conn.execute("UPDATE documents SET condensed=?, doc_type=?, interest_score=? WHERE id=?",
                     (condensed, new_type, new_score, doc_id))
        if new_score < 40:
            stats["removed"] += 1
        else:
            stats["kept"] += 1

    conn.commit()

    total_highlights = conn.execute("SELECT COUNT(*) FROM documents WHERE interest_score >= 40").fetchone()[0]
    print(f"\nResults:")
    print(f"  Highlights (score >= 40): {total_highlights}")
    print(f"  Filtered out: {stats['removed']}")

    # show type breakdown of highlights
    types = conn.execute("SELECT doc_type, COUNT(*) c FROM documents WHERE interest_score >= 40 GROUP BY doc_type ORDER BY c DESC").fetchall()
    print("\nHighlight types:")
    for t, c in types:
        print(f"  {t}: {c}")

    # show some samples of the new condensed text
    print("\n--- Sample condensed highlights ---")
    samples = conn.execute("""
        SELECT filename, doc_type, interest_score, substr(condensed, 1, 300)
        FROM documents WHERE interest_score >= 60 ORDER BY RANDOM() LIMIT 5
    """).fetchall()
    for fname, dtype, score, preview in samples:
        print(f"\n[{score}] {fname} ({dtype}):")
        print(preview[:250])

    conn.close()

if __name__ == "__main__":
    main()
