# short_merge.py
import json, re
from pathlib import Path
from collections import defaultdict

BASE = Path("data/processed")
CHUNKS = BASE / "chunks.jsonl"
OUT = BASE / "chunks_cleaned.jsonl"

MIN_JOIN = 150  # 150자 미만 청크 병합 기준

BAD_TOKENS = re.compile(r"(?:\s*(?:판결|주문|이유|와|로)\s*){1,}$")
ELLIPSIS = re.compile(r"\s*\.\.\.\s*")

def clean_tail(s):
    s = ELLIPSIS.sub(" … ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = BAD_TOKENS.sub("", s).strip()
    return s

def load_chunks(path):
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            obj = json.loads(line)
            text = clean_tail(obj["text"])
            if text:
                obj["text"] = text
                obj["char_len"] = len(text)
                rows.append(obj)
    return rows

def merge_short(rows):
    key = lambda r: (r["case_uid"], r["section_name"])
    grouped = defaultdict(list)
    for r in rows:
        grouped[key(r)].append(r)

    merged = []
    for k, lst in grouped.items():
        lst.sort(key=lambda r: r["chunk_index"])
        buf = []
        for r in lst:
            if buf and len(r["text"]) < MIN_JOIN:
                buf[-1]["text"] = (buf[-1]["text"] + " " + r["text"]).strip()
                buf[-1]["char_len"] = len(buf[-1]["text"])
            else:
                buf.append(r)
        for i, r in enumerate(buf):
            r["chunk_index"] = i
        merged.extend(buf)
    return merged

def main():
    rows = load_chunks(CHUNKS)
    print(f"[LOAD] {len(rows)} chunks")
    merged = merge_short(rows)
    print(f"[MERGE] {len(merged)} chunks after merge")
    with OUT.open("w", encoding="utf-8") as f:
        for r in merged:
            json.dump(r, f, ensure_ascii=False)
            f.write("\n")
    print(f"[DONE] saved → {OUT}")

if __name__ == "__main__":
    main()
