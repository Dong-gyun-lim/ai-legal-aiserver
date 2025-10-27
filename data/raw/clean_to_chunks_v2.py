# -*- coding: utf-8 -*-
"""
[역할]
- 섹션 텍스트를 문장 단위로 분할한 뒤, 길이(문자 수) 기준으로 묶어 '청크'를 생성한다.
- RAG 전처리의 2단계: '섹션 → 청크(JSONL)'로 만들어 임베딩/벡터DB에 넣을 준비를 함.

[입력]
- data/processed/clean_sections_v2.jsonl
  (각 줄: {"case_uid":..., "section_name":..., "text":"..."} )

[출력]
- data/processed/chunks.jsonl (혹은 너희가 만든 최종 파일: chunks_cleaned.jsonl)
  (각 줄: {"case_uid":..., "section_name":..., "chunk_index":0.., "text":"...", "char_len":..., "hash":"..."} )

[파라미터]
- --max / --min / --hard 로 청크 길이 조절(문자 기준).
  예: --max 900 --min 500 --hard 1200

[언제 다시 쓰나?]
- 섹션 파일이 갱신되었거나, 청크 길이 전략을 바꾸고 싶을 때.
- 청크 생성 규칙(문장분리, normalize)을 수정한 뒤 재생성할 때.

[의존/주의]
- 문장분리기가 단순 규칙 기반이라 일부 문장 경계가 덜 자연스러울 수 있음(실무에서 후속 튜닝 가능).
"""




# data/raw/clean_to_chunks_v2.py
import os, json, re, argparse, hashlib
from pathlib import Path
from html import unescape

def normalize(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    # 공백/개행 정리
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\s*\n\s*", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = re.sub(r"[ \u00A0\u200b\u200c\u200d]+", " ", s)  # NBSP, ZW* 제거
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()

def split_sentences(text: str) -> list[str]:
    """
    아주 단순한 한국어 문장 분할기.
    '다.', '요.', '.', '!', '?', '…', '?!' 등에서 끊어줌. (따옴표/괄호 닫힘 포함)
    """
    if not text:
        return []

    # 끝나는 패턴들(따옴표/괄호 닫힘과 조합)
    end = r"(?:\.\.\.|…|[\.!?]|다\.|요\.)"
    closer = r"[\"'”’〉》）)]?"
    pat = re.compile(rf"(.+?{end}{closer})(\s+|$)")
    out, i = [], 0
    for m in pat.finditer(text):
        out.append(text[i:m.end(1)].strip())
        i = m.end()
    tail = text[i:].strip()
    if tail:
        out.append(tail)
    return out

def chunk_by_length(sentences: list[str], max_chars=900, min_chars=500, hard_max=1200) -> list[str]:
    """
    문장 리스트를 길이 제약에 맞춰 묶어 청크 생성.
    - 기본 목표: 700~1000자(기본 min/max) 사이
    - 문장이 너무 길면 hard_max 기준으로 단어 단위 강제 분할
    """
    chunks, buf = [], []
    buf_len = 0

    def flush():
        nonlocal buf, buf_len
        if buf:
            chunks.append(" ".join(buf).strip())
            buf, buf_len = [], 0

    for s in sentences:
        s = s.strip()
        if not s:
            continue
        # 문장 하나가 hard_max를 초과하면 단어 단위 강제 분할
        if len(s) > hard_max:
            words = s.split()
            cur = []
            cur_len = 0
            for w in words:
                if (cur_len + len(w) + 1) > hard_max:
                    # 현재까지를 하나로
                    if cur:
                        # 남은 버퍼 처리
                        if buf_len and (buf_len + cur_len) > max_chars:
                            flush()
                        buf.append(" ".join(cur))
                        buf_len += cur_len
                        flush()
                    cur, cur_len = [], 0
                cur.append(w)
                cur_len += len(w) + 1
            if cur:
                if buf_len and (buf_len + cur_len) > max_chars:
                    flush()
                buf.append(" ".join(cur))
                buf_len += cur_len
                flush()
            continue

        # 일반 문장 누적
        if (buf_len + len(s) + 1) <= max_chars:
            buf.append(s)
            buf_len += len(s) + 1
        else:
            # 최소 길이를 만족 못하면 조금 더 붙여서 보냄
            if buf_len < min_chars:
                buf.append(s)
                buf_len += len(s) + 1
                flush()
            else:
                flush()
                buf.append(s)
                buf_len = len(s) + 1

    flush()
    return [normalize(c) for c in chunks if normalize(c)]

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def main(
    in_path="data/processed/clean_sections_v2.jsonl",
    out_path="data/processed/chunks.jsonl",
    max_chars=900,
    min_chars=500,
    hard_max=1200,
):
    in_p = Path(in_path)
    out_p = Path(out_path)
    out_p.parent.mkdir(parents=True, exist_ok=True)

    if not in_p.exists():
        raise FileNotFoundError(f"입력 파일이 없습니다: {in_p}")

    # 1) 같은 사건/같은 섹션을 먼저 이어붙여서 큰 섹션 텍스트 준비
    grouped: dict[tuple[str, str], list[str]] = {}
    total_lines = 0
    with in_p.open("r", encoding="utf-8") as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            uid = str(obj.get("case_uid", "")).strip()
            sec = str(obj.get("section_name", "")).strip() or "전문"
            txt = normalize(str(obj.get("text", "")))
            if not uid or not txt:
                continue
            grouped.setdefault((uid, sec), []).append(txt)

    # 2) 이어붙인 섹션을 문장→청크화
    records = 0
    with out_p.open("w", encoding="utf-8") as w:
        for (uid, sec), parts in grouped.items():
            merged = normalize(" ".join(parts))
            sents = split_sentences(merged)
            chunks = chunk_by_length(
                sents, max_chars=max_chars, min_chars=min_chars, hard_max=hard_max
            )
            for ci, ch in enumerate(chunks):
                rec = {
                    "case_uid": uid,
                    "section_name": sec,
                    "chunk_index": ci,
                    "text": ch,
                    "char_len": len(ch),
                    "hash": sha1(f"{uid}|{sec}|{ci}|{len(ch)}"),
                }
                w.write(json.dumps(rec, ensure_ascii=False) + "\n")
                records += 1

    print(f"[DONE] 입력 {total_lines}행 → 섹션 {len(grouped)}개 → 청크 {records}개 저장")
    print(f"[OUT ] {out_p}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", default="data/processed/clean_sections_v2.jsonl")
    ap.add_argument("--out", dest="out_path", default="data/processed/chunks.jsonl")
    ap.add_argument("--max", dest="max_chars", type=int, default=900)
    ap.add_argument("--min", dest="min_chars", type=int, default=500)
    ap.add_argument("--hard", dest="hard_max", type=int, default=1200)
    args = ap.parse_args()
    main(args.in_path, args.out_path, args.max_chars, args.min_chars, args.hard_max)
