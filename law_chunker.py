# filename: law_chunker.py
import json
import re
import hashlib
from pathlib import Path
from typing import Dict, List, Any

IN_PATH  = Path("out/precedents_cleaned_이혼.json")   # 입력
OUT_PATH = Path("out/chunks_이혼.jsonl")              # 청크 출력 (JSONL)
STAT_PATH= Path("out/chunk_stats_이혼.json")          # 통계

# ─────────────────────────────────────────────────────────────
# 1) 섹션 고정 매핑 (일관성 보장)
#    * 섹션이 없더라도 번호는 바꾸지 않는다.
#    * 필요시 여기만 수정하면 전체 데이터가 같은 인덱스로 유지됨.
SECTION_INDEX: Dict[str, int] = {
    "판결요지":   0,
    "판시사항":   1,
    "주문":      2,
    "판례내용":   4,
    "전문":      5,
    "참조조문":   6,
    "참조판례":   7,
}
# 청크 대상으로 우선 처리할 섹션 우선순위(존재하는 것만 순서대로 처리)
SECTION_ORDER: List[str] = ["판결요지", "판시사항", "주문", "판례내용", "전문", "참조조문", "참조판례"]

# ─────────────────────────────────────────────────────────────
# 2) 청크 크기 정책 (한글 기준 문자 수)
MIN_CHARS = 350     # 너무 짧으면 이웃 병합
SOFT_MAX  = 1200    # 이 값을 넘으면 문장경계로 분할
HARD_MAX  = 1600    # 문장 하나가 너무 길면 비상 분할(절대 초과 금지)

# ─────────────────────────────────────────────────────────────
# ⚠ lookbehind 금지(3.11+ 에러) → 안전한 문장 분리 정규식
SENT_SPLIT = re.compile(r"([\.!?…]|\)|\]|\.)\s+|\n+")
WS = re.compile(r"\s+")

def norm_text(s: str) -> str:
    s = s.replace("\u200b", "").replace("\xa0", " ")
    s = WS.sub(" ", s).strip()
    return s

def split_sentences(text: str) -> List[str]:
    # 문장 경계 대략 분할 후 트림
    parts = [t.strip() for t in SENT_SPLIT.split(text) if t and t.strip()]
    # 너무 짧게 잘린 조각 합치기 (ex. “다.” 같은 꼬리)
    merged: List[str] = []
    buf = ""
    for p in parts:
        if not buf:
            buf = p
            continue
        if len(buf) < 60:   # 꼬리문장 방지
            buf = (buf + " " + p).strip()
        else:
            merged.append(buf)
            buf = p
    if buf:
        merged.append(buf)
    return merged

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def make_chunks_from_text(base_meta: Dict[str, Any], section: str, raw: str):
    """
    주어진 섹션 텍스트를 문장 기준으로 350~1200자 사이의 청크로 생성.
    """
    section_idx = SECTION_INDEX.get(section, 99)  # 알 수 없는 섹션은 99
    sents = split_sentences(norm_text(raw))
    chunks: List[str] = []
    cur = ""

    def flush():
        nonlocal cur
        if cur and cur.strip():
            chunks.append(cur.strip())
        cur = ""

    for st in sents:
        # 너무 긴 단일 문장은 강제 하드컷
        if len(st) > HARD_MAX:
            tmp = st
            while len(tmp) > HARD_MAX:
                cut = tmp[:HARD_MAX]
                idx = cut.rfind(" ")
                if idx < MIN_CHARS:
                    idx = HARD_MAX
                chunks.append(tmp[:idx].strip())
                tmp = tmp[idx:].strip()
            if tmp:
                if len(cur) + 1 + len(tmp) <= SOFT_MAX:
                    cur = (cur + " " + tmp).strip() if cur else tmp
                else:
                    flush()
                    cur = tmp
            continue

        if not cur:
            cur = st
            continue

        if len(cur) + 1 + len(st) <= SOFT_MAX:
            cur = f"{cur} {st}"
        else:
            if len(cur) < MIN_CHARS:
                cur = f"{cur} {st}"
                if len(cur) > SOFT_MAX:
                    flush()
                    cur = st
            else:
                flush()
                cur = st

    flush()

    # 마지막 안전망: 여전히 너무 짧은 꼬리면 앞 청크와 병합
    if len(chunks) >= 2 and len(chunks[-1]) < 200:
        chunks[-2] = f"{chunks[-2]} {chunks[-1]}".strip()
        chunks.pop()

    results = []
    for local_idx, txt in enumerate(chunks):
        item = dict(base_meta)
        item.update({
            "section": section,
            "section_index": section_idx,
            "chunk_index": local_idx,
            "text": txt,
            "chars": len(txt),
        })
        results.append(item)
    return results

def main():
    IN_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    data = json.loads(IN_PATH.read_text(encoding="utf-8"))

    seen_hash = set()
    total_chunks = 0
    total_cases = 0
    by_section = {k: 0 for k in SECTION_INDEX.keys()}
    dropped_empty = 0
    deduped = 0

    with OUT_PATH.open("w", encoding="utf-8") as fout:
        for row in data:
            total_cases += 1
            d = row.get("data", {}) or {}
            uid = str(row.get("id") or row.get("uid") or "")
            case_no = d.get("사건번호") or d.get("case_no") or ""
            title = d.get("사건명") or row.get("case_name") or ""
            court = d.get("법원명") or row.get("court") or ""
            jdate = d.get("선고일자") or row.get("judgment_date") or ""

            base_meta = {
                "uid": uid,
                "case_no": case_no,
                "title": title,
                "court": court,
                "judgment_date": jdate,
            }

            # 지정한 ORDER 순서대로 섹션을 훑되, 실제 텍스트가 있는 섹션만 처리
            for sec in SECTION_ORDER:
                raw = d.get(sec)
                if not raw:
                    continue
                text = norm_text(str(raw))
                if not text:
                    dropped_empty += 1
                    continue

                # 섹션별 chunk_index는 이 함수 내부에서 0부터
                made = make_chunks_from_text(base_meta, sec, text)

                # 중복 제거 후 기록
                for item in made:
                    h = sha1(item["text"])
                    if h in seen_hash:
                        deduped += 1
                        continue
                    seen_hash.add(h)
                    item["sha1"] = h
                    # 관측/디버깅 편의를 위한 전역 chunk_id
                    item["chunk_id"] = f"{item['uid']}:{SECTION_INDEX.get(sec,99)}:{item['chunk_index']}"
                    fout.write(json.dumps(item, ensure_ascii=False) + "\n")
                    total_chunks += 1
                    by_section[sec] = by_section.get(sec, 0) + 1

    stats = {
        "cases": total_cases,
        "chunks": total_chunks,
        "by_section": by_section,
        "dropped_empty_sections": dropped_empty,
        "deduped": deduped,
        "policy": {
            "min_chars": MIN_CHARS,
            "soft_max": SOFT_MAX,
            "hard_max": HARD_MAX,
            "section_index_fixed_map": SECTION_INDEX,
            "section_order": SECTION_ORDER,
        },
        "inputs": str(IN_PATH),
        "outputs": str(OUT_PATH),
    }
    STAT_PATH.write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ 청크 완료: {total_chunks} chunks / {total_cases} cases")
    print(f"📊 통계 저장: {STAT_PATH}")

if __name__ == "__main__":
    main()
