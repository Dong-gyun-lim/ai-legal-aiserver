# -*- coding: utf-8 -*-
"""
[역할] 
- '전문'으로만 되어 있는 판결문을 섹션(판시사항/판결요지/주문/이유/전문/참조조문/참조판례) 단위로 분해한다.
- RAG 전처리의 1단계: '전문 → 여러 섹션'으로 쪼개어 이후 청크화 품질을 높임.

[입력]
- data/processed/clean_sections.jsonl 
  (각 줄: {"case_uid":..., "section_name":"전문", "text":"..."} 형태)

[출력]
- data/processed/clean_sections_v2.jsonl
  (각 줄: {"case_uid":..., "section_name":"판시사항|주문|이유|...","text":"..."} 형태)

[언제 다시 쓰나?]
- 새로 크롤링했거나, 기존 본문 정제 규칙을 바꿨을 때.
- 섹션 패턴(정규식)을 늘리거나 수정할 때 재실행.

[의존/주의]
- 한국어 섹션 제목 패턴에 의존. 드물게 인식 못 하는 문서가 있을 수 있음(SECTION_PATTERNS 튜닝).
"""



# -*- coding: utf-8 -*-
"""
전문(全文) 안에 들어있는 소제목을 인식해 판시사항/주문/이유 등 섹션으로 분해.
입력 : data/processed/clean_sections.jsonl  (현재는 section_name=전문 하나짜리)
출력 : data/processed/clean_sections_v2.jsonl (여러 섹션으로 분해)
"""

import json, re
from pathlib import Path

IN = Path("data/processed/clean_sections.jsonl")
OUT = Path("data/processed/clean_sections_v2.jsonl")
OUT.parent.mkdir(parents=True, exist_ok=True)

# 자주 쓰이는 제목 패턴들(전/구 표기, 한/영 혼용을 넉넉히)
# 순서가 중요: 위에서부터 매칭
SECTION_PATTERNS = [
    r"【\s*판시사항\s*】",
    r"【\s*판결요지\s*】",
    r"【\s*주\s*문\s*】|주\s*문",           # '【주 문】' 혹은 '주문'
    r"【\s*이\s*유\s*】|이\s*유",           # '【이 유】' 혹은 '이유'
    r"【\s*전\s*문\s*】|전\s*문",           # '【전 문】' 혹은 '전문' (문서 중간에 또 나오는 경우)
    r"【\s*참조조문\s*】",
    r"【\s*참조판례\s*】",
    r"【\s*전\s*문\s*】"                    # 백업
]

# 제목 라벨 정규화 매핑
LABEL_MAP = {
    "판시사항": "판시사항",
    "판결요지": "판결요지",
    "주문": "주문",
    "이유": "이유",
    "전문": "전문",
    "참조조문": "참조조문",
    "참조판례": "참조판례",
}

# 패턴을 하나의 큰 정규식으로
SECTION_REGEX = re.compile(
    "(" + "|".join(SECTION_PATTERNS) + ")"
)

def normalize_title(title: str) -> str:
    t = re.sub(r"[【】\s]", "", title)
    # '주문','이유','전문' 등으로 정규화
    for key in LABEL_MAP:
        if key in t:
            return LABEL_MAP[key]
    return "기타"

def split_into_sections(text: str):
    """
    전문 텍스트에서 [제목, 본문] 블록들을 추출.
    제목이 하나도 없으면 전문 그대로 반환.
    """
    if not text or not isinstance(text, str):
        return [("전문", "")]

    # 소제목 기준으로 split, 제목 토큰도 함께 캡처됨
    parts = SECTION_REGEX.split(text)
    # parts 예: ["서론...", "【판시사항】", "판시사항 내용...", "【이 유】", "이유 내용...", ...]
    if len(parts) <= 1:
        # 소제목이 전혀 없으면 통째로 전문
        return [("전문", text.strip())]

    sections = []
    # 앞부분(제목 없이 시작한 내용)이 있으면 '전문'으로 넣어줌
    lead = parts[0].strip()
    if lead:
        sections.append(("전문", lead))

    # 이후는 [제목, 내용, 제목, 내용, ...] 구조
    cur_title = None
    cur_body = []
    for token in parts[1:]:
        token = token.strip()
        if not token:
            continue
        if SECTION_REGEX.match(token):
            # 이전 섹션 flush
            if cur_title is not None and cur_body:
                sections.append((normalize_title(cur_title), "\n".join(cur_body).strip()))
                cur_body = []
            cur_title = token
        else:
            cur_body.append(token)

    # 마지막 섹션 flush
    if cur_title is not None:
        sections.append((normalize_title(cur_title), "\n".join(cur_body).strip()))

    # 비어있는 섹션 제거
    sections = [(t, b) if b else (t, "") for t, b in sections if b.strip()]
    return sections or [("전문", text.strip())]


def process():
    count_in = count_out = 0
    with IN.open("r", encoding="utf-8") as fin, OUT.open("w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            row = json.loads(line)
            case_uid = row.get("case_uid", "")
            section_name = row.get("section_name", "전문")
            text = row.get("text", "")

            # 이미 '전문'으로만 온 걸 섹션 쪼개기
            if section_name == "전문":
                blocks = split_into_sections(text)
            else:
                # 혹시 다른 섹션으로 온 것도 그대로 통과
                blocks = [(section_name, text)]

            for idx, (title, body) in enumerate(blocks):
                out = {
                    "case_uid": case_uid,
                    "section_name": title,
                    "text": body
                }
                fout.write(json.dumps(out, ensure_ascii=False) + "\n")
                count_out += 1
            count_in += 1

    print(f"[DONE] sectionize v2 → in={count_in}, out={count_out}, path={OUT}")

if __name__ == "__main__":
    process()
