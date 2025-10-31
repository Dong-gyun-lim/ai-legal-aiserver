# filename: law_api_fetch.py
import os
import re
import json
import html
import requests
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional

# =========================
# 🔧 환경설정
# =========================
load_dotenv()
LAW_OC = os.getenv("LAW_OC")
if not LAW_OC:
    raise RuntimeError("환경변수 LAW_OC 가 없습니다. .env에 LAW_OC=... 를 설정하세요.")

LIST_URL = "https://www.law.go.kr/DRF/lawSearch.do"
DETAIL_URL = "https://www.law.go.kr/DRF/lawService.do"
OUT_DIR = Path("out")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# 🔹 유틸 함수
# =========================
CANDIDATE_ID_KEYS = [
    "판례정보일련번호", "판례일련번호", "판례ID", "precSeq", "precId", "ID"
]

def clean_html(s: Any) -> Any:
    """HTML 태그 제거 및 개행 처리"""
    if not isinstance(s, str):
        return s
    s = html.unescape(s)
    s = s.replace("<br/>", "\n").replace("<br>", "\n")
    s = re.sub(r"<[^>]+>", "", s)
    return s.strip()

def extract_prec_id(item: Dict[str, Any]) -> Optional[str]:
    """판례정보일련번호 추출 (여러 후보 키 검사)"""
    for k in CANDIDATE_ID_KEYS:
        if k in item and item[k]:
            return str(item[k])
    for k, v in item.items():
        if isinstance(k, str) and "일련번호" in k and v:
            return str(v)
    return None

def extract_year(date_str: Optional[str]) -> Optional[int]:
    """'YYYYMMDD' 또는 'YYYY.MM.DD' 형식에서 연도 추출"""
    if not date_str:
        return None
    m = re.search(r"(19|20)\d{2}", str(date_str))
    return int(m.group()) if m else None

# dat_srn_nm='법원도서관' → 법원도서관이 정제·공개한 공식 판례 DB만 검색 (품질 안정)
# 미지정 시: 대법원/헌재 등 통합검색 (중복/비정형 데이터 포함 가능)
def fetch_case_list_page(keyword: str, page: int, display: int = 100,
                         dat_srn_nm: Optional[str] = "법원도서관",
                         sort: str = "ddes") -> Dict[str, Any]:
    """판례 목록 1페이지 조회"""
    params = {
        "OC": LAW_OC,
        "target": "prec",
        "type": "JSON",
        "search": "1",
        "query": keyword,
        "display": display,
        "page": page,
        "sort": sort,
    }
    if dat_srn_nm:
        params["datSrnNm"] = dat_srn_nm

    res = requests.get(LIST_URL, params=params, timeout=20)
    res.raise_for_status()
    return res.json()

def fetch_case_list_all(keyword: str, limit: int = 1000,
                        dat_srn_nm: Optional[str] = "법원도서관") -> List[Dict[str, Any]]:
    """페이지네이션으로 전체 판례 목록 조회"""
    collected: List[Dict[str, Any]] = []
    page = 1
    per_page = 100

    while len(collected) < limit:
        data = fetch_case_list_page(keyword, page=page, display=per_page, dat_srn_nm=dat_srn_nm)
        prec_block = data.get("PrecSearch", {})
        items = prec_block.get("prec")

        if not items:
            break
        if isinstance(items, dict):
            items = [items]

        collected.extend(items)

        total_cnt = prec_block.get("totalCnt")
        try:
            total_cnt = int(total_cnt) if total_cnt else None
        except Exception:
            total_cnt = None

        if len(items) < per_page or (total_cnt and len(collected) >= total_cnt):
            break
        page += 1

    if len(collected) > limit:
        collected = collected[:limit]
    return collected

def fetch_case_detail(prec_id: str) -> Optional[Dict[str, Any]]:
    """특정 판례 본문 조회"""
    params = {
        "OC": LAW_OC,
        "target": "prec",
        "type": "JSON",
        "ID": prec_id,
    }
    res = requests.get(DETAIL_URL, params=params, timeout=20)
    res.raise_for_status()
    data = res.json()
    return data.get("PrecService")

def save_json(path: Path, obj: Any):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

# =========================
# 🚀 메인 실행
# =========================
def main():
    keyword = "이혼"
    max_results = 500
    dat_srn_nm = "법원도서관"
    min_year = 2000  # ✅ 2000년 이후 판례만 수집

    print(f"📡 '{keyword}' 판례 {max_results}건까지 수집 시작 (소스={dat_srn_nm or '전체'})")
    items = fetch_case_list_all(keyword, limit=max_results * 2, dat_srn_nm=dat_srn_nm)  # 여유 확보
    print(f"✅ 원본 목록 수집 완료: {len(items)}건")

    # 🧹 2000년 이후 판례만 필터링
    filtered = []
    for it in items:
        year = extract_year(it.get("선고일자"))
        if year and year >= min_year:
            filtered.append(it)
    print(f"📅 {min_year}년 이후 판례만 남김: {len(filtered)}건")

    # 최대 500건까지만 사용
    items = filtered[:max_results]

    # 1️⃣ 목록 요약 저장
    list_summary = []
    for it in items:
        pid = extract_prec_id(it)
        list_summary.append({
            "사건명": it.get("사건명"),
            "판례정보일련번호": pid,
            "사건번호": it.get("사건번호"),
            "선고일자": it.get("선고일자"),
            "법원명": it.get("법원명"),
        })
    save_json(OUT_DIR / f"list_{keyword}.json", list_summary)
    print(f"💾 목록 저장 완료: out/list_{keyword}.json")

    # 2️⃣ 본문 수집 (원본 / 정리본)
    raw_all = []
    cleaned_all = []
    missing = []

    for i, it in enumerate(items, 1):
        pid = extract_prec_id(it)
        if not pid:
            continue
        detail = fetch_case_detail(pid)
        if not detail:
            missing.append({"id": pid, "case_name": it.get("사건명")})
            continue

        raw_all.append({"id": pid, "case_name": detail.get("사건명"), "data": detail})
        cleaned = {k: clean_html(v) for k, v in detail.items()}
        cleaned_all.append({"id": pid, "case_name": detail.get("사건명"), "data": cleaned})
        print(f"  [{i}/{len(items)}] {pid} {detail.get('사건명')} 수집 완료")

    save_json(OUT_DIR / f"precedents_raw_{keyword}.json", raw_all)
    save_json(OUT_DIR / f"precedents_cleaned_{keyword}.json", cleaned_all)
    if missing:
        save_json(OUT_DIR / f"missing_{keyword}.json", missing)

    print(f"📘 본문 원본 저장 완료: out/precedents_raw_{keyword}.json")
    print(f"📗 본문 정리본 저장 완료: out/precedents_cleaned_{keyword}.json")
    print(f"📂 출력 폴더: {OUT_DIR.resolve()}")

if __name__ == "__main__":
    main()
