# ingestion/scourt_crawler.py
import time, random, json, requests, datetime, re
from pathlib import Path
from html import unescape
from bs4 import BeautifulSoup

BASE = "https://portal.scourt.go.kr"
SEARCH_URL = f"{BASE}/pgp/pgp1011/selectJdcpctSrchRsltLst.on"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json; charset=UTF-8",
    "Origin": BASE,
    "Referer": f"{BASE}/pgp/pgp1011/selectJdcpctSrchRqstPage.on",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
}

RAW_DIR = Path("data/raw"); RAW_DIR.mkdir(parents=True, exist_ok=True)

def build_body(keyword, page_no=1, page_size=20):
    return {"dma_searchParam":{
        "srchwd": keyword,
        "sort":"jis_jdcpc_instn_dvs_cd_s asc, $relevance desc, prnjdg_ymd_o desc, jdcpct_gr_cd_s asc",
        "sortType":"정확도",
        "pageNo":str(page_no),
        "pageSize":str(page_size),
        "jdcpctGrCd":"111|112|130|141|180|182|232|235|201",
        "category":"jdcpct",
        "isKwdSearch":"N"
    }}

def _dump_debug(content, suffix):
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    p = RAW_DIR / f"_resp_{ts}.{suffix}"
    if isinstance(content, (dict, list)):
        p.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        p.write_text(str(content), encoding="utf-8")
    return str(p)

# --- HTML -> 텍스트
def strip_html(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    txt = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", txt).strip()

# --- 항목 정리
def clean_item_fields(it: dict) -> dict:
    out = {}

    # (1) 기본 키 매핑: 실제 응답 키들을 폭넓게 커버
    #   - id
    out["case_uid"] = (
        it.get("jdcpctUid") or it.get("jdcpctSn") or it.get("docId") or
        it.get("uid") or it.get("docno")
    )
    #   - 사건번호
    out["case_no"] = (
        it.get("jdcpctCaseNo") or it.get("caseNo") or it.get("cnm") or
        it.get("case_no")
    )
    #   - 제목
    out["title"] = (
        it.get("jdcpctSjNm") or it.get("title") or it.get("sj") or it.get("ttl")
    )
    #   - 법원
    out["court"] = (
        it.get("jdcpctInstnNm") or it.get("courtName") or it.get("court")
    )
    #   - 선고일
    out["judgment_date"] = (
        it.get("prnjdgYmd") or it.get("judgmentDate") or it.get("jdgDt")
    )
    #   - 그룹/유형/공보
    out["group"]   = it.get("jdcpctGrNm")  or it.get("group")
    out["type"]    = it.get("jdcpctTypNm") or it.get("type")
    out["publish"] = it.get("prnjdgNo")    or it.get("publish")

    # (2) 요약/이유/원문 비슷한 "본문성" 필드 자동 탐색
    # 응답에서 본문 텍스트는 보통 ...Ctt / ...CntntsCtt 같은 접미사로 들어오고 HTML 포함.
    text_like_fields = []
    for k, v in it.items():
        if not isinstance(v, str):
            continue
        if any(s in k.lower() for s in ["ctt", "cntnts", "sumry", "summary", "reason", "xml"]):
            # 태그가 있거나, 꽤 긴 텍스트면 본문처럼 간주
            if "<" in v or len(v) > 80:
                text_like_fields.append((k, v))

    # 우선순위: introduction/int -> summary, reason/rcn -> reason, xml -> xml
    summary_src = next((v for k, v in text_like_fields if "int" in k.lower() or "sum" in k.lower()), None)
    reason_src  = next((v for k, v in text_like_fields if "rcn" in k.lower() or "reason" in k.lower()), None)
    xml_src     = next((v for k, v in text_like_fields if "xml" in k.lower()), None)

    out["summary"] = strip_html(summary_src or "")
    out["reason"]  = strip_html(reason_src  or "")
    out["xml"]     = strip_html(xml_src     or "")

    # (3) 전반적인 문자열 필드 클린업(제목 포함)
    for key in ("title", "court", "group", "type", "publish"):
        if isinstance(out.get(key), str):
            out[key] = strip_html(out[key])

    return out

def crawl(keyword="이혼", max_pages=1, delay_ms=(600,1200), debug=False,
          clean_text=True):
    results = []
    total_count = 0

    with requests.Session() as sess:
        # warmup
        sess.get(BASE, headers=HEADERS, timeout=10)
        sess.get(f"{BASE}/pgp/pgp1011/selectJdcpctSrchRqstPage.on", headers=HEADERS, timeout=10)

        for page in range(1, max_pages+1):
            r = sess.post(
                SEARCH_URL, headers=HEADERS,
                json=build_body(keyword, page, 20),
                timeout=20, allow_redirects=True
            )
            ctype = (r.headers.get("Content-Type") or "").lower()
            if r.status_code >= 400:
                if debug: _dump_debug(r.text[:800], "txt")
                break

            if "application/json" not in ctype:
                if debug: _dump_debug(r.text, "html")
                break

            try:
                data = r.json()
                if debug: _dump_debug(data, "json")
            except Exception:
                if debug: _dump_debug(r.text[:800], "txt")
                break

            node = data.get("result") or data.get("data") or data

            # ✅ 실제 목록 키 반영: dlt_jdcpctRslt 우선
            items = []
            if isinstance(node, dict):
                for key in ("dlt_jdcpctRslt", "list", "items", "rows", "resultList", "dataList"):
                    v = node.get(key)
                    if isinstance(v, list) and v:
                        items = v; break
                total_count = node.get("totalCount") or node.get("total") or total_count

            if not items:
                # 더 이상 없음
                break

            for it in items:
                row = clean_item_fields(it) if clean_text else dict(it)
                row["keyword"] = keyword
                results.append(row)

            time.sleep(random.uniform(delay_ms[0]/1000, delay_ms[1]/1000))

    out_path = RAW_DIR / f"{keyword}_cases.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for row in results:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return str(out_path), len(results), int(total_count or 0)
