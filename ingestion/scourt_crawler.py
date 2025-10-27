# -*- coding: utf-8 -*-
"""
[역할]
- 대법원 포털(https://portal.scourt.go.kr)에서 판결문 목록/상세를 가져와 raw JSONL로 저장한다.
- RAG 원천 데이터 수집 단계.

[입출력]
- 출력: data/raw/{keyword}_cases.jsonl
  (각 줄: {"case_uid":..., "case_no":..., "summary":..., "reason":..., "xml":..., "_detail":"hit|miss", ...})

[핵심 흐름]
1) 검색 목록 API 호출(키워드, 페이지네이션)
2) 각 항목의 상세 API/페이지를 다양한 방식으로 시도(Ctxt/Dtl/Ctt, POST/GET/페이지스크랩)
3) 요약/이유/전문(HTML)을 텍스트화하여 저장

[언제 다시 쓰나?]
- 새로운 키워드나 기간으로 '추가 수집'이 필요할 때.
- 기존 데이터가 너무 오래되어 업데이트가 필요할 때.

[의존/주의]
- requests + BeautifulSoup 필요.
- 포털의 응답 포맷/보안정책이 바뀌면 엔드포인트/헤더/페이로드를 조정해야 함.
- 과도한 요청 방지를 위해 delay_ms 튜닝 가능.
"""



# ingestion/scourt_crawler.py
import time, random, json, requests, datetime, re
from pathlib import Path
from html import unescape
from bs4 import BeautifulSoup

BASE = "https://portal.scourt.go.kr"

# 🔎 목록 API
SEARCH_URL = f"{BASE}/pgp/pgp1011/selectJdcpctSrchRsltLst.on"

# 📄 상세 API (우선순위: Ctxt → Dtl → (구형)Ctt)
DETAIL_URLS = [
    f"{BASE}/pgp/pgp1011/selectJdcpctCtxt.on",               # ✅ 전문 HTML(orgdocXmlCtt)
    f"{BASE}/pgp/pgp1011/selectJdcpctDtl.on",                # 메타(사건번호/선고일 등)
    f"{BASE}/pgp/pgp1011/PGP1011M04/selectJdcpctCtt.on",     # 일부 환경에서 쓰는 구형/내부 엔드포인트
]

# 🔁 상세 페이지(직접 HTML 스크랩) 폴백
DETAIL_PAGES = [
    f"{BASE}/pgp/pgp1011/selectJdcpctCttPage.on",
    f"{BASE}/pgp/pgp1011/PGP1011M04/selectJdcpctCttPage.on",
]
DETAIL_REFERER_TPL = f"{BASE}/pgp/pgp1011/selectJdcpctCttPage.on?jisCntntsSrno={{uid}}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json; charset=UTF-8",
    "Origin": BASE,
    "Referer": f"{BASE}/pgp/pgp1011/selectJdcpctSrchRqstPage.on",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    # 실무에서 종종 요구됨 (세션 컨텍스트)
    "SC-Pgmid": "PGP1011M04",
}

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

def build_body(keyword, page_no=1, page_size=20):
    return {
        "dma_searchParam": {
            "srchwd": keyword,
            "sort": "jis_jdcpc_instn_dvs_cd_s asc, $relevance desc, prnjdg_ymd_o desc, jdcpct_gr_cd_s asc",
            "sortType": "정확도",
            "pageNo": str(page_no),
            "pageSize": str(page_size),
            # 검색 그룹코드(대법원 간행/전원합의체 등) – 필요시 조정
            "jdcpctGrCd": "111|112|130|141|180|182|232|235|201",
            "category": "jdcpct",
            "isKwdSearch": "N",
        }
    }

def _dump_debug(content, suffix):
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    p = RAW_DIR / f"_resp_{ts}.{suffix}"
    if isinstance(content, (dict, list)):
        p.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
    else:
        p.write_text(str(content), encoding="utf-8")
    return str(p)

def strip_html(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    txt = BeautifulSoup(s, "html.parser").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", txt).strip()

def _detail_payload_variants(uid: str):
    # 실제로는 Ctxt/Dtl 모두 {"dma_searchParam":{"jisCntntsSrno": uid}} 한 방이면 충분
    return [
        {"dma_searchParam": {"jisCntntsSrno": uid}},
        {"dma_searchParam": {"jisCntntsSrno": uid, "jdcpctUid": uid}},
        {"dma_detailParam": {"jisCntntsSrno": uid}},
        {"jisCntntsSrno": uid},
    ]

def _pick_first(d: dict, keys: list[str]) -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""

def _parse_detail_json(payload: dict) -> dict:
    """
    JSON 응답에서 요약/이유/전문 텍스트를 최대한 뽑아냄.
    *전문 HTML은 Ctxt 응답의 orgdocXmlCtt에 있음*
    """
    node = payload.get("result") or payload.get("data") or payload
    candidates = [node]
    if isinstance(node, dict):
        candidates += [v for v in node.values() if isinstance(v, dict)]

    summary = reason = xml = ""
    for n in candidates:
        if not isinstance(n, dict):
            continue
        # 요약
        summary = summary or _pick_first(
            n, ["jdcpctSumrCtt", "sumry", "summary", "intdCtt", "sumCtt", "smrCtt"]
        )
        # 이유/결론
        reason = reason or _pick_first(
            n, ["dcdcsCtt", "reason", "rcnCtt", "rsnCtt", "mainCtt", "jdgCtt"]
        )
        # 본문: Ctxt 응답은 orgdocXmlCtt, 과거엔 jdcpctXmlCtt 등도 존재
        xml = xml or _pick_first(
            n, ["orgdocXmlCtt", "jdcpctXmlCtt", "xml", "cnCtt", "cntntsCtt"]
        )

    return {
        "full_summary": strip_html(summary),
        "full_reason": strip_html(reason),
        "full_xml": strip_html(xml),  # 원문 HTML을 텍스트화(필요시 HTML 그대로 저장하도록 바꿔도 됨)
    }

def _parse_detail_html(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    def pick_text(*selectors):
        for sel in selectors:
            el = soup.select_one(sel)
            if el:
                return strip_html(el.get_text(" ", strip=True))
        return ""

    return {
        "full_summary": pick_text("#jdcpctSumrCtt", ".sumr", "#summary", ".summary", "[data-field='summary']"),
        "full_reason": pick_text("#dcdcsCtt", ".reason", "#decision", ".dcdcs", "[data-field='reason']"),
        "full_xml": pick_text("#jdcpctXmlCtt", ".xml", "#content", ".content", ".viewArea", "[data-field='xml']"),
    }

def _csrf_header(sess: requests.Session) -> dict:
    token = (
        sess.cookies.get("XSRF-TOKEN")
        or sess.cookies.get("CSRF-TOKEN")
        or sess.cookies.get("csrfToken")
    )
    return {"X-CSRF-TOKEN": token} if token else {}

def _submission_for(url: str) -> str:
    """
    페이지 내부에서 쓰는 submissionid 헤더. 없어도 동작하나, 성공률/안정성↑
    """
    if url.endswith("selectJdcpctCtxt.on"):
        return "mf_wfm_pgpDtlMain_sbm_selectJdcpctCtxt"
    if url.endswith("selectJdcpctDtl.on"):
        return "mf_wfm_pgpDtlMain_sbm_selectJdcpctDtl"
    if url.endswith("selectJdcpctCtt.on"):
        return "mf_wfm_pgpDtlMain_sbm_selectJdcpctCtt"
    return ""

def _try_detail_post_json(sess: requests.Session, url: str, uid: str):
    headers = {
        **HEADERS,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": DETAIL_REFERER_TPL.format(uid=uid),
        "Accept": "application/json, text/html, */*",
        "Content-Type": "application/json; charset=UTF-8",
        **_csrf_header(sess),
    }
    sub = _submission_for(url)
    if sub:
        headers["submissionid"] = sub

    for payload in _detail_payload_variants(uid):
        try:
            r = sess.post(url, headers=headers, json=payload, timeout=20, allow_redirects=True)
            if r.status_code >= 400:
                continue
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "json" in ctype or r.text.strip().startswith("{"):
                try:
                    return _parse_detail_json(r.json())
                except Exception:
                    pass
            return _parse_detail_html(r.text)
        except Exception:
            continue
    return None

def _try_detail_post_form(sess: requests.Session, url: str, uid: str):
    headers = {
        **HEADERS,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": DETAIL_REFERER_TPL.format(uid=uid),
        "Accept": "text/html,application/json,*/*",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        **_csrf_header(sess),
    }
    sub = _submission_for(url)
    if sub:
        headers["submissionid"] = sub

    for payload in _detail_payload_variants(uid):
        flat = {}
        for k, v in payload.items():
            if isinstance(v, dict):
                flat.update(v)
            else:
                flat[k] = v
        try:
            r = sess.post(url, headers=headers, data=flat, timeout=20, allow_redirects=True)
            if r.status_code >= 400:
                continue
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "json" in ctype:
                try:
                    return _parse_detail_json(r.json())
                except Exception:
                    pass
            return _parse_detail_html(r.text)
        except Exception:
            continue
    return None

def _try_detail_get(sess: requests.Session, url: str, uid: str):
    headers = {
        **HEADERS,
        "X-Requested-With": "XMLHttpRequest",
        "Referer": DETAIL_REFERER_TPL.format(uid=uid),
        "Accept": "text/html,application/json,*/*",
        **_csrf_header(sess),
    }
    sub = _submission_for(url)
    if sub:
        headers["submissionid"] = sub

    params_list = [
        {"jisCntntsSrno": uid},
        {"jisCntntsSrno": uid, "jdcpctUid": uid},
    ]
    for params in params_list:
        try:
            r = sess.get(url, headers=headers, params=params, timeout=20, allow_redirects=True)
            if r.status_code >= 400:
                continue
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "json" in ctype:
                try:
                    return _parse_detail_json(r.json())
                except Exception:
                    pass
            return _parse_detail_html(r.text)
        except Exception:
            continue
    return None

def _try_detail_page(sess: requests.Session, uid: str, debug=False):
    """
    AJAX가 막히면 상세 페이지(HTML)를 직접 열어 스크랩.
    """
    headers = {
        **HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": f"{BASE}/pgp/pgp1011/selectJdcpctSrchRqstPage.on",
    }
    params_list = [
        {"jisCntntsSrno": uid},
        {"jisCntntsSrno": uid, "jdcpctUid": uid},
    ]
    for page_url in DETAIL_PAGES:
        for params in params_list:
            try:
                r = sess.get(page_url, headers=headers, params=params, timeout=20, allow_redirects=True)
                if r.status_code >= 400:
                    continue
                parsed = _parse_detail_html(r.text)
                if any(parsed.values()):
                    return parsed
            except Exception:
                continue
    if debug:
        _dump_debug({"detail_page_failed_for": uid}, "json")
    return None

def fetch_detail(sess: requests.Session, uid: str, debug=False) -> dict:
    """
    uid(=jisCntntsSrno)로 상세 전문을 가져온다.
    1) JSON POST → 2) FORM POST → 3) GET → 4) 페이지 스크랩
    """
    for url in DETAIL_URLS:
        parsed = _try_detail_post_json(sess, url, uid)
        if parsed and any(parsed.values()):
            return parsed
        parsed = _try_detail_post_form(sess, url, uid)
        if parsed and any(parsed.values()):
            return parsed
        parsed = _try_detail_get(sess, url, uid)
        if parsed and any(parsed.values()):
            return parsed

    parsed = _try_detail_page(sess, uid, debug=debug)
    if parsed and any(parsed.values()):
        return parsed

    if debug:
        _dump_debug({"detail_failed_for": uid, "err": "all detail urls/payloads failed"}, "json")
    return {"full_summary": "", "full_reason": "", "full_xml": ""}

def crawl(keyword="이혼", max_pages=1, delay_ms=(800, 1600), debug=False, fetch_detail_flag=True):
    results = []
    total_count = 0

    with requests.Session() as sess:
        # 🔸 워밍업(쿠키/세션 생성)
        sess.get(BASE, headers=HEADERS, timeout=10)
        sess.get(f"{BASE}/pgp/pgp1011/selectJdcpctSrchRqstPage.on", headers=HEADERS, timeout=10)

        for page in range(1, max_pages + 1):
            r = sess.post(
                SEARCH_URL,
                headers=HEADERS,
                json=build_body(keyword, page, 20),
                timeout=20,
                allow_redirects=True,
            )
            if r.status_code >= 400:
                if debug: _dump_debug(r.text[:800], "txt")
                break

            ctype = (r.headers.get("Content-Type") or "").lower()
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

            items = []
            if isinstance(node, dict):
                for key in ("dlt_jdcpctRslt","list","items","rows","resultList","dataList"):
                    v = node.get(key)
                    if isinstance(v, list) and v:
                        items = v
                        break
                total_count = node.get("totalCount") or node.get("total") or total_count

            if not items:
                break

            for it in items:
                uid = it.get("jisCntntsSrno")
                row = {
                    "case_uid": uid,
                    "case_no": it.get("csNoLstCtt"),
                    "title": strip_html(it.get("csNmLstCtt")),
                    "court": it.get("cortNm"),
                    "judgment_date": it.get("prnjdgYmd"),
                    "group": it.get("grpJdcpctGrNm"),
                    "type": it.get("adjdTypNm"),
                    "publish": it.get("jdcpctPublcCtt"),
                    # 목록에서 오는 건 대부분 요약/요지(줄임표 가능)
                    "summary": strip_html(it.get("jdcpctSumrCtt") or ""),
                    "reason":  strip_html(it.get("dcdcsCtt") or ""),
                    "xml":     strip_html(it.get("jdcpctXmlCtt") or ""),  # 전문은 아래에서 교체
                    "keyword": keyword,
                    "_detail": "skip",
                }

                if fetch_detail_flag and uid:
                    detail = fetch_detail(sess, uid, debug=debug)
                    hit = False
                    if detail.get("full_summary"):
                        row["summary"] = detail["full_summary"]; hit = True
                    if detail.get("full_reason"):
                        row["reason"]  = detail["full_reason"];  hit = True
                    if detail.get("full_xml"):
                        row["xml"]     = detail["full_xml"];     hit = True
                    row["_detail"] = "hit" if hit else "miss"

                results.append(row)

            time.sleep(random.uniform(delay_ms[0] / 1000, delay_ms[1] / 1000))

    out_path = RAW_DIR / f"{keyword}_cases.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for row in results:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    return str(out_path), len(results), int(total_count or 0)
