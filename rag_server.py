# rag_server.py
# Flask RAG 서버: E5 임베딩 + FAISS 검색 + (옵션) 리랭커 + Qwen(Ollama)
# 요구: pip install flask sentence-transformers faiss-cpu requests ujson python-dotenv
# (리랭커 사용 시) pip install FlagEmbedding

from __future__ import annotations
import os, json, ujson, time, re
from typing import List, Dict, Any
import numpy as np
import faiss
import requests
from flask import Flask, request, Response
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

# ----------------------------
# 설정
# ----------------------------
load_dotenv()  # .env 로드

VEC_DIR    = os.getenv("VEC_DIR", "vectorstore")
INDEX_PATH = os.path.join(VEC_DIR, "faiss.index")
IDS_PATH   = os.path.join(VEC_DIR, "ids.npy")
META_PATH  = os.path.join(VEC_DIR, "meta.jsonl")

OLLAMA_URL   = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b-instruct")

TOP_N_CANDIDATES = int(os.getenv("TOP_N_CANDIDATES", "30"))  # 1차 후보 (FAISS)
TOP_K            = int(os.getenv("TOP_K", "5"))               # 프롬프트에 넣을 개수

USE_RERANKER = os.getenv("USE_RERANKER", "0") == "1"

# 섹션별 가중치: '이유', '판시사항' 우선, '주문'은 낮게 유지
PREFERRED_SECTIONS: Dict[str, float] = {
    "이유": 2.0,
    "판시사항": 1.5,
    "요지": 1.2,
    "주문": 0.8,
}

# ----------------------------
# 앱 부트
# ----------------------------
app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False  # 한글 이스케이프 금지

def respond(obj: dict, status: int = 200) -> Response:
    return Response(
        json.dumps(obj, ensure_ascii=False),
        status=status,
        mimetype="application/json; charset=utf-8",
    )

def load_meta(meta_path: str) -> List[Dict[str, Any]]:
    meta = []
    with open(meta_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                meta.append(json.loads(line))
    return meta

print("[BOOT] loading FAISS/index & meta…")
index: faiss.Index = faiss.read_index(INDEX_PATH)
ids: np.ndarray = np.load(IDS_PATH)
meta: List[Dict[str, Any]] = load_meta(META_PATH)
print(f"[BOOT] index={INDEX_PATH}, size={index.ntotal}, ids={len(ids)}, meta={len(meta)}")

print("[BOOT] loading E5 embedding model…")
embed = SentenceTransformer("intfloat/multilingual-e5-base")

if USE_RERANKER:
    from FlagEmbedding import FlagReranker
    print("[BOOT] loading BGE reranker…")
    reranker = FlagReranker("BAAI/bge-reranker-v2-m3", use_fp16=True)

# ----------------------------
# 유틸
# ----------------------------
def encode_query(text: str) -> np.ndarray:
    """E5는 쿼리에 'query: ' 프리픽스를 붙임."""
    v = embed.encode([f"query: {text}"], normalize_embeddings=True)
    return v.astype("float32")

def _pick_text_field(m: Dict[str, Any]) -> str:
    return (
        m.get("text")
        or m.get("chunk_text")
        or m.get("content")
        or m.get("body")
        or m.get("section_text")
        or ""
    )

def faiss_search(qvec: np.ndarray, top_n: int) -> List[Dict[str, Any]]:
    """FAISS 검색 + 섹션 가중치 적용 후 재정렬."""
    D, I = index.search(qvec, top_n)
    scores = D[0].tolist()
    idxs   = I[0].tolist()
    out = []
    for s, ix in zip(scores, idxs):
        if ix < 0:
            continue
        m = meta[int(ids[ix])]
        txt = _pick_text_field(m)
        sec = (m.get("section_name") or "").strip()
        weight = PREFERRED_SECTIONS.get(sec, 1.0)
        out.append({
            "score": float(s) * weight,
            **m,            # case_uid, case_no, section_name, chunk_index, ...
            "text": txt,    # 프롬프트/리랭커에서는 항상 'text' 사용
        })
    out.sort(key=lambda x: x["score"], reverse=True)
    return out

def rerank_if_enabled(query: str, candidates: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
    """리랭커 사용 시 쿼리-문서 점수로 재정렬."""
    if not USE_RERANKER:
        return candidates[:top_k]
    clean = [c for c in candidates if c.get("text")]
    if not clean:
        return candidates[:top_k]
    pairs = [(query, c["text"]) for c in clean]
    scores = reranker.compute_score(pairs)  # 리스트 반환
    ranked = sorted(
        [{"rerank_score": float(sc), **c} for sc, c in zip(scores, clean)],
        key=lambda x: x["rerank_score"],
        reverse=True
    )
    return ranked[:top_k]

def diversify_by_case(cands: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    """같은 사건번호(case_no) 중복을 제거해 다양성 확보."""
    seen = set()
    picked = []
    for c in cands:
        case_no = c.get("case_no")
        if not case_no or case_no in seen:
            continue
        seen.add(case_no)
        picked.append(c)
        if len(picked) >= limit:
            break
    return picked

def build_prompt(question: str, picked: List[Dict[str, Any]]) -> str:
    """
    평문 + 각주번호 포맷 강제.
    - 본문 내 메타(사건번호/섹션/청크 라벨) 금지
    - 문장 끝 각주 [n]만 허용
    - 마지막 줄: (참고: 사건번호, …)
    """
    context_lines = []
    ref_nums = []
    for i, r in enumerate(picked, 1):
        ref_nums.append(f"{i}:{r.get('case_no','?')}")
        context_lines.append(f"[{i}] { (r.get('text') or '').strip() }")
    context = "\n\n".join(context_lines)

    system_rules = (
        "너는 한국 판례를 바탕으로 이혼/양육권/위자료/재산분할을 설명하는 법률 보조 전문가다.\n"
        "출력 형식 규칙:\n"
        "- 마크다운 금지(###, ** 등). 평문 단락만 작성.\n"
        "- 사건번호, 섹션명 같은 메타는 본문에 쓰지 말라.\n"
        "- 인용은 문장 끝에 [1], [2] 등 각주 번호만 사용.\n"
        "- 마지막 줄에만 (참고: 사건번호1, 사건번호2) 형태로 사건번호를 정리.\n"
        "- 단정 금지. 정보가 부족하면 부족함을 명시.\n"
        "- 성중립 표현 사용. 특정 성별 편향 금지.\n"
        "- 양육권 판단은 자녀의 최선의 이익 기준이 최우선임을 명시하고,\n"
        "  주양육자·양육연속성, 자녀의 연령·의사, 부모의 양육능력·시간·환경, 형제자매 분리 최소화 등 고려요소를 설명.\n"
        "- 외도는 직접적 결정요소가 아니며, 양육적격성에 영향을 줄 때에만 간접 고려됨을 명시.\n"
    )

    prompt = (
        f"{system_rules}\n"
        f"[질문]\n{question}\n\n"
        f"[근거 텍스트]\n{context}\n\n"
        f"[작성 지시]\n"
        "- 1단락: 질문을 사실적으로 재진술.\n"
        "- 2단락: 양육권 판단의 일반 기준 요약.\n"
        "- 3단락: 제공 근거와의 연결 및 시사점(각주 번호 인용).\n"
        "- 4단락: 부족한 점/추가 필요 정보 제시.\n"
        "- 마지막 줄: (참고: 사건번호 나열)\n"
    )
    return prompt

def call_ollama(prompt: str) -> str:
    url = f"{OLLAMA_URL}/api/generate"
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.2,
            "top_p": 0.9
        }
    }
    r = requests.post(url, json=payload, timeout=120)
    r.raise_for_status()
    data = r.json()  # Ollama /api/generate => {"response": "..."}
    return (data.get("response") or "").strip()

def postprocess_answer(ans: str, picked: List[Dict[str, Any]]) -> str:
    """마크다운/메타 라벨 제거 + (참고: …) 꼬리표 강제."""
    # 1) 마크다운 제목 등 제거
    ans = re.sub(r'^\s*#{1,6}\s*', '', ans, flags=re.MULTILINE)
    # 2) "사건번호:..., 섹션:..., 청크:..." 같은 메타 라벨 제거
    ans = re.sub(r'사건번호\s*:\s*\S+(?:\s*·\s*섹션\s*:\s*\S+)?(?:\s*·\s*청크\s*:\s*\d+)?', '', ans)
    # 3) 연속 개행 정리
    ans = re.sub(r'\n{3,}', '\n\n', ans).strip()

    # 4) (참고: …) 꼬리표 안전 생성
    uniq_cases = []
    seen = set()
    for r in picked:
        c = r.get("case_no")
        if c and c not in seen:
            seen.add(c)
            uniq_cases.append(c)
    tail = f"(참고: {', '.join(uniq_cases)})" if uniq_cases else ""
    if tail and tail not in ans:
        ans = f"{ans}\n{tail}"
    return ans

# ----------------------------
# API
# ----------------------------
@app.route("/rag", methods=["POST"])
def rag():
    t0 = time.time()
    body = request.get_json(force=True, silent=True) or {}
    question = (body.get("question") or "").strip()
    top_k = int(body.get("top_k") or TOP_K)
    if not question:
        return respond({"ok": False, "error": "question is required"}, status=400)

    # 1) 쿼리 임베딩
    qvec = encode_query(question)

    # 2) FAISS 후보 검색
    cands = faiss_search(qvec, max(TOP_N_CANDIDATES, top_k))

    # 3) (옵션) 리랭킹 → 최종 후보 확장 후 다양화 적용
    ranked = rerank_if_enabled(question, cands, max(top_k * 2, 10))
    picked = diversify_by_case(ranked, top_k)

    # 4) 프롬프트 구성
    prompt = build_prompt(question, picked)

    # 5) LLM 호출 + 후처리
    try:
        raw = call_ollama(prompt)
        answer = postprocess_answer(raw, picked)
    except Exception as e:
        return respond({"ok": False, "error": f"Ollama call failed: {e}"}, status=500)

    # 6) 출처 구성
    sources = []
    for r in picked:
        sources.append({
            "case_uid": r.get("case_uid"),
            "case_no": r.get("case_no"),
            "section_name": r.get("section_name"),
            "chunk_index": r.get("chunk_index"),
            "score": float(r.get("score", 0.0)),
        })

    return respond({
        "ok": True,
        "answer": answer,
        "sources": sources,
        "latency_ms": int((time.time() - t0) * 1000)
    })

if __name__ == "__main__":
    # 개발용 CORS가 필요하면 아래 주석 해제
    # from flask_cors import CORS
    # CORS(app, resources={r"/*": {"origins": "*"}})
    app.run(host="0.0.0.0", port=5001, debug=True)
