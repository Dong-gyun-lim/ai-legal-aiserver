from flask import Flask, request, jsonify
from flask_cors import CORS
import numpy as np
from data_loader import load_precedents
from ollama_client import embed as ollama_embed, generate as ollama_gen
from ingestion.scourt_crawler import crawl  # 크롤러

app = Flask(__name__)
CORS(app)

# 한글 JSON 깨짐 방지
app.config['JSON_AS_ASCII'] = False
try:
    app.json.ensure_ascii = False  # Flask 3.x 호환용
except Exception:
    pass

# 1) 데이터 로드
df = load_precedents()

# 2) 문서 임베딩 (서버 시작 시 1회)
DOC_EMB = []
if len(df):
    DOC_EMB = ollama_embed(df["text"].fillna("").tolist())

# --- 코사인 유사도 계산 ---
def cosine(q, M):
    """
    q: (d,) 질의 벡터
    M: (n, d) 문서 임베딩 행렬
    return: (n,) 각 문서와의 코사인 유사도
    """
    q = np.array(q, dtype="float32")
    M = np.array(M, dtype="float32")
    q = q / (np.linalg.norm(q) + 1e-9)
    M = M / (np.linalg.norm(M, axis=1, keepdims=True) + 1e-9)
    return M @ q

# --------- 크롤링 트리거 (관리용) ----------
@app.route("/crawl", methods=["POST"])
def trigger_crawl():
    """
    요청 JSON 예:
    {
      "keyword": "이혼",
      "pages": 1,
      "debug": true,
      "delay_ms": [600, 1200]
    }
    """
    data = request.get_json(force=True) or {}
    keyword = data.get("keyword", "이혼")
    pages = int(data.get("pages", 1))
    debug = bool(data.get("debug", True))
    delay_ms = data.get("delay_ms", [600, 1200])

    if not isinstance(delay_ms, (list, tuple)) or len(delay_ms) != 2:
        delay_ms = (600, 1200)
    else:
        delay_ms = (int(delay_ms[0]), int(delay_ms[1]))

    # [MOD] crawl()이 (out_path, scraped_count, total_available) 3가지를 반환하도록 맞춤
    out_path, scraped, total = crawl(
        keyword=keyword,
        max_pages=pages,
        delay_ms=delay_ms,
        debug=debug
    )
    # [MOD] total 필드 포함해 응답
    return jsonify({"ok": True, "path": str(out_path), "count": int(scraped), "total": int(total)})

@app.get("/health")
def health():
    return {"ok": True, "count": int(len(df))}

@app.post("/embed")
def embed_api():
    body = request.get_json(silent=True) or {}
    text = body.get("text", "")
    if not text:
        return {"ok": False, "error": "text required"}, 400
    vec = ollama_embed(text)[0]
    return {"ok": True, "embedding": vec}

@app.post("/similar")
def similar_api():
    if not len(df) or not len(DOC_EMB):
        return {"ok": True, "items": [], "total": int(len(df))}
    body = request.get_json(silent=True) or {}
    query = body.get("query", "").strip()
    top_k = int(body.get("top_k", 5))

    if not query:
        return {"ok": False, "error": "query required"}, 400
    if top_k <= 0:
        return {"ok": False, "error": "top_k must be > 0"}, 400

    qv = ollama_embed(query)[0]
    sims = cosine(qv, DOC_EMB)
    idx = np.argsort(-sims)[:top_k]

    items = []
    for i in idx:
        row = df.iloc[int(i)]
        items.append({
            "id": int(row.get("id", i)),
            "title": row.get("title", ""),
            "snippet": str(row.get("text", ""))[:180] + "...",
            "label": row.get("label", None),
            "score": float(sims[i]),
        })
    return {"ok": True, "items": items, "total": int(len(df))}

@app.post("/predict")
def predict_api():
    body = request.get_json(silent=True) or {}
    prompt = f"""너는 가정법 보조 변호사다.
다음 사건 정보를 간단히 한국어 한 줄로 요약해줘.
입력: {body}
한 줄 요약:"""
    summary = ollama_gen(prompt)
    return {"ok": True, "summary": summary}

# --- (RAG 유틸) 관련 판례 컨텍스트 만들기 ---
def make_rag_context(df_, indices, max_chars_per_doc=700):
    sources = []
    chunks = []
    for rank, i in enumerate(indices, start=1):
        row = df_.iloc[int(i)]
        title = str(row.get("title", f"문서 {i}"))
        text = str(row.get("text", ""))[:max_chars_per_doc]
        label = row.get("label", None)

        sources.append({
            "rank": rank,
            "id": int(row.get("id", i)),
            "title": title,
            "label": label,
        })
        chunks.append(f"[{rank}] 제목: {title}\n본문: {text}")

    context = "\n\n---\n\n".join(chunks)
    return context, sources

# --- (RAG API) 검색 + 생성 ---
@app.post("/rag")
def rag_api():
    if not len(df) or not len(DOC_EMB):
        return {"ok": False, "error": "no data"}, 400

    body = request.get_json(silent=True) or {}
    question = body.get("question", "").strip()
    top_k = int(body.get("top_k", 3))
    max_chars = int(body.get("max_chars_per_doc", 700))

    if not question:
        return {"ok": False, "error": "question required"}, 400
    if top_k <= 0:
        return {"ok": False, "error": "top_k must be > 0"}, 400

    qv = ollama_embed(question)[0]
    sims = cosine(qv, DOC_EMB)
    idx = np.argsort(-sims)[:top_k]
    context, sources = make_rag_context(df, idx, max_chars_per_doc=max_chars)

    prompt = f"""
너는 한국어 법률 보조 변호사다.
아래 '관련 판례 발췌'를 반드시 근거로 사용하여, 사용자의 질문에 간단하고 정확하게 답하라.
답변 끝에는 참고한 판례 번호를 대괄호로 표기하라. 예: [1][3]

[사용자 질문]
{question}

[관련 판례 발췌]
{context}

[지시사항]
- 제공된 판례의 내용 범위 안에서만 답하라. 근거가 부족하면 '판례 범위에서 확답이 어렵다'고 말하라.
- 핵심 위주로 3~5문장 이내로 요약하라.
- 일반인이 이해할 수 있게 쉽게 설명하라.

[최종 답변]
"""
    answer = ollama_gen(prompt)
    for r, i in zip(sources, idx):
        r["score"] = float(sims[i])

    return {"ok": True, "answer": answer, "sources": sources, "top_k": top_k}

if __name__ == "__main__":
    # 개발 편의를 위해 debug=True 유지
    app.run(host="0.0.0.0", port=5001, debug=True)
