from flask import Flask, request, jsonify
from flask_cors import CORS
import numpy as np
import json
import pandas as pd
from ollama_client import embed as ollama_embed, generate as ollama_gen
from ingestion.scourt_crawler import crawl  # 크롤러

app = Flask(__name__)
CORS(app)

# --- 한글 JSON 깨짐 방지 ---
app.config['JSON_AS_ASCII'] = False
try:
    app.json.ensure_ascii = False  # Flask 3.x 호환용
except Exception:
    pass

# --------------------------------------------------------
# 1️⃣ 청크 데이터 로드
# --------------------------------------------------------
chunks_path = "data/processed/chunks.jsonl"

chunks = []
try:
    with open(chunks_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            chunks.append(json.loads(line))
    df = pd.DataFrame(chunks)
    print(f"[INFO] 청크 데이터 로드 완료 ({len(df)}건) from {chunks_path}")
except FileNotFoundError:
    print(f"[WARN] 청크 파일이 없습니다: {chunks_path}")
    df = pd.DataFrame(columns=["case_uid", "section_name", "chunk_index", "text"])

# --------------------------------------------------------
# 2️⃣ 문서 임베딩 (서버 시작 시 1회)
# --------------------------------------------------------
DOC_EMB = []
if len(df):
    texts = df["text"].fillna("").tolist()
    DOC_EMB = []
    for i, t in enumerate(texts):
        try:
            emb = ollama_embed(t)[0]
            DOC_EMB.append(emb)
        except Exception as e:
            print(f"[WARN] {i}번째 청크 임베딩 실패: {e}")
            DOC_EMB.append([0.0] * 768)  # 기본 벡터 크기(예시)
    print(f"[INFO] 임베딩 완료 ({len(DOC_EMB)}개)")

# --------------------------------------------------------
# --- 코사인 유사도 계산 ---
# --------------------------------------------------------
def cosine(q, M):
    q = np.array(q, dtype="float32")
    M = np.array(M, dtype="float32")
    q = q / (np.linalg.norm(q) + 1e-9)
    M = M / (np.linalg.norm(M, axis=1, keepdims=True) + 1e-9)
    return M @ q

# --------------------------------------------------------
# --------- 크롤링 트리거 (관리용) ----------
# --------------------------------------------------------
@app.route("/crawl", methods=["POST"])
def trigger_crawl():
    data = request.get_json(force=True) or {}
    keyword = data.get("keyword", "이혼")
    pages = int(data.get("pages", 1))
    debug = bool(data.get("debug", True))
    delay_ms = data.get("delay_ms", [600, 1200])
    fetch_detail_flag = bool(data.get("detail", True))

    if not isinstance(delay_ms, (list, tuple)) or len(delay_ms) != 2:
        delay_ms = (600, 1200)
    else:
        delay_ms = (int(delay_ms[0]), int(delay_ms[1]))

    out_path, scraped, total = crawl(
        keyword=keyword,
        max_pages=pages,
        delay_ms=delay_ms,
        debug=debug,
        fetch_detail_flag=fetch_detail_flag,
    )
    return jsonify({
        "ok": True,
        "path": str(out_path),
        "count": int(scraped),
        "total": int(total),
        "detail": bool(fetch_detail_flag)
    })

# --------------------------------------------------------
# 헬스체크
# --------------------------------------------------------
@app.get("/health")
def health():
    return {"ok": True, "count": int(len(df))}

# --------------------------------------------------------
# /embed : 단일 텍스트 임베딩
# --------------------------------------------------------
@app.post("/embed")
def embed_api():
    body = request.get_json(silent=True) or {}
    text = body.get("text", "")
    if not text:
        return {"ok": False, "error": "text required"}, 400
    vec = ollama_embed(text)[0]
    return {"ok": True, "embedding": vec}

# --------------------------------------------------------
# /similar : 유사 청크 검색
# --------------------------------------------------------
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
            "case_uid": row.get("case_uid", ""),
            "section": row.get("section_name", ""),
            "snippet": str(row.get("text", ""))[:200] + "...",
            "score": float(sims[i]),
        })
    return {"ok": True, "items": items, "total": int(len(df))}

# --------------------------------------------------------
# /predict : GPT 스타일 답변 생성
# --------------------------------------------------------
@app.post("/predict")
def predict_api():
    body = request.get_json(silent=True) or {}
    prompt = f"""너는 가정법 보조 변호사다.
다음 사건 정보를 간단히 한국어 한 줄로 요약해줘.
입력: {body}
한 줄 요약:"""
    summary = ollama_gen(prompt)
    return {"ok": True, "summary": summary}

# --------------------------------------------------------
# /rag : 질의 + 판례기반 답변
# --------------------------------------------------------
def make_rag_context(df_, indices, max_chars_per_doc=700):
    sources = []
    chunks = []
    for rank, i in enumerate(indices, start=1):
        row = df_.iloc[int(i)]
        uid = str(row.get("case_uid", f"id-{i}"))
        section = str(row.get("section_name", ""))
        text = str(row.get("text", ""))[:max_chars_per_doc]
        sources.append({"rank": rank, "case_uid": uid, "section": section})
        chunks.append(f"[{rank}] 사건: {uid} ({section})\n본문: {text}")
    context = "\n\n---\n\n".join(chunks)
    return context, sources

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
- 제공된 판례의 내용 범위 안에서만 답하라.
- 근거가 부족하면 '판례 범위에서 확답이 어렵다'고 말하라.
- 핵심 위주로 3~5문장 이내로 요약하라.
- 일반인이 이해할 수 있게 쉽게 설명하라.

[최종 답변]
"""
    answer = ollama_gen(prompt)
    for r, i in zip(sources, idx):
        r["score"] = float(sims[i])

    return {"ok": True, "answer": answer, "sources": sources, "top_k": top_k}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
