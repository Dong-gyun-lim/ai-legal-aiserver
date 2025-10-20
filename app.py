from flask import Flask, request, jsonify
from flask_cors import CORS
import numpy as np
from data_loader import load_precedents
from ollama_client import embed as ollama_embed, generate as ollama_gen

app = Flask(__name__)
CORS(app)

# 1) 데이터 로드
df = load_precedents()

# 2) 문서 임베딩 (서버 시작 시 1회)
DOC_EMB = []
if len(df):
    DOC_EMB = ollama_embed(df["text"].fillna("").tolist())

def cosine(q, M):
    q = np.array(q, dtype="float32")
    M = np.array(M, dtype="float32")
    q = q / (np.linalg.norm(q) + 1e-9)
    M = M / (np.linalg.norm(M, axis=1, keepdims=True) + 1e-9)
    return M @ q

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
    if not len(df):
        return {"ok": True, "items": [], "total": 0}
    body = request.get_json(silent=True) or {}
    query = body.get("query", "")
    top_k = int(body.get("top_k", 5))
    if not query:
        return {"ok": False, "error": "query required"}, 400
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
    # 데모: 입력을 한 줄 요약으로 생성
    body = request.get_json(silent=True) or {}
    prompt = f"""너는 가정법 보조 변호사다.
다음 사건 정보를 간단히 한국어 한 줄로 요약해줘.
입력: {body}
한 줄 요약:"""
    summary = ollama_gen(prompt)
    return {"ok": True, "summary": summary}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
