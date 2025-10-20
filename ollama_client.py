import os
import ollama

HOST = os.getenv("OLLAMA_HOST")
client = ollama.Client(host=HOST) if HOST else ollama

GEN_MODEL = os.getenv("OLLAMA_GEN_MODEL", "llama3.1:8b")
EMB_MODEL = os.getenv("OLLAMA_EMB_MODEL", "nomic-embed-text")

def generate(prompt: str) -> str:
    r = client.generate(model=GEN_MODEL, prompt=prompt)
    return r["response"]

def embed(texts):
    # texts: str or list[str]
    if isinstance(texts, str):
        texts = [texts]

    vecs = []
    for t in texts:
        # ✅ 이 버전에서는 prompt= 로 한 건씩 호출해야 함
        r = client.embeddings(model=EMB_MODEL, prompt=t)
        # 응답 키가 버전에 따라 'embedding' 또는 'embeddings'일 수 있어 안전 처리
        if "embedding" in r:
            vecs.append(r["embedding"])
        elif "embeddings" in r and isinstance(r["embeddings"], list):
            vecs.append(r["embeddings"][0])
        else:
            raise RuntimeError(f"Unexpected embeddings response shape: {r.keys()}")
    return vecs
