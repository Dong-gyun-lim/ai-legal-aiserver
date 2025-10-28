# build_faiss_index.py
# 목적: MariaDB의 precedent_chunks를 읽어 E5 임베딩 후 FAISS 인덱스를 저장
import os, json, math
import numpy as np
import pymysql
from pathlib import Path
from sentence_transformers import SentenceTransformer
import faiss
from dotenv import load_dotenv

# .env를 확실히 찾도록 현재 파일과 같은 폴더의 .env를 지정
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

# ✅ 디버그용 프린트
print("[ENV]", os.getenv("DB_HOST"), os.getenv("DB_USER"), os.getenv("DB_PASS"))

DATA_DIR = Path("vectorstore")
DATA_DIR.mkdir(parents=True, exist_ok=True)
INDEX_PATH = DATA_DIR / "faiss.index"
ID_PATH    = DATA_DIR / "ids.npy"
META_PATH  = DATA_DIR / "meta.jsonl"

# E5 임베딩 (중요: prefix 사용)
EMBED_MODEL = "intfloat/multilingual-e5-base"
BATCH = 64

def connect():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", ""),
        database=os.getenv("DB_NAME", "test"),
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def fetch_chunks(cur, limit=None):
    # section_name, chunk_index, text, precedent_id, 그리고 조인으로 case_no 가져오기
    sql = """
    SELECT pc.id, pc.precedent_id, pc.section_name, pc.chunk_index, pc.text, p.case_no
    FROM precedent_chunks pc
    JOIN precedents p ON p.id = pc.precedent_id
    WHERE pc.text IS NOT NULL AND LENGTH(pc.text) > 20
    ORDER BY pc.precedent_id, pc.section_name, pc.chunk_index
    """
    if limit:
        sql += f" LIMIT {int(limit)}"
    cur.execute(sql)
    return cur.fetchall()

def main():
    print("[1/4] DB 접속 및 데이터 로딩…")
    conn = connect()
    with conn.cursor() as cur:
        rows = fetch_chunks(cur)

    print(f" - 청크 개수: {len(rows)}")

    print("[2/4] 임베딩 모델 로딩…")
    model = SentenceTransformer(EMBED_MODEL)

    print("[3/4] 문서 임베딩 생성(E5, passage prefix)…")
    texts = [f"passage: {r['text']}" for r in rows]
    embs = []
    for i in range(0, len(texts), BATCH):
        batch = texts[i:i+BATCH]
        vec = model.encode(batch, normalize_embeddings=True, show_progress_bar=False)
        embs.append(vec.astype("float32"))
    X = np.vstack(embs)  # (N, 768) float32
    print(" - 임베딩 shape:", X.shape)

    print("[3.5/4] 메타/ID 저장…")
    ids = np.array([r["id"] for r in rows], dtype=np.int64)
    np.save(ID_PATH, ids)
    with META_PATH.open("w", encoding="utf-8") as fw:
        for r in rows:
            fw.write(json.dumps({
                "id": r["id"],
                "precedent_id": r["precedent_id"],
                "case_no": r["case_no"],
                "section_name": r["section_name"],
                "chunk_index": r["chunk_index"]
            }, ensure_ascii=False) + "\n")

    print("[4/4] FAISS IndexFlatIP(내적 기반, L2 norm 전제) 생성/저장…")
    index = faiss.IndexFlatIP(X.shape[1])
    index.add(X)
    faiss.write_index(index, str(INDEX_PATH))

    print(f"\n[DONE] 인덱스 저장 완료\n - {INDEX_PATH}\n - {ID_PATH}\n - {META_PATH}")

if __name__ == "__main__":
    main()
