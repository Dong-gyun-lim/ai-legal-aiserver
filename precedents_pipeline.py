# precedents_pipeline.py
# - 한 번에: (선택)초기화 → 적재(섹션/청크) → 메타 보정
# - year는 생성 컬럼일 수 있으므로 절대 INSERT/UPDATE 하지 않음
# - case_no UNIQUE 충돌 시: case_no 업데이트는 건너뛰고 나머지만 보정

import os
import re
import json
import argparse
import datetime
from collections import defaultdict

import pymysql
from dotenv import load_dotenv

load_dotenv()

# ---------- 정규식 패턴 ----------
COURT_PAT = re.compile(r'(대법원|고등법원|가정법원|지방법원)')
DATE_PAT  = re.compile(r'(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?')     # 2020. 5. 14. / 1996.04.26.
CASE_NO_PAT = re.compile(r'\b(\d{2,4}[가-힣]{1,2}\d{1,6})\b')         # 2019므15302, 2017다233849 ...
TYPE_PAT  = re.compile(r'(이혼|재산분할|양육비|친권|면접교섭|위자료)')

# ---------- DB 연결 ----------
def connect():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", ""),
        database=os.getenv("DB_NAME", "test"),
        charset="utf8mb4",
        autocommit=False,
    )

# ---------- 공통 유틸 ----------
def read_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

# ---------- 1) 데이터 수집 ----------
def collect_meta_from_sections(sections_path):
    """
    clean_sections_v2.jsonl에서 판례별 메타 구성
      - summary: section_name in {"판결요지","요지"} 중 최초 1개
      - issues_text: section_name == "판시사항" 전부 합치기
      - full_text: 모든 섹션을 '【섹션명】\n본문' 형태로 이어붙임
    """
    meta = defaultdict(lambda: {"summary": "", "issues_text": "", "full_text": ""})
    buckets = defaultdict(list)

    for row in read_jsonl(sections_path):
        uid = row["case_uid"]
        sec = row.get("section_name", "").strip()
        txt = str(row.get("text", "")).strip()
        buckets[uid].append((sec, txt))

    for uid, items in buckets.items():
        full_parts, issues_parts = [], []
        summary = ""

        for sec, txt in items:
            if not txt:
                continue
            if not summary and sec in {"판결요지", "요지"}:
                summary = txt
            if sec == "판시사항":
                issues_parts.append(txt)
            full_parts.append(f"【{sec}】\n{txt}")

        meta[uid]["summary"]     = summary
        meta[uid]["issues_text"] = "\n\n".join(issues_parts)
        meta[uid]["full_text"]   = "\n\n".join(full_parts)

    return meta

def read_chunks(chunks_path):
    out = defaultdict(list)
    for row in read_jsonl(chunks_path):
        out[row["case_uid"]].append({
            "section_name": row.get("section_name", ""),
            "chunk_index": int(row.get("chunk_index", 0)),
            "text": str(row.get("text", "")).strip()
        })
    for uid in out:
        out[uid].sort(key=lambda x: (x["section_name"], x["chunk_index"]))
    return out

# ---------- 2) 적재(UPSERT) ----------
def upsert_precedent(cur, uid, meta):
    """
    precedents 테이블에 판례 1건 UPSERT.
    - 초기 case_no에 uid(숫자 UID)를 넣고, 나중에 메타 보정에서 정규 사건번호로 치환 가능
    - year는 생성 컬럼일 수 있으므로 INSERT/UPDATE 모두에서 배제
    """
    court = meta.get("court") or None  # 별도 추정 로직을 원하면 여기에 추가

    sql = """
    INSERT INTO precedents
      (case_no, court, summary, full_text, source_url, alimony_amount, custody_to, issues_text, created_at, updated_at)
    VALUES
      (%s, %s, %s, %s, NULL, NULL, NULL, %s, NOW(), NOW())
    ON DUPLICATE KEY UPDATE
      court=VALUES(court),
      summary=VALUES(summary),
      full_text=VALUES(full_text),
      issues_text=VALUES(issues_text),
      updated_at=NOW()
    """
    cur.execute(sql, (
        uid,
        court,
        meta.get("summary", ""),
        meta.get("full_text", ""),
        meta.get("issues_text", ""),
    ))
    cur.execute("SELECT id FROM precedents WHERE case_no=%s", (uid,))
    return cur.fetchone()[0]

def replace_chunks(cur, precedent_id, chunks):
    cur.execute("DELETE FROM precedent_chunks WHERE precedent_id=%s", (precedent_id,))
    if not chunks:
        return
    sql = "INSERT INTO precedent_chunks (precedent_id, section_name, chunk_index, text, created_at) VALUES (%s,%s,%s,%s,NOW())"
    data = [(precedent_id, c.get("section_name",""), c["chunk_index"], c["text"]) for c in chunks]
    cur.executemany(sql, data)

# ---------- 3) 메타 보정 ----------
def parse_meta_from_full_text(full_text: str):
    """full_text 상단에서 case_no, court, judgment_date, type 추출 (year는 반환만, DB에 쓰지 않음)"""
    if not full_text:
        return None, None, None, None, None
    head = full_text[:2000]

    case_no = None
    m = CASE_NO_PAT.search(head)
    if m:
        case_no = m.group(1)

    court = None
    m = COURT_PAT.search(head)
    if m:
        court = m.group(1)

    jdate = None
    year  = None
    m = DATE_PAT.search(head)
    if m:
        y, mo, d = map(int, m.groups())
        try:
            jdate = datetime.date(y, mo, d)
            year = y
        except ValueError:
            pass

    typ = None
    m = TYPE_PAT.search(head)
    if m:
        typ = m.group(1)

    return case_no, court, jdate, typ, year

def fill_case_uid_from_numeric_case_no(cur):
    """case_uid가 NULL이고 case_no가 숫자만이면 case_uid ← case_no"""
    cur.execute("""
        UPDATE precedents
        SET case_uid = case_no
        WHERE case_uid IS NULL AND case_no REGEXP '^[0-9]+$'
    """)

def fix_meta(cur):
    """메타 보정: 정규 사건번호/법원/선고일/유형 업데이트
       - case_no UNIQUE 충돌시: case_no 업데이트는 스킵하고 나머지만 보정
       - year는 생성 컬럼일 수 있으므로 절대 쓰지 않음
    """
    fill_case_uid_from_numeric_case_no(cur)

    cur.execute("""
        SELECT id, case_no, full_text
        FROM precedents
        WHERE (court IS NULL OR judgment_date IS NULL OR type IS NULL
               OR case_no REGEXP '^[0-9]+$')
    """)
    rows = cur.fetchall()

    fixed = 0
    for pid, case_no_now, full_text in rows:
        if not full_text or str(full_text).startswith('{"timestamp"'):
            # 크롤링 실패 로그/찌꺼기는 스킵
            continue

        case_no, court, jdate, typ, _year = parse_meta_from_full_text(full_text)

        sets, params = [], []

        # 숫자형이면 정규 사건번호로 교체 시도 (단, 중복 있으면 스킵)
        if case_no and not re.match(r'^[0-9]+$', case_no):
            cur.execute(
                "SELECT id FROM precedents WHERE case_no=%s AND id<>%s LIMIT 1",
                (case_no, pid),
            )
            dup = cur.fetchone()
            if not dup:
                sets.append("case_no=%s"); params.append(case_no)

        if court and court.strip():
            sets.append("court=%s"); params.append(court)

        if jdate:
            sets.append("judgment_date=%s"); params.append(jdate)

        if typ:
            sets.append("type=%s"); params.append(typ)

        # year는 생성 컬럼 → 절대 쓰지 않음

        if sets:
            q = f"UPDATE precedents SET {', '.join(sets)}, updated_at=NOW() WHERE id=%s"
            params.append(pid)
            cur.execute(q, params)
            fixed += 1

    return fixed

# ---------- 0) 초기화/청소 ----------
def truncate_all(cur):
    """precedents / precedent_chunks 데이터만 비우기 (구조 유지, AI키 초기화)"""
    cur.execute("SET FOREIGN_KEY_CHECKS=0;")
    cur.execute("TRUNCATE TABLE precedent_chunks;")
    cur.execute("TRUNCATE TABLE precedents;")
    cur.execute("SET FOREIGN_KEY_CHECKS=1;")

def delete_garbage(cur):
    """크롤링 실패 로그 형태의 쓰레기(full_text가 {"timestamp"...})/summary NULL 행 정리(선택)"""
    cur.execute("DELETE FROM precedents WHERE full_text REGEXP '^{\\\"timestamp\\\"';")
    cur.execute("DELETE FROM precedents WHERE summary IS NULL;")

# ---------- 메인 ----------
def main():
    ap = argparse.ArgumentParser(description="Precedents end-to-end pipeline (reset → ingest → fix-meta)")
    ap.add_argument("--sections", default="data/processed/clean_sections_v2.jsonl")
    ap.add_argument("--chunks",   default="data/processed/chunks_cleaned.jsonl")
    ap.add_argument("--reset", action="store_true", help="테이블 데이터 TRUNCATE 후 실행")
    ap.add_argument("--clean", action="store_true", help="로그/찌꺼기 행 정리(Delete garbage)")
    ap.add_argument("--no-ingest", action="store_true", help="적재 단계 건너뛰기")
    ap.add_argument("--fix-only", action="store_true", help="메타보정만 수행 (적재/초기화 없음)")
    ap.add_argument("--no-fix", action="store_true", help="메타 보정 건너뛰기")
    args = ap.parse_args()

    # 입력 파일 체크 (ingest를 수행할 때만)
    if not args.fix_only and not args.no_ingest:
        if not os.path.exists(args.sections):
            raise FileNotFoundError(f"sections file not found: {args.sections}")
        if not os.path.exists(args.chunks):
            raise FileNotFoundError(f"chunks file not found: {args.chunks}")

    conn = connect()
    try:
        with conn.cursor() as cur:
            if args.reset:
                truncate_all(cur)
                print("[RESET] tables truncated.")

            if args.clean:
                delete_garbage(cur)
                print("[CLEAN] garbage rows deleted.")

            total_ingested = 0
            if not args.fix_only and not args.no_ingest:
                meta = collect_meta_from_sections(args.sections)
                chunks = read_chunks(args.chunks)

                for uid, m in meta.items():
                    cs = chunks.get(uid, [])
                    if not cs:
                        continue
                    pid = upsert_precedent(cur, uid, m)  # year 미사용
                    replace_chunks(cur, pid, cs)
                    total_ingested += 1

                print(f"[INGEST] precedents/chunks upsert 완료: {total_ingested}건")

            fixed = 0
            if not args.no_fix:
                fixed = fix_meta(cur)  # case_no 충돌 회피 + year 미사용
                print(f"[FIX] 메타 보정 완료: {fixed}건 갱신")

        conn.commit()
        print("[DONE] all operations completed.")
    except Exception as e:
        conn.rollback()
        print("[ERROR]", e)
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
