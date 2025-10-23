# data_loader.py
from pathlib import Path
import os, json, csv, re
import pandas as pd

RAW_DIR = Path("data/raw")
OUT_CSV = Path("data/precedents.csv")  # 출력 위치

FIELDS = [
    "case_uid", "case_no", "title", "court", "judgment_date",
    "group", "type", "publish",
    "summary", "reason", "xml",
    "keyword", "_detail",
]

def _clean(s):
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()

def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception as e:
                print(f"[WARN] JSON 파싱 실패: {path.name}:{i} → {e}")

def _build_row(obj: dict) -> dict:
    return {
        "case_uid": obj.get("case_uid") or obj.get("jisCntntsSrno", ""),
        "case_no": obj.get("case_no") or obj.get("csNoLstCtt", ""),
        "title": obj.get("title", ""),
        "court": obj.get("court", ""),
        "judgment_date": obj.get("judgment_date", ""),
        "group": obj.get("group", ""),
        "type": obj.get("type", ""),
        "publish": obj.get("publish", ""),
        "summary": _clean(obj.get("summary", "")),
        "reason": _clean(obj.get("reason", "")),
        "xml": _clean(obj.get("xml", "")),
        "keyword": obj.get("keyword", ""),
        "_detail": obj.get("_detail", ""),
    }

def convert_jsonl_to_csv():
    inputs = sorted(RAW_DIR.glob("*_cases.jsonl"))
    if not inputs:
        print(f"[ERR] 입력 파일이 없습니다: {RAW_DIR/'*_cases.jsonl'}")
        return

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    wrote = 0

    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as wf:
        writer = csv.DictWriter(wf, fieldnames=FIELDS)
        writer.writeheader()
        for p in inputs:
            print(f"[INFO] 읽는 중: {p}")
            for obj in _iter_jsonl(p):
                uid = (obj.get("case_uid") or obj.get("jisCntntsSrno") or "").strip()
                if uid:
                    if uid in seen:
                        continue
                    seen.add(uid)
                writer.writerow(_build_row(obj))
                wrote += 1
    print(f"[DONE] {OUT_CSV} 생성 완료. 총 {wrote}건 (중복제거 후).")

# ↓↓↓ 기존 하위호환 로더 함수 (그대로 놔둬도 됨)
def load_precedents():
    env_path = os.getenv("PRECEDENTS_PATH", "").strip()
    candidates = [p for p in [env_path, "data/precedents.csv", "precedents.csv"] if p]
    path = next((p for p in candidates if os.path.exists(p)), None)
    if not path:
        print(f"[WARN] 파일이 존재하지 않습니다: {candidates}")
        return pd.DataFrame(columns=["id", "title", "text", "label"])

    try:
        df = pd.read_csv(path, encoding="utf-8-sig").fillna("")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="utf-8").fillna("")

    if "id" not in df.columns or df["id"].eq("").all():
        if "case_uid" in df.columns:
            df["id"] = df["case_uid"]
        elif "case_no" in df.columns:
            df["id"] = df["case_no"]
        else:
            df["id"] = ""

    if "title" not in df.columns or df["title"].eq("").all():
        t1 = df["title"] if "title" in df.columns else ""
        t2 = df["case_no"] if "case_no" in df.columns else ""
        df["title"] = (t2.astype(str) + " " + t1.astype(str)).str.strip()

    if "text" in df.columns and df["text"].astype(str).str.strip().ne("").any():
        txt = df["text"].astype(str)
    else:
        summary = df["summary"] if "summary" in df.columns else ""
        reason  = df["reason"]  if "reason"  in df.columns else ""
        xml     = df["xml"]     if "xml"     in df.columns else ""
        txt = (summary.astype(str) + "\n" + reason.astype(str) + "\n" + xml.astype(str))
    df["text"] = txt.map(_clean)

    if "label" in df.columns and df["label"].astype(str).str.strip().ne("").any():
        lab = df["label"].astype(str)
    else:
        candidate_labels = ["court", "type", "keyword"]
        found = next((c for c in candidate_labels if c in df.columns), None)
        lab = df[found].astype(str) if found else ""
    df["label"] = lab

    out = df[["id", "title", "text", "label"]].copy()
    print(f"[INFO] 판례 데이터 로드 완료 ({len(out)}건) from {path}")
    return out

if __name__ == "__main__":
    convert_jsonl_to_csv()
