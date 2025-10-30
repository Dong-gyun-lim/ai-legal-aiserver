import os, sys, json, time
import requests
from dotenv import load_dotenv

load_dotenv()

OC = os.getenv("LAW_OC")
BASE = "http://www.law.go.kr/DRF/lawService.do"
TIMEOUT = 20

if not OC:
    print("❌ LAW_OC 값이 없습니다. .env에 LAW_OC=g4c 형태로 넣어주세요.")
    sys.exit(1)

def call_list(keyword="이혼", page=1, out_type="JSON"):
    """판례 목록 조회 (listPrec)"""
    params = {
        "OC": OC,
        "target": "listPrec",
        "type": out_type,      # JSON 권장
        "search": keyword,
        "page": page
    }
    r = requests.get(BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    # 일부 케이스에서 JSON 아닌 응답일 수 있어 방어
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}

def call_detail(prec_id: int, out_type="JSON"):
    """판례 본문 조회 (prec)"""
    params = {
        "OC": OC,
        "target": "prec",
        "type": out_type,      # JSON 권장 (특정 라인은 HTML만 제공될 수 있음)
        "ID": prec_id
    }
    r = requests.get(BASE, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {"raw": r.text}

def pretty_print_list(data):
    arr = data.get("PrecSearch", [])
    print(f"✅ 목록 {len(arr)}건")
    for it in arr[:10]:
        print(f"- 사건명:{it.get('사건명')} | 사건번호:{it.get('사건번호')} | ID:{it.get('판례정보일련번호')} | 법원:{it.get('법원명')}")

def pretty_print_detail(obj):
    # JSON 구조: {"PrecService": [ {...} ]} (HTML일 수도 있어 방어)
    if "PrecService" not in obj:
        print("⚠️ JSON 대신 HTML/텍스트가 내려왔습니다. 앞부분만 표시:")
        print(str(obj)[:500])
        return
    info = (obj.get("PrecService") or [{}])[0]
    print("\n🧾 본문 요약")
    print("사건명   :", info.get("사건명"))
    print("사건번호 :", info.get("사건번호"))
    print("선고일자 :", info.get("선고일자"))
    print("법원명   :", info.get("법원명"))
    print("판시사항 :", (info.get("판시사항") or "")[:200], "...")
    print("판결요지 :", (info.get("판결요지") or "")[:200], "...")
    body = (info.get("판례내용") or "")
    print("판례내용 :", body[:500], "..." if len(body) > 500 else "")

if __name__ == "__main__":
    kw = "이혼"  # 필요에 따라 변경 (예: '양육권', '재산분할', '위자료')
    print(f"🔎 목록 조회: keyword='{kw}'")
    data = call_list(keyword=kw, page=1)
    pretty_print_list(data)

    items = data.get("PrecSearch") or []
    if not items:
        print("⚠️ 목록이 비었습니다. 키워드/승인/IP 화이트리스트/요청 파라미터를 확인하세요.")
        sys.exit(0)

    first_id = int(items[0].get("판례정보일련번호"))
    print(f"\n🔎 본문 조회: ID={first_id}")
    detail = call_detail(first_id)
    pretty_print_detail(detail)

    # 저장(옵션)
    out_dir = "law_samples"
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, f"list_{int(time.time())}.json"), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    with open(os.path.join(out_dir, f"detail_{first_id}.json"), "w", encoding="utf-8") as f:
        json.dump(detail, f, ensure_ascii=False, indent=2)
    print(f"\n💾 saved: {out_dir}/list_*.json, {out_dir}/detail_{first_id}.json")
