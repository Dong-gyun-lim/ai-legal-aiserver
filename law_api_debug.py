import os, sys, urllib.parse
import requests
from dotenv import load_dotenv

load_dotenv()
OC = os.getenv("LAW_OC")  # 예: qkrwlscks3  (이메일 아이디 부분만)
BASE = "http://www.law.go.kr/DRF/lawService.do"

def get(url, params):
    try:
        r = requests.get(url, params=params, timeout=30)
        print("URL =", r.url)
        print("STATUS =", r.status_code)
        print("CT =", r.headers.get("Content-Type"))
        # 에러여도 본문 먼저 보여줌
        print("---- BODY HEAD ----")
        print(r.text[:1200])
        print("---- /BODY HEAD ----")
        return r
    except Exception as e:
        print("REQ ERROR:", e)
        return None

if __name__ == "__main__":
    if not OC:
        print("❌ .env에 LAW_OC가 없습니다.")
        sys.exit(1)

    kw = "이혼"

    print("\n[1] listPrec (JSON)")
    get(BASE, {"OC": OC, "target": "listPrec", "type": "JSON", "search": kw, "page": 1})

    print("\n[2] listPrec (XML)  # 에러는 XML로만 설명될 때가 많음")
    get(BASE, {"OC": OC, "target": "listPrec", "type": "XML", "search": kw, "page": 1})

    print("\n[3] prec 샘플(ID=228541) (JSON)")
    get(BASE, {"OC": OC, "target": "prec", "type": "JSON", "ID": 228541})

    print("\n[4] prec 샘플(ID=228541) (XML)")
    get(BASE, {"OC": OC, "target": "prec", "type": "XML", "ID": 228541})
