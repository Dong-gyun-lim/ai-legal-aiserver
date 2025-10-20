import os
import pandas as pd

def load_precedents():
    """
    판례 CSV 파일을 로드합니다.
    - 기본 경로: data/precedents.csv
    - 파일이 없거나 컬럼이 부족하면 안전하게 기본값 처리
    """
    path = os.getenv("PRECEDENTS_PATH", "data/precedents.csv")

    if not os.path.exists(path):
        print(f"[WARN] 파일이 존재하지 않습니다: {path}")
        return pd.DataFrame(columns=["id", "title", "text", "label"])

    df = pd.read_csv(path).fillna("")
    
    # 필수 컬럼 보장
    required_cols = ["id", "title", "text", "label"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = ""
    
    print(f"[INFO] 판례 데이터 로드 완료 ({len(df)}건)")
    return df
