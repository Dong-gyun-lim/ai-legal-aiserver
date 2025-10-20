from flask import Flask
from data_loader import load_precedents

app = Flask(__name__)

# CSV 데이터 로드
df = load_precedents()

@app.route('/')
def home():
    return f"판례 데이터 {len(df)}건 로드됨!"  # ✅ 데이터 로드 확인

if __name__ == '__main__':
    app.run(debug=True, port=5001)
