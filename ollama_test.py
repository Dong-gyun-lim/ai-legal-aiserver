# ollama_test.py
import requests
import json

OLLAMA_URL = "http://127.0.0.1:11434/api/generate"
MODEL = "qwen2.5:1.5b-instruct"

prompt = "안녕? 오늘 기분 어때?"

payload = {
    "model": MODEL,
    "prompt": prompt,
    "stream": False   # False면 전체 답변을 한 번에 받아옴
}

response = requests.post(OLLAMA_URL, json=payload)
result = response.json()

print("✅ 모델 응답:")
print(result["response"])
