import os
import json
# 중요: google 패키지 안에서 genai를 불러옵니다.
from google import genai

# API 키 설정
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

SYSTEM_PROMPT = """
너는 전문적인 K-beauty 트렌드 분석가야.
사용자가 제공하는 키워드, 지역 정보, 핵심 지표(WoW, z-score 등)를 분석해서 인사이트를 도출해줘.
반드시 한국어로 작성하고, JSON 형식으로만 응답해.
"""


def analyze_term(term: str, geo: str, metrics: dict) -> dict:
    prompt = f"""
    분석 키워드: "{term}"
    지역: "{geo}"
    핵심 지표 데이터: {json.dumps(metrics)}

    위 데이터를 바탕으로 다음 항목을 분석해서 JSON으로 반환해:
    1. expectation (기대 포인트)
    2. importance (왜 중요한가)
    3. actions (추천 액션 리스트)
    """

    response = client.models.generate_content(
        # model="gemini-3-flash-preview", 
        model = "gemini-2.5-flash-lite",
        contents=prompt,
        config={
            "system_instruction": SYSTEM_PROMPT,
            "response_mime_type": "application/json"
        }
    )
    try:
        # 최신 SDK는 response.text 대신 직접 파싱 가능하거나 .text를 사용합니다.
        return json.loads(response.text)
    except Exception as e:
        return {"error": str(e), "raw": response.text}
