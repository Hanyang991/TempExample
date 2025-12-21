from __future__ import annotations
from dataclasses import dataclass

@dataclass
class InsightCard:
    title: str
    expectation: str
    why: str
    action: str


RULES = [
    # 1️⃣ 장벽/진정
    (
        ["barrier", "ceramide", "panthenol", "cica", "centella", "heartleaf"],
        "피부 장벽 강화 및 저자극 진정 기대",
        "민감 피부 및 장벽 손상 관련 검색 증가",
        "장벽 강화 임상/전후 비교 강조 + 진정 라인 확장 또는 세트화"
    ),

    # 2️⃣ 색소/톤 개선
    (
        ["hyperpigmentation", "dark spot", "melasma", "tranexamic", "azelaic", "vitamin c"],
        "잡티·기미·톤 개선에 대한 가시적 효과 기대",
        "미백/톤 균일 니즈가 성분 중심으로 세분화됨",
        "색소 단계별 루틴 제안 + 성분 조합 인포그래픽 강화"
    ),

    # 3️⃣ 트러블/저자극
    (
        ["acne", "fungal acne", "non comedogenic", "oil control"],
        "트러블 완화 및 모공 막힘 없는 사용감 기대",
        "여드름 피부용 안전성/논코메도 검색 증가",
        "논코메도 테스트 근거 명시 + 트러블 루틴 숏폼 콘텐츠 제작"
    ),

    # 4️⃣ 안티에이징/재생
    (
        ["retinol", "bakuchiol", "peptide", "pdrn", "anti aging", "wrinkle"],
        "저자극 안티에이징 및 피부 재생 효과 기대",
        "강한 레티놀 대체 성분 및 재생 키워드 부상",
        "민감피부 사용 가이드 제공 + 야간 루틴 콘텐츠 연계"
    ),

    # 5️⃣ 수분/보습
    (
        ["hydrating", "hyaluronic", "beta glucan", "cream toner", "essence"],
        "속건조 개선 및 장시간 보습 유지 기대",
        "보습을 ‘수분 유지력’ 중심으로 인식 전환",
        "보습 지속력 데이터 강조 + 레이어링 루틴 제안"
    ),

    # 6️⃣ 선케어
    (
        ["sunscreen", "sun stick", "tone up", "spf"],
        "가볍고 백탁 없는 데일리 선케어 기대",
        "메이크업 전 사용 가능한 선케어 니즈 증가",
        "메이크업 궁합 테스트 + 휴대성/덧바름 USP 강조"
    ),

    # 7️⃣ 베이스 메이크업
    (
        ["cushion", "foundation", "base makeup"],
        "빠르고 밀착력 높은 베이스 메이크업 기대",
        "쿠션 중심의 루틴 단축 트렌드 확산",
        "커버력/지속력 비교 콘텐츠 + 피부 타입별 추천"
    ),

    # 8️⃣ K-Beauty / SNS 바이럴
    (
        ["k beauty", "korean skincare", "tiktok", "viral", "amazon"],
        "검증된 K-뷰티 및 바이럴 제품에 대한 신뢰 기대",
        "SNS 기반 구매 의사결정 비중 확대",
        "숏폼 리뷰/UGC 확보 + 글로벌 PDP 현지화 강화"
    ),

    # 9️⃣ 구매 결정 직전
    (
        ["dermatologist", "fragrance free", "vegan", "cruelty free", "pregnancy"],
        "안전성·윤리성·전문가 추천에 대한 신뢰 기대",
        "성분 안정성 및 가치 소비 키워드 급부상",
        "인증/테스트 배지 시각화 + FAQ 강화"
    ),
]


def make_insight(term: str) -> InsightCard:
    t = term.lower()
    for keys, exp, why, action in RULES:
        if any(k in t for k in keys):
            return InsightCard(
                title=f'“{term}”',
                expectation=exp,
                why=why,
                action=action
            )

    return InsightCard(
        title=f'“{term}”',
        expectation="효능 및 사용 맥락에 대한 명확한 설명 기대",
        why="검색 관심이 증가하나 니즈가 아직 명확히 분화되지 않음",
        action="연관 성분/루틴 키워드 확장 테스트 + 콘텐츠 반응 확인"
    )
