# engine.py (FIXED / MERGED / FULL)
import os
import re
import json
import requests
from datetime import datetime, timedelta, date

import pandas as pd
import streamlit as st
from supabase import create_client, Client

# (Optional) Gemini SDK
try:
    import google.generativeai as genai
except Exception:
    genai = None


# =============================================================================
# Secrets & Clients
# =============================================================================

def _safe_secrets() -> dict:
    try:
        _ = st.secrets
        return dict(st.secrets)
    except Exception:
        return {}

SECRETS = _safe_secrets()

SUPABASE_URL = SECRETS.get("SUPABASE_URL", os.getenv("SUPABASE_URL", "https://qipphcdzlmqidhrjnjtt.supabase.co")).strip()
SUPABASE_KEY = SECRETS.get("SUPABASE_KEY", os.getenv("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFpcHBoY2R6bG1xaWRocmpuanR0Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjY5NTIwMTIsImV4cCI6MjA4MjUyODAxMn0.AsuvjVGCLUJF_IPvQevYASaM6uRF2C6F-CjwC3eCNVk")).strip()
GEMINI_API_KEY = SECRETS.get("GEMINI_API_KEY", os.getenv("GEMINI_API_KEY", "AIzaSyAQaiwm46yOITEttdr0ify7duXCW3TwGRo")).strip()

# Hybrid plan table name (configurable)
HYBRID_PLAN_TABLE = SECRETS.get("HYBRID_PLAN_TABLE", "production_plan_2026_01")
HYBRID_HIST_TABLE = SECRETS.get("HYBRID_HIST_TABLE", "production_investigation")

# Legacy default year (configurable)
LEGACY_DEFAULT_YEAR = str(SECRETS.get("LEGACY_DEFAULT_YEAR", "2025"))

# Hybrid config
HYBRID_TEST_MODE = bool(SECRETS.get("HYBRID_TEST_MODE", True))
HYBRID_TODAY_STR = str(SECRETS.get("HYBRID_TODAY", "2026-01-05"))
HYBRID_FROZEN_DAYS = int(SECRETS.get("HYBRID_FROZEN_DAYS", 3))

CAPA_LIMITS_DEFAULT = SECRETS.get("CAPA_LIMITS", {"조립1": 3300, "조립2": 3700, "조립3": 3600})

# Hybrid report style config (restore "수사 리포트" 느낌)
HYBRID_REPORT_STYLE = str(SECRETS.get("HYBRID_REPORT_STYLE", "investigation")).lower()  # investigation | simple
HYBRID_DEFAULT_TARGET_UTIL = float(SECRETS.get("HYBRID_DEFAULT_TARGET_UTIL", 0.81))  # 예시 리포트의 81%
HYBRID_TARGET_ROUNDING = int(SECRETS.get("HYBRID_TARGET_ROUNDING", 100))  # 목표 수량 반올림 단위(100단위 등)


@st.cache_resource
def init_supabase() -> Client | None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return None
    try:
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception:
        return None

supabase: Client | None = init_supabase()

if genai and GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
    except Exception:
        pass


# =============================================================================
# Router
# =============================================================================

HYBRID_INTENT_WORDS = [
    "감축", "줄여", "줄이고", "낮춰", "줄여줘",
    "증량", "늘려", "늘리고", "추가", "샘플",
    "이송", "옮겨", "연기", "미뤄", "당겨", "선행",
    "가동률", "목표", "맞춰", "하이브리드", "수사", "검증", "전략",
]
LEGACY_FORCE_WORDS = [
    "사례", "이슈",
    "브리핑", "월간",
    "초과",  # "CAPA 초과"는 레거시가 더 자연스러움(월 단위 조회)
    "fan", "motor", "flange", "팬", "모터", "플랜지",
    "0차", "최종", "납기", "생산일",
]

def _extract_year(prompt: str, default_year: str) -> str:
    m = re.search(r"(20\d{2})\s*년", prompt)
    if m:
        return m.group(1)
    m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})", prompt)
    if m:
        return m.group(1)
    return default_year

def _get_hybrid_today() -> date:
    if HYBRID_TEST_MODE:
        try:
            return datetime.strptime(HYBRID_TODAY_STR, "%Y-%m-%d").date()
        except Exception:
            return date(2026, 1, 5)
    return datetime.now().date()

def _extract_date_any(prompt: str, default_year: str = "2026") -> str | None:
    """
    Returns YYYY-MM-DD if found.
    Supports:
      - YYYY-MM-DD
      - M/D
      - M월 D일
      - 오늘/내일/모레 (uses hybrid TODAY)
    """
    p = prompt.strip()

    if any(k in p for k in ["오늘", "내일", "모레"]):
        today = _get_hybrid_today()
        if "오늘" in p:
            return today.strftime("%Y-%m-%d")
        if "내일" in p:
            return (today + timedelta(days=1)).strftime("%Y-%m-%d")
        if "모레" in p:
            return (today + timedelta(days=2)).strftime("%Y-%m-%d")

    m = re.search(r'(20\d{2})-(\d{1,2})-(\d{1,2})', p)
    if m:
        yy, mm, dd = m.groups()
        return f"{int(yy):04d}-{int(mm):02d}-{int(dd):02d}"

    m = re.search(r'(\d{1,2})/(\d{1,2})', p)
    if m:
        mm, dd = m.groups()
        return f"{int(default_year):04d}-{int(mm):02d}-{int(dd):02d}"

    m = re.search(r'(\d{1,2})월\s*(\d{1,2})일', p)
    if m:
        mm, dd = m.groups()
        return f"{int(default_year):04d}-{int(mm):02d}-{int(dd):02d}"

    return None

def _has_adjustment_intent(prompt: str) -> bool:
    p = prompt.lower()
    if re.search(r"\d+\s*%", p):
        return True
    return any(w in p for w in [x.lower() for x in HYBRID_INTENT_WORDS])

def classify_route(prompt: str) -> tuple[str, dict]:
    meta = {}

    if "사례" in prompt:
        meta["reason"] = "force_legacy_case"
        return "legacy", meta

    p_lower = prompt.lower()

    if any(w.lower() in p_lower for w in [x.lower() for x in LEGACY_FORCE_WORDS]):
        if _has_adjustment_intent(prompt):
            meta["reason"] = "legacy_word_but_adjustment_intent"
            return "hybrid", meta
        meta["reason"] = "force_legacy_words"
        return "legacy", meta

    if ("capa" in p_lower or "카파" in prompt) and not _has_adjustment_intent(prompt):
        meta["reason"] = "capa_lookup_legacy"
        return "legacy", meta

    if _has_adjustment_intent(prompt):
        meta["reason"] = "adjustment_intent"
        return "hybrid", meta

    meta["reason"] = "default_legacy"
    return "legacy", meta


# =============================================================================
# Legacy
# =============================================================================

def extract_date_info_legacy(text: str, default_year: str):
    year = _extract_year(text, default_year)
    info = {"date": None, "month": None, "year": year}

    m = re.search(r"(20\d{2})-(\d{1,2})-(\d{1,2})", text)
    if m:
        yy, mm, dd = m.groups()
        info["year"] = yy
        info["month"] = int(mm)
        info["date"] = f"{int(yy):04d}-{int(mm):02d}-{int(dd):02d}"
        return info

    match_date = re.search(r"(\d{1,2})월\s*(\d{1,2})일", text)
    if match_date:
        mm, dd = match_date.groups()
        info["month"] = int(mm)
        info["date"] = f"{int(info['year']):04d}-{int(mm):02d}-{int(dd):02d}"
        return info

    match_month = re.search(r"(\d{1,2})월", text)
    if match_month:
        info["month"] = int(match_month.group(1))

    return info

def extract_version(text: str) -> str:
    if "0차" in text or "초기" in text or "계획" in text:
        return "0차"
    return "최종"

def extract_product_keyword(text: str) -> str | None:
    ignore_words = [
        "생산량","알려줘","비교해줘","비교","제품","최종","0차","월","일","capa","카파",
        "초과","어떻게","돼","있어","사례","총","월간","브리핑",
        "fan","motor","flange","팬","모터","플랜지",
        "조립1","조립2","조립3",
        "늘려","증량","증가",
    ]
    words = text.split()
    for w in words:
        clean = re.sub(r"[^a-zA-Z0-9가-힣]", "", w)
        if clean and clean.lower() not in [x.lower() for x in ignore_words] and not re.match(r"\d+(월|일)", clean):
            return clean
    return None

def normalize_line_name(line_val):
    s = str(line_val).strip()
    if s == "1": return "조립1"
    if s == "2": return "조립2"
    if s == "3": return "조립3"
    if "조립" in s: return s
    return s

def normalize_date(date_val):
    if not date_val:
        return ""
    s = str(date_val).strip()
    return s[:10] if len(s) >= 10 else s

def _is_month_total_query(user_input: str) -> bool:
    u = user_input.replace(" ", "")
    # "00월 총 생산량", "00월 총생산량", "00월 생산량(총)" 등
    if "월" not in u:
        return False
    if "총" in u and "생산" in u:
        return True
    # "00월 생산량 알려줘" 같은 질의도 총 생산량으로 취급(제품키워드 없을 때만 적용)
    if "생산량" in u and ("알려줘" in u or "알려" in u or "얼마" in u):
        return True
    return False

def fetch_db_data_legacy(user_input: str) -> str:
    if supabase is None:
        return "SUPABASE_URL/SUPABASE_KEY가 설정되지 않아 DB 조회를 할 수 없습니다. Streamlit Secrets를 확인하세요."

    info = extract_date_info_legacy(user_input, LEGACY_DEFAULT_YEAR)
    target_date = info["date"]
    target_month = info["month"]
    target_version = extract_version(user_input)
    product_key = extract_product_keyword(user_input)

    try:
        # =====================================================================
        # 0) 생산량 증량 사례 검색 (NEW - 최우선 순위)
        # =====================================================================
        if ("늘려" in user_input or "증량" in user_input or "증가" in user_input) and "사례" in user_input:
            query = supabase.table("final_issue").select("날짜, 품목명, 생산량, final_role, final_remark")
            query = query.or_("final_remark.ilike.%긴급 물량 증량%,final_remark.ilike.%품목간 간섭%")
            response = query.execute()
            
            if response.data:
                date_groups = {}
                for item in response.data:
                    date_key = normalize_date(item.get('날짜', ''))
                    if not date_key:
                        continue
                    if date_key not in date_groups:
                        date_groups[date_key] = {'선순위': [], '후순위': []}
                    
                    role = item.get('final_role', '')
                    if '선순위' in role:
                        date_groups[date_key]['선순위'].append(item)
                    elif '후순위' in role:
                        date_groups[date_key]['후순위'].append(item)
                
                valid_cases = []
                for date_key, roles in date_groups.items():
                    if roles['선순위'] and roles['후순위']:
                        valid_cases.append({
                            'date': date_key,
                            'increased': roles['선순위'],
                            'decreased': roles['후순위']
                        })
                
                if valid_cases:
                    context = "[PRODUCTION_INCREASE CASE FOUND]\n"
                    context += "Title: 생산량 증량 사례 (품목간 우선순위 조정)\n"
                    context += "Data:\n"
                    
                    for case in valid_cases[:3]:
                        context += f"\n[날짜: {case['date']}]\n"
                        context += "증가(선순위):\n"
                        for item in case['increased']:
                            context += f"  - {item['품목명']}: {item['생산량']}\n"
                        context += "감소(후순위):\n"
                        for item in case['decreased']:
                            context += f"  - {item['품목명']}: {item['생산량']}\n"
                    
                    return context
                else:
                    return "같은 날짜에 선순위 증가와 후순위 감소가 함께 발생한 사례를 찾을 수 없습니다."
            else:
                return "생산량 증량 관련 과거 사례를 찾을 수 없습니다."

        # =====================================================================
        # 1) 과거 이슈 사례
        # =====================================================================
        if "사례" in user_input:
            issue_mapping = {
                "MDL1": {"keywords": ["먼저", "줄여", "순위", "교체"], "db_text": "생산순위 조정",
                         "title": "MDL1: 미달(생산순위 조정/모델 교체)"},
                "MDL2": {"keywords": ["감사", "정지", "설비", "라인전체"], "db_text": "라인전체이슈",
                         "title": "MDL2: 미달(라인전체이슈/설비)"},
                "MDL3": {"keywords": ["부품", "자재", "결품", "수급", "안되는"], "db_text": "자재결품",
                         "title": "MDL3: 미달(부품수급/자재결품)"},
                "PRP": {"keywords": ["선행", "미리", "당겨", "땡겨"], "db_text": "선행 생산",
                        "title": "PRP: 선행 생산(숙제 미리하기)"},
                "SMP": {"keywords": ["샘플", "긴급"], "db_text": "계획외 긴급 생산",
                        "title": "SMP: 계획외 긴급 생산"},
                "CCL": {"keywords": ["취소"], "db_text": "계획 취소", "title": "CCL: 계획 취소/라인 가동중단"},
            }
            detected_code = None
            for code, meta in issue_mapping.items():
                if any(k in user_input for k in meta["keywords"]):
                    detected_code = code
                    break

            if detected_code:
                meta = issue_mapping[detected_code]
                query = supabase.table("production_issue_analysis_8_11") \
                    .select("품목명, 날짜, 계획_v0, 실적_v2, 누적차이_Gap, 최종_이슈분류")

                if detected_code == "MDL2":
                    query = query.or_("최종_이슈분류.ilike.%라인전체이슈%,최종_이슈분류.ilike.%설비%")
                elif detected_code == "MDL3":
                    query = query.or_("최종_이슈분류.ilike.%부품수급%,최종_이슈분류.ilike.%자재결품%")
                else:
                    query = query.ilike("최종_이슈분류", f"%{meta['db_text']}%")

                res = query.limit(3).execute()
                if res.data:
                    return (
                        f"[CODE CASE FOUND]\n"
                        f"Code: {detected_code}\n"
                        f"Title: {meta['title']}\n"
                        f"Data: {json.dumps(res.data, ensure_ascii=False)}"
                    )
                return "관련된 과거 유사 사례를 찾을 수 없습니다."

        # =====================================================================
        # 2) 월간 총 생산량 브리핑 (두 달 이상)
        # =====================================================================
        found_months = re.findall(r"(\d{1,2})월", user_input)
        found_months = sorted(list(set([int(m) for m in found_months])))

        if len(found_months) >= 2 and product_key is None:
            target_ver = extract_version(user_input)
            res = supabase.table("monthly_production") \
                .select("월, 총_생산량") \
                .in_("월", found_months) \
                .eq("버전", target_ver) \
                .execute()
            if res.data:
                df = pd.DataFrame(res.data).sort_values(by="월")
                out = [f"[{target_ver} 월간 총 생산량 브리핑]"]
                prev_val, prev_month = None, None
                for _, row in df.iterrows():
                    m = row["월"]
                    val = row["총_생산량"]
                    msg = f"{m}월: {val:,}"
                    if prev_val is not None:
                        diff = val - prev_val
                        if diff > 0:
                            msg += f" (전월({prev_month}월) 대비 {diff:,} 증가)"
                        elif diff < 0:
                            msg += f" (전월({prev_month}월) 대비 {abs(diff):,} 감소)"
                        else:
                            msg += " (변동 없음)"
                    out.append(f"- {msg}")
                    prev_val, prev_month = val, m
                return "\n".join(out)
            return "요청하신 월의 데이터가 monthly_production 테이블에 없습니다."

        # =====================================================================
        # 2-1) [FIX] 단일 월 총 생산량 ("00월 총 생산량 알려줘")
        # =====================================================================
        if target_month and product_key is None and not target_date and _is_month_total_query(user_input):
            res = supabase.table("monthly_production") \
                .select("월, 총_생산량") \
                .eq("월", target_month) \
                .eq("버전", target_version) \
                .limit(1) \
                .execute()
            if res.data:
                row = res.data[0]
                return f"[{row['월']}월 {target_version} 총 생산량]: {int(row['총_생산량']):,}"
            return f"{target_month}월 {target_version} 총 생산량 데이터가 monthly_production 테이블에 없습니다."

        # =====================================================================
        # 3) 월 CAPA 조회
        # =====================================================================
        if target_month and (("capa" in user_input.lower()) or ("카파" in user_input)) \
           and "비교" not in user_input and "초과" not in user_input and not target_date:
            res = supabase.table("daily_capa") \
                .select("라인, capa") \
                .eq("월", target_month) \
                .eq("버전", target_version) \
                .execute()
            if res.data:
                df = pd.DataFrame(res.data)
                df["라인"] = df["라인"].apply(normalize_line_name)
                grouped = df.groupby("라인")["capa"].apply(list).to_dict()
                display = {}
                for line, capas in grouped.items():
                    uniq = sorted(list(set(capas)))
                    display[line] = uniq[0] if len(uniq) == 1 else uniq
                return f"[{target_month}월 {target_version} 라인별 CAPA 정보]: {display}"
            return f"{target_month}월 {target_version} CAPA 데이터가 없습니다."

        # =====================================================================
        # 4) CAPA 초과/비교 (월 단위)
        # =====================================================================
        if ("초과" in user_input and "월" in user_input) or ("비교" in user_input and "월" in user_input and product_key is None):
            res_capa = supabase.table("daily_capa").select("*").eq("월", target_month).eq("버전", "최종").execute()
            res_prod = supabase.table("daily_total_production").select("*").eq("월", target_month).eq("버전", "최종").execute()
            if not res_capa.data or not res_prod.data:
                return "데이터 조회 실패(월/버전 확인 필요)"

            capa_ref = {}
            for it in res_capa.data:
                capa_ref[normalize_line_name(it["라인"])] = it["capa"]

            over = []
            for row in res_prod.data:
                d = normalize_date(row["날짜"])
                line = normalize_line_name(row["라인"])
                qty = row["총_생산량"]
                limit = capa_ref.get(line, 0)
                if limit > 0 and qty > limit:
                    over.append(f"| {d} | {line} | {limit} | {qty} |")

            if "초과" in user_input:
                if over:
                    over.sort()
                    return "[CAPA 초과 리스트]\n" + "\n".join(over)
                return f"{target_month}월 실적을 검토했으나 CAPA 초과한 날이 없습니다."
            return f"{target_month}월 데이터 비교 완료"

        # =====================================================================
        # 5) 구분 합계(Fan/Motor/Flange)
        # =====================================================================
        gubun_keywords = ["fan","motor","flange","팬","모터","플랜지"]
        if target_month and any(k in user_input.lower() for k in gubun_keywords):
            if "fan" in user_input.lower() or "팬" in user_input:
                g = "Fan"
            elif "motor" in user_input.lower() or "모터" in user_input:
                g = "Motor"
            else:
                g = "Flange"

            res = supabase.table("production_data") \
                .select("생산량") \
                .eq("월", target_month) \
                .eq("버전", "최종") \
                .ilike("구분", f"%{g}%") \
                .execute()
            if res.data:
                total = sum([x["생산량"] for x in res.data])
                return f"[{target_month}월 {g} 총 생산량(최종)]: {total:,}"
            return f"{target_month}월 {g} 데이터가 없습니다."

        # =====================================================================
        # 6) 특정 일자 + 제품명 생산량 (0차 vs 최종 비교)
        # =====================================================================
        if target_date and product_key:
            if "비교" in user_input:
                res_v0 = supabase.table("production_data").select("*") \
                    .eq("납기일", target_date).eq("버전", "0차").ilike("품명", f"%{product_key}%").execute()
                res_final = supabase.table("production_data").select("*") \
                    .eq("생산일", target_date).eq("버전", "최종").ilike("품명", f"%{product_key}%").execute()

                v0_qty = sum([x.get("생산량", 0) for x in (res_v0.data or [])])
                final_qty = sum([x.get("생산량", 0) for x in (res_final.data or [])])

                return (
                    f"[비교 결과 ({target_date} {product_key})]\n"
                    f"- 0차(납기일 기준): {v0_qty:,}\n"
                    f"- 최종(생산일 기준): {final_qty:,}\n"
                )

            ver_col = "납기일" if target_version == "0차" else "생산일"
            res = supabase.table("production_data").select("*") \
                .eq("버전", target_version).eq(ver_col, target_date).ilike("품명", f"%{product_key}%").execute()
            if res.data:
                total = sum([x.get("생산량", 0) for x in res.data])
                return f"[제품 데이터 ({target_date} {product_key} / {target_version})]\n[총 생산량]: {total:,}\nData: {json.dumps(res.data, ensure_ascii=False)}"
            return f"[알림] {target_date}에 '{product_key}' {target_version} 데이터가 없습니다."

        # =====================================================================
        # 7) 일자별 총 생산량
        # =====================================================================
        if target_date and ("생산량" in user_input):
            res = supabase.table("daily_total_production") \
                .select("총_생산량").eq("날짜", target_date).eq("버전", target_version).execute()
            if res.data:
                total = sum([x["총_생산량"] for x in res.data])
                return f"[{target_date} {target_version} 총 생산량]: {total:,} (daily_total 합계)"

            ver_col = "납기일" if target_version == "0차" else "생산일"
            res_fallback = supabase.table("production_data").select("생산량") \
                .eq(ver_col, target_date).eq("버전", target_version).execute()
            if res_fallback.data:
                total = sum([x.get("생산량", 0) for x in res_fallback.data])
                return f"[{target_date} {target_version} 총 생산량]: {total:,} (item 합계)"
            return f"[{target_date}] 데이터가 없습니다."

        return "요청하신 조건에 맞는 데이터를 찾을 수 없습니다."

    except Exception as e:
        return f"레거시 DB 조회 오류: {str(e)}"


def query_gemini_legacy(user_input: str, context: str) -> str:
    if not GEMINI_API_KEY:
        return context

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}

    system_prompt = f"""
당신은 숙련된 생산계획 담당자입니다. 제공된 데이터(Context)를 기반으로 사용자의 질문에 답하세요.

[중요: CAPA 초과 답변 규칙]
Context에 '[CAPA 초과 리스트]'가 포함되어 있다면, 반드시 아래 형식의 마크다운 표(Table)로 출력하세요.

| 날짜 | 라인 | CAPA | 총 생산량 |
|---|---|---|---|
| ... | ... | ... | ... |

[중요: 생산량 증량 사례 답변 규칙]
Context에 [PRODUCTION_INCREASE CASE FOUND]가 있다면:
1. 답변 최상단에 "# 생산량 증량 사례 (품목간 우선순위 조정)" 제목을 적으세요.
2. 각 날짜별로 다음 형식의 표를 작성하세요:

**[날짜: YYYY-MM-DD]**

증가한 제품 (선순위):
| 제품명 | 생산량 |
|---|---|
| ... | ... |

감소한 제품 (후순위):
| 제품명 | 생산량 |
|---|---|
| ... | ... |

3. final_remark는 표시하지 마세요.
4. 여러 날짜의 사례가 있다면 각각 구분하여 표시하세요.

[중요: 이슈 코드 답변 규칙]
Context에 [CODE CASE FOUND]가 있다면:
1) 답변 최상단에 코드명과 제목을 # Heading 1로 적으세요.
2) 데이터(Data)를 바탕으로 표를 작성하세요: [날짜 | 품목명 | 계획(V0) | 실적(V2) | 차이(Gap)]

[일반 답변 규칙]
1) 숫자는 제공된 그대로 전달하세요.
2) 데이터가 없으면 없다고 하세요.

[Context Data]
{context}

[User Question]
{user_input}
"""
    data = {"contents": [{"parts": [{"text": system_prompt}]}]}
    try:
        r = requests.post(url, headers=headers, json=data, timeout=60)
        if r.status_code != 200:
            return context
        j = r.json()
        return j["candidates"][0]["content"]["parts"][0]["text"]
    except Exception:
        return context


def run_legacy(prompt: str) -> str:
    ctx = fetch_db_data_legacy(prompt)
    if ("오류" in ctx) or ("설정되지" in ctx) or ("찾을 수 없습니다" in ctx):
        return ctx
    return query_gemini_legacy(prompt, ctx)


# =============================================================================
# Hybrid
# =============================================================================

TODAY = None
CAPA_LIMITS = None

def initialize_globals(today: date, capa_limits: dict):
    global TODAY, CAPA_LIMITS
    TODAY = today
    CAPA_LIMITS = capa_limits

def hybrid_is_workday_in_db(plan_df, date_str):
    if plan_df.empty or 'is_workday' not in plan_df.columns:
        return False
    row = plan_df[plan_df['plan_date'] == date_str]
    if not row.empty:
        return bool(row.iloc[0].get('is_workday', False))
    return False

def get_workdays_from_db(plan_df, start_date_str, direction='future', days_count=10):
    if plan_df.empty or 'is_workday' not in plan_df.columns:
        return []
    db_dates = plan_df[['plan_date', 'is_workday']].drop_duplicates().sort_values('plan_date')

    if direction == 'future':
        available = db_dates[(db_dates['plan_date'] >= start_date_str) & (db_dates['is_workday'] == True)]
        return available['plan_date'].head(days_count).tolist()

    available = db_dates[(db_dates['plan_date'] < start_date_str) &
                         (db_dates['plan_date'] >= TODAY.strftime('%Y-%m-%d')) &
                         (db_dates['is_workday'] == True)]
    return available['plan_date'].tail(days_count).tolist()

def step1_list_current_stock(plan_df, target_date, target_line):
    current = plan_df[(plan_df['plan_date'] == target_date) & (plan_df['line'] == target_line)].copy()
    if current.empty:
        return None, "해당 날짜에 생산 계획이 없습니다."
    total = int(current['qty_1차'].sum())
    items = []
    for _, row in current.iterrows():
        q = int(row.get('qty_1차', 0))
        if q > 0:
            items.append({
                'name': str(row.get('product_name', '')),
                'qty_0차': int(row.get('qty_0차', 0)),
                'qty_1차': q,
                'plt': int(row.get('plt', 1)),
            })
    return {'date': target_date, 'line': target_line, 'total': total, 'items': items}, None

def step2_calculate_cumulative_slack(plan_df, stock_result):
    """
    return list with additional fields for reporting:
      - cumsum_target, cumsum_actual, future_slack
    """
    items_with_slack = []
    for item in stock_result['items']:
        p_name = item['name']
        p_series = plan_df[plan_df['product_name'] == p_name].sort_values('plan_date').copy()
        if p_series.empty:
            continue

        p_series['cumsum_0차'] = p_series['qty_0차'].cumsum()
        p_series['cumsum_1차'] = p_series['qty_1차'].cumsum()

        today_row = p_series[p_series['plan_date'] == stock_result['date']]
        if today_row.empty:
            continue
        today_row = today_row.iloc[0]

        cumsum_target = int(today_row.get('cumsum_0차', 0))
        cumsum_actual = int(today_row.get('cumsum_1차', 0))
        max_movable_cumsum = cumsum_actual - cumsum_target

        future_demand = int(p_series[p_series['plan_date'] > stock_result['date']]['qty_0차'].sum())
        future_prod = int(p_series[p_series['plan_date'] > stock_result['date']]['qty_1차'].sum())
        future_slack = int(future_prod - future_demand)

        if max_movable_cumsum > 0:
            max_movable = max_movable_cumsum
        else:
            if future_slack >= 0:
                max_movable = int(item['qty_1차'])
            else:
                max_movable = max(0, int(item['qty_1차']) + future_slack)

        due_dates = p_series[p_series['qty_0차'] > 0]['plan_date'].tolist()
        last_due = max(due_dates) if due_dates else "미확인"

        if last_due != "미확인":
            last_due_dt = datetime.strptime(last_due, '%Y-%m-%d').date()
            target_dt = datetime.strptime(stock_result['date'], '%Y-%m-%d').date()
            buffer_days = (last_due_dt - target_dt).days
        else:
            buffer_days = 999

        items_with_slack.append({
            'name': p_name,
            'qty_0차': int(item.get('qty_0차', 0)),
            'qty_1차': int(item['qty_1차']),
            'plt': int(item['plt']),
            'cumsum_target': int(cumsum_target),
            'cumsum_actual': int(cumsum_actual),
            'future_slack': int(future_slack),
            'max_movable': int(max_movable),
            'last_due': last_due,
            'buffer_days': int(buffer_days),
            'movable': int(max_movable) >= int(item['plt'])
        })
    return items_with_slack

def step3_analyze_destination_capacity(plan_df, target_date, target_line):
    future_workdays = get_workdays_from_db(plan_df, target_date, direction='future', days_count=10)
    capa_status = {}

    for line in ["조립1", "조립2", "조립3"]:
        if line != target_line:
            current = plan_df[(plan_df['plan_date'] == target_date) & (plan_df['line'] == line)]['qty_1차'].sum()
            remaining = CAPA_LIMITS[line] - current
            capa_status[f"{target_date}_{line}"] = {
                'date': target_date, 'line': line,
                'current': int(current),
                'remaining': int(remaining),
                'max': int(CAPA_LIMITS[line]),
                'usage_rate': (float(current) / float(CAPA_LIMITS[line]) * 100.0) if CAPA_LIMITS[line] else 0.0
            }

        if line == target_line:
            for d in future_workdays:
                current = plan_df[(plan_df['plan_date'] == d) & (plan_df['line'] == line)]['qty_1차'].sum()
                remaining = CAPA_LIMITS[line] - current
                capa_status[f"{d}_{line}"] = {
                    'date': d, 'line': line,
                    'current': int(current),
                    'remaining': int(remaining),
                    'max': int(CAPA_LIMITS[line]),
                    'usage_rate': (float(current) / float(CAPA_LIMITS[line]) * 100.0) if CAPA_LIMITS[line] else 0.0
                }
    return capa_status

def step4_prepare_constraint_info(items_with_slack, target_line):
    constraint_info = []
    for item in items_with_slack:
        if not item['movable']:
            continue
        is_t6 = "T6" in item['name'].upper()
        is_a2xx = "A2XX" in item['name'].upper()

        if is_t6:
            possible_lines = [l for l in ["조립1","조립2","조립3"] if l != target_line]
            constraint = "조립1,2,3 모두 가능"
            priority = "타라인 이송 우선"
        elif is_a2xx:
            possible_lines = [l for l in ["조립1","조립2"] if l != target_line]
            constraint = "조립1,2만 가능(조립3 금지)"
            priority = "조립2 이송 우선"
        else:
            possible_lines = []
            constraint = f"{target_line} 내 날짜 이동만 가능"
            priority = "동일라인 연기/선행"

        constraint_info.append({
            **item,
            'possible_lines': possible_lines,
            'is_t6': is_t6,
            'is_a2xx': is_a2xx,
            'constraint': constraint,
            'priority': priority,
        })
    return constraint_info

def _parse_target_percent(prompt: str) -> float | None:
    m = re.search(r"(\d+)\s*%", prompt)
    if m:
        return int(m.group(1)) / 100.0
    return None

def _parse_sample_qty(prompt: str) -> int | None:
    m = re.search(r"샘플\s*(\d+)", prompt)
    if m:
        return int(m.group(1))
    return None

def _parse_add_qty(prompt: str) -> int | None:
    m = re.search(r"추가\s*(\d+)", prompt)
    if m:
        return int(m.group(1))
    m = re.search(r"(\d+)\s*추가", prompt)
    if m:
        return int(m.group(1))
    return None

def step6_validate_moves_with_adjust(moves, constraint_info, capa_status, plan_df, target_line):
    valid = []
    violations = []

    def find_item(n): return next((x for x in constraint_info if x['name'] == n), None)

    for i, mv in enumerate(moves or [], 1):
        item_name = mv.get("item")
        qty = int(mv.get("qty", 0))
        to_loc = (mv.get("to") or "").strip()

        item = find_item(item_name)
        if not item:
            violations.append(f"❌[{i}] {item_name}: 이동 가능 목록에 없음")
            continue

        if qty <= 0:
            violations.append(f"❌[{i}] {item_name}: qty<=0")
            continue

        if qty > int(item["max_movable"]):
            violations.append(f"❌[{i}] {item_name}: 누적여유 초과({qty:,} > {item['max_movable']:,})")
            continue

        if qty % int(item["plt"]) != 0:
            violations.append(f"❌[{i}] {item_name}: PLT 단위 아님(qty%plt!=0)")
            continue

        if "_" not in to_loc:
            violations.append(f"❌[{i}] {item_name}: to 형식 오류(YYYY-MM-DD_라인)")
            continue

        to_date, to_line = to_loc.split("_", 1)
        to_date = to_date.strip()
        to_line = to_line.strip()

        if item["is_a2xx"] and to_line == "조립3":
            violations.append(f"❌[{i}] {item_name}: A2XX는 조립3 금지")
            continue

        if (not item["is_t6"]) and (not item["is_a2xx"]) and (to_line != target_line):
            violations.append(f"❌[{i}] {item_name}: 전용모델 타라인 금지")
            continue

        capa_key = f"{to_date}_{to_line}"
        if capa_key not in capa_status:
            violations.append(f"⚠️[{i}] {item_name}: 목적지 CAPA 정보 없음({capa_key})")
            continue

        if not hybrid_is_workday_in_db(plan_df, to_date):
            violations.append(f"❌[{i}] {item_name}: 목적지 {to_date} 휴무일")
            continue

        remaining = int(capa_status[capa_key]["remaining"])
        plt = int(item["plt"])

        if qty > remaining:
            adj_plts = remaining // plt
            adj_qty = adj_plts * plt
            if adj_qty >= plt:
                mv = dict(mv)
                mv["original_qty"] = qty
                mv["qty"] = int(adj_qty)
                mv["adjusted"] = True
                violations.append(f"✅[{i}] {item_name}: CAPA 부족으로 자동 조정({qty:,} → {adj_qty:,})")
                qty = int(adj_qty)
            else:
                violations.append(f"❌[{i}] {item_name}: 목적지 CAPA 부족 및 조정불가(요청 {qty:,}, 잔여 {remaining:,})")
                continue
        else:
            mv = dict(mv)
            mv["adjusted"] = False

        capa_status[capa_key]["remaining"] = int(capa_status[capa_key]["remaining"]) - int(qty)
        valid.append(mv)

    return valid, violations

@st.cache_data(ttl=600)
def fetch_data_hybrid(target_date: str):
    if supabase is None:
        return pd.DataFrame(), pd.DataFrame()

    dt = datetime.strptime(target_date, "%Y-%m-%d")
    start = (dt - timedelta(days=10)).strftime("%Y-%m-%d")
    end = (dt + timedelta(days=10)).strftime("%Y-%m-%d")

    plan_res = supabase.table(HYBRID_PLAN_TABLE).select("*").gte("plan_date", start).lte("plan_date", end).execute()
    plan_df = pd.DataFrame(plan_res.data)

    hist_df = pd.DataFrame()
    try:
        hist_res = supabase.table(HYBRID_HIST_TABLE).select("*").execute()
        hist_df = pd.DataFrame(hist_res.data)
    except Exception:
        pass

    return plan_df, hist_df

def _pick_target_line(prompt: str, plan_df: pd.DataFrame, target_date: str) -> str | None:
    for ln in ["조립1","조립2","조립3"]:
        if ln in prompt:
            return ln

    date_rows = plan_df[plan_df["plan_date"] == target_date]
    if date_rows.empty:
        return None

    up = prompt.upper()
    if "T6" in up:
        lines = date_rows[date_rows["product_name"].str.contains("T6", case=False, na=False)]["line"].unique()
        return lines[0] if len(lines) else None
    if "A2XX" in up:
        lines = date_rows[date_rows["product_name"].str.contains("A2XX", case=False, na=False)]["line"].unique()
        return lines[0] if len(lines) else None

    grp = date_rows.groupby("line")["qty_1차"].sum()
    if grp.empty:
        return None
    return grp.idxmax()

def _round_target(qty: int, unit: int) -> int:
    if unit <= 0:
        return qty
    return int(round(qty / unit) * unit)

def _ai_build_moves(prompt, target_date, target_line, need_qty, constraint_info, capa_status, from_loc):
    """
    감축 전용 AI 플래너(예전 수사 흐름에 맞춰 감축 이동만 생성)
    """
    if not (genai and GEMINI_API_KEY):
        return []

    fact = {
        "mode": "reduce",
        "target": {"date": target_date, "line": target_line, "need_reduce_qty": int(need_qty)},
        "capa_remaining": {k: int(v["remaining"]) for k, v in capa_status.items()},
        "items": [{
            "name": x["name"],
            "plt": int(x["plt"]),
            "max_movable": int(x["max_movable"]),
            "is_t6": bool(x["is_t6"]),
            "is_a2xx": bool(x["is_a2xx"]),
            "constraint": x["constraint"],
            "priority": x["priority"],
        } for x in constraint_info[:80]],
        "from_loc": from_loc,
    }

    ai_prompt = f"""
아래 FACT_JSON과 규칙을 기반으로 이동 계획을 JSON으로만 출력하라.

규칙:
1) qty는 plt의 정수배
2) A2XX는 조립3 금지
3) 전용모델은 타라인 금지(가능하면 동일라인 날짜이동만)
4) 목적지 remaining 초과 금지 (remaining 내에서 PLT 단위로 조정해 제안 가능)
5) 출력은 JSON만

출력 형식:
{{
  "moves":[
    {{"item":"품목","qty":정수,"from":"YYYY-MM-DD_조립X","to":"YYYY-MM-DD_조립Y","reason":"..." }}
  ]
}}

FACT_JSON:
{json.dumps(fact, ensure_ascii=False)}
"""
    try:
        model = genai.GenerativeModel("gemini-2.0-flash-exp")
        resp = model.generate_content(ai_prompt)
        raw = (resp.text or "").strip()
        raw = re.sub(r"```json\s*|\s*```", "", raw)
        s = raw.find("{")
        e = raw.rfind("}") + 1
        data = json.loads(raw[s:e])
        return data.get("moves", []) or []
    except Exception:
        return []

def _fallback_reduce(constraint_info, capa_status, from_loc, target_line, need_reduce_qty):
    moves = []
    remaining_need = int(need_reduce_qty)
    from_date, _ = from_loc.split("_", 1)

    def best_dest(keys):
        candidates = []
        for k in keys:
            if k in capa_status and int(capa_status[k]["remaining"]) > 0:
                candidates.append((int(capa_status[k]["remaining"]), k))
        candidates.sort(reverse=True)
        return [k for _, k in candidates]

    same_day_other = [k for k in best_dest([f"{from_date}_조립3", f"{from_date}_조립2", f"{from_date}_조립1"])
                      if not k.endswith(f"_{target_line}")]

    future_same_line = best_dest([k for k in capa_status.keys() if k.endswith(f"_{target_line}") and not k.startswith(from_date)])

    items_sorted = sorted(constraint_info, key=lambda x: (int(x["buffer_days"]), int(x["max_movable"])), reverse=True)

    for it in items_sorted:
        if remaining_need <= 0:
            break

        plt = int(it["plt"])
        movable = min(int(it["max_movable"]), int(it["qty_1차"]))
        movable = (movable // plt) * plt
        if movable <= 0:
            continue

        qty_to_move = min(movable, remaining_need)
        qty_to_move = (qty_to_move // plt) * plt
        if qty_to_move <= 0:
            continue

        if it["is_t6"]:
            dests = same_day_other
        elif it["is_a2xx"]:
            dests = [d for d in same_day_other if d.endswith("_조립2") or d.endswith("_조립1")]
        else:
            dests = future_same_line

        for dest in dests:
            if remaining_need <= 0 or qty_to_move <= 0:
                break
            cap = int(capa_status[dest]["remaining"])
            if cap < plt:
                continue
            move_qty = min(qty_to_move, cap)
            move_qty = (move_qty // plt) * plt
            if move_qty <= 0:
                continue

            moves.append({
                "item": it["name"],
                "qty": int(move_qty),
                "from": from_loc,
                "to": dest,
                "reason": "폴백: 제약/PLT/CAPA 기반 감축"
            })
            capa_status[dest]["remaining"] = int(capa_status[dest]["remaining"]) - int(move_qty)
            remaining_need -= int(move_qty)
            qty_to_move -= int(move_qty)

    return moves

def _badge_by_remaining(remaining: int, max_capa: int) -> str:
    if max_capa <= 0:
        return "⚠️"
    usage = 100.0 * (max_capa - remaining) / max_capa
    if remaining <= 0 or usage >= 100:
        return "❌"
    if usage >= 90:
        return "⚠️"
    return "✅"

def _render_hybrid_investigation_report(
    user_prompt: str,
    today_str: str,
    target_date: str,
    target_line: str,
    stock: dict,
    target_qty: int,
    need_reduce_qty: int,
    sample_qty: int | None,
    constraint_info: list,
    capa_status: dict,
    ai_used: bool,
    ai_fail_reason: str | None,
    valid_moves: list,
    violations: list,
):
    moved = sum(int(x["qty"]) for x in (valid_moves or []))
    current_total = int(stock["total"])
    final_wo_sample = current_total - moved
    final_with_sample = final_wo_sample + (int(sample_qty) if sample_qty else 0)

    lines = []
    lines.append(user_prompt.strip())
    lines.append("")
    lines.append("✅ [OK] 하이브리드 수사 완료")
    lines.append("")
    lines.append(f"📊 {target_date} {target_line} 하이브리드 수사 보고서")
    lines.append("🔍 수사 방식")
    lines.append(f"전략 수립: {'AI 하이브리드 전략 (Gemini 2.0 Flash)' if ai_used else '폴백 전략 (룰 기반)'}")
    lines.append("검증 엔진: Python 6단계 검증 ✅")
    lines.append(f"분석 기준일: {today_str}")
    lines.append("")

    # 1단계 현황
    lines.append("📋 [1단계] 현황 파악")
    lines.append("기본 정보")
    lines.append(f"대상: {target_date} / {target_line}")
    lines.append(f"현재 생산량: {current_total:,}개")
    util_pct = (target_qty / CAPA_LIMITS[target_line] * 100.0) if CAPA_LIMITS.get(target_line) else 0.0
    lines.append(f"목표 생산량: {target_qty:,}개 ({util_pct:.0f}% CAPA)")
    if sample_qty:
        lines.append(f"샘플 추가: {int(sample_qty):,}개")
    lines.append(f"필요 감축량: {need_reduce_qty:,}개")
    lines.append("")

    items = stock.get("items", []) or []
    lines.append(f"품목 목록 ({len(items)}개)")
    for it in items[:20]:
        unit = int(it["qty_1차"]) // int(it["plt"]) if int(it["plt"]) else 0
        lines.append(f"- {it['name']}: {int(it['qty_1차']):,}개 ({int(it['plt'])}PLT, 단위: {unit:,}개/PLT)")

    # 2단계 누적 납기 여유
    lines.append("")
    lines.append("🔍 [2단계] 누적 납기 여유 분석")
    movable = [x for x in constraint_info if x.get("movable")]
    lines.append(f"✅ 이동 가능 품목 ({len(movable)}개)")
    for idx, x in enumerate(movable[:10], 1):
        lines.append(f"{idx}. {x['name']}")
        lines.append(f"- 계획 수량: {int(x['qty_1차']):,}개")
        lines.append(f"- 누적 납기: {int(x['cumsum_target']):,}개")
        lines.append(f"- 누적 생산: {int(x['cumsum_actual']):,}개")
        lines.append(f"- 이동 가능 여유: {int(x['max_movable']):,}개 ✅")
        lines.append(f"- 최종 납기: {x['last_due']} (여유: {int(x['buffer_days'])}일)")

    # 3단계 CAPA 현황
    lines.append("")
    lines.append("🎯 [3단계] 목적지 CAPA 현황")
    lines.append("타라인 이송 가능 여부")
    for ln in ["조립1", "조립2", "조립3"]:
        if ln == target_line:
            continue
        k = f"{target_date}_{ln}"
        if k in capa_status:
            rem = int(capa_status[k]["remaining"])
            mx = int(capa_status[k]["max"])
            badge = _badge_by_remaining(rem, mx)
            lines.append(f"{badge} {ln}: 잔여 {rem:,}개 / {mx:,}개 (가동률: {capa_status[k]['usage_rate']:.1f}%)")

    lines.append("")
    lines.append("동일라인 연기 가능 날짜")
    future_keys = [k for k in capa_status.keys() if k.endswith(f"_{target_line}") and not k.startswith(target_date)]
    future_keys_sorted = sorted(future_keys)[:10]
    for k in future_keys_sorted:
        rem = int(capa_status[k]["remaining"])
        mx = int(capa_status[k]["max"])
        badge = _badge_by_remaining(rem, mx)
        lines.append(f"{badge} {capa_status[k]['date']}: 잔여 {rem:,}개 (가동률: {capa_status[k]['usage_rate']:.1f}%)")

    # 4단계 물리 제약
    lines.append("")
    lines.append("🔒 [4단계] 물리 제약 정보")
    lines.append("제약 조건 요약")
    lines.append("T6 모델: 조립1,2,3 가능 (타라인 이송 가능)")
    lines.append("A2XX 모델: 조립1,2만 가능 (조립3 금지)")
    lines.append("전용 모델: 동일 라인 내 날짜 이동만 가능")
    lines.append("")
    lines.append("이동 가능 품목 제약 현황")
    for x in movable[:10]:
        lines.append(f"- {x['name']}: {x['constraint']} → {x['priority']}")

    # 5단계 전략
    lines.append("")
    lines.append("🤖 [5단계] 전략 수립 결과")
    if ai_used:
        lines.append("전략 개요: AI가 제약/PLT/CAPA를 고려해 감축 이동안을 생성했습니다.")
    else:
        lines.append("전략 개요: 룰 기반 폴백 로직으로 감축 이동안을 생성했습니다.")
        if ai_fail_reason:
            lines.append(f"AI 비사용 사유: {ai_fail_reason}")

    # 6단계 검증
    lines.append("")
    lines.append("✅ [6단계] Python 최종 검증")
    if valid_moves:
        lines.append("검증 결과: ✅ 승인 가능한 조치가 생성되었습니다.")
        lines.append("")
        lines.append(f"최종 승인된 조치 계획 ({len(valid_moves)}개)")
        for i, mv in enumerate(valid_moves, 1):
            adj = ""
            if mv.get("adjusted"):
                adj = f" (조정: {mv.get('original_qty', 0):,}→{mv.get('qty', 0):,})"
            lines.append(f"조치 {i}: {mv.get('item','')}")
            lines.append(f"- 이동량: {int(mv.get('qty',0)):,}개{adj}")
            lines.append(f"- 출발: {mv.get('from','')}")
            lines.append(f"- 도착: {mv.get('to','')}")
            lines.append(f"- 이유: {mv.get('reason','')}")
            lines.append("")
    else:
        lines.append("검증 결과: ❌ 승인된 이동 계획이 없습니다.")

    if violations:
        lines.append("검증 경고/실패")
        for v in violations[:30]:
            lines.append(f"- {v}")

    # 최종 결과
    lines.append("")
    lines.append("🎯 최종 결과")
    lines.append("항목\t수치")
    lines.append(f"현재 생산량\t{current_total:,}개")
    lines.append(f"목표 생산량\t{target_qty:,}개")
    lines.append(f"필요 감축량\t{need_reduce_qty:,}개")
    lines.append(f"실제 감축량\t{moved:,}개")
    lines.append(f"이동 후(샘플 제외)\t{final_wo_sample:,}개")
    if sample_qty:
        lines.append(f"샘플 포함 최종\t{final_with_sample:,}개")

    # 달성률: 감축 기준
    achieve = (moved / need_reduce_qty * 100.0) if need_reduce_qty else 0.0
    lines.append(f"목표 달성률\t{achieve:.1f}%")

    lines.append("")
    lines.append("📋 상세 데이터 보기")
    return "\n".join(lines)

def _render_hybrid_simple_report(target_date, target_line, stock_total, target_qty, need_reduce_qty, valid_moves, violations, ai_used, ai_failed_reason=None):
    moved = sum(int(x["qty"]) for x in (valid_moves or []))
    final_qty = stock_total - moved
    achieve = (moved / need_reduce_qty * 100.0) if need_reduce_qty else 0.0

    out = []
    out.append("[하이브리드 감축 결과]")
    out.append(f"- 대상: {target_date} {target_line}")
    out.append(f"- 현재: {stock_total:,}")
    out.append(f"- 목표: {target_qty:,}")
    out.append(f"- 필요 감축: {need_reduce_qty:,}")
    out.append(f"- 실제 감축: {moved:,} (달성률 {achieve:.1f}%)")
    out.append(f"- 최종: {final_qty:,}")
    out.append("")
    out.append(f"- 계획 생성: {'AI' if ai_used else '폴백'}" + (f" (AI 실패: {ai_failed_reason})" if ai_failed_reason else ""))

    if valid_moves:
        out.append("\n[승인된 이동 계획]")
        for i, mv in enumerate(valid_moves, 1):
            adj = ""
            if mv.get("adjusted"):
                adj = f" (조정: {mv.get('original_qty', 0):,}→{mv.get('qty',0):,})"
            out.append(f"{i}. {mv.get('item','')} | {int(mv.get('qty',0)):,}{adj} | {mv.get('from','')} → {mv.get('to','')} | {mv.get('reason','')}")
    else:
        out.append("\n승인된 이동 계획이 없습니다.")

    if violations:
        out.append("\n[검증 경고/실패]")
        out.extend([f"- {v}" for v in violations[:40]])

    return "\n".join(out)

def run_hybrid(prompt: str) -> str:
    if supabase is None:
        return "SUPABASE_URL/SUPABASE_KEY가 설정되지 않아 하이브리드 DB 조회를 할 수 없습니다. Streamlit Secrets를 확인하세요."

    target_date = _extract_date_any(prompt, default_year="2026")
    if not target_date:
        return "조정 요청으로 보이지만 날짜를 인식할 수 없습니다. 예: `1/21 T6 샘플 100개 추가` 또는 `2026-01-21 ...`"

    today = _get_hybrid_today()
    capa_limits = CAPA_LIMITS_DEFAULT if isinstance(CAPA_LIMITS_DEFAULT, dict) else {"조립1": 3300, "조립2": 3700, "조립3": 3600}
    initialize_globals(today, capa_limits)

    plan_df, _hist_df = fetch_data_hybrid(target_date)
    if plan_df.empty:
        return f"{target_date} 기준 생산계획 데이터를 불러오지 못했습니다(테이블/날짜 확인 필요: {HYBRID_PLAN_TABLE})."

    target_line = _pick_target_line(prompt, plan_df, target_date)
    if not target_line:
        return "대상 라인을 찾을 수 없습니다. `조립1/2/3` 또는 품목 힌트(T6/A2XX)를 포함해서 입력하세요."

    stock, err = step1_list_current_stock(plan_df, target_date, target_line)
    if err:
        return err

    items_slack = step2_calculate_cumulative_slack(plan_df, stock)
    constraint_info = step4_prepare_constraint_info(items_slack, target_line)
    capa_status = step3_analyze_destination_capacity(plan_df, target_date, target_line)

    # --- intent parsing ---
    pct = _parse_target_percent(prompt)
    if pct is None:
        pct = HYBRID_DEFAULT_TARGET_UTIL

    # 목표 생산량: 예전 리포트 느낌(예: 81% CAPA, 100단위 반올림)
    raw_target = int(CAPA_LIMITS[target_line] * float(pct))
    target_qty = _round_target(raw_target, HYBRID_TARGET_ROUNDING)

    sample_qty = _parse_sample_qty(prompt)  # "샘플 100"
    add_qty = _parse_add_qty(prompt)        # "추가 100" (샘플 없이도)

    # 시나리오:
    # - "샘플 N 추가" => 샘플이 들어오면 당일 계획을 감축/이송해서 목표 가동률(=target_qty) 이내로 맞추는 수사
    # - 그 외 % 기반 감축 수사
    if sample_qty is not None:
        # 샘플 포함 예상 총량이 목표 초과하면 그 초과분만큼 감축, 아니면 "샘플 추가해도 목표 이내" 안내
        expected_with_sample = int(stock["total"]) + int(sample_qty)
        need_reduce_qty = max(0, expected_with_sample - int(target_qty))
        if need_reduce_qty == 0:
            # 예전처럼 리포트 포맷은 유지하되, 조치 없음으로 안내
            if HYBRID_REPORT_STYLE == "investigation":
                return _render_hybrid_investigation_report(
                    user_prompt=prompt,
                    today_str=today.strftime("%Y-%m-%d"),
                    target_date=target_date,
                    target_line=target_line,
                    stock=stock,
                    target_qty=int(target_qty),
                    need_reduce_qty=0,
                    sample_qty=int(sample_qty),
                    constraint_info=constraint_info,
                    capa_status=capa_status,
                    ai_used=False,
                    ai_fail_reason="샘플 추가 후에도 목표 이하(감축 불필요)",
                    valid_moves=[],
                    violations=[],
                )
            return f"[현황]\n- 대상: {target_date} {target_line}\n- 현재: {stock['total']:,}\n- 샘플 추가: {int(sample_qty):,}\n- 목표: {target_qty:,}\n\n샘플 추가 후에도 목표 생산량 이내라 감축 조치가 필요 없습니다."

        # 감축 수사 수행
    else:
        # % 기반 감축: 현재가 목표 이하이면 감축 불필요
        need_reduce_qty = int(stock["total"] - target_qty)
        if need_reduce_qty <= 0:
            if HYBRID_REPORT_STYLE == "investigation":
                return _render_hybrid_investigation_report(
                    user_prompt=prompt,
                    today_str=today.strftime("%Y-%m-%d"),
                    target_date=target_date,
                    target_line=target_line,
                    stock=stock,
                    target_qty=int(target_qty),
                    need_reduce_qty=0,
                    sample_qty=None,
                    constraint_info=constraint_info,
                    capa_status=capa_status,
                    ai_used=False,
                    ai_fail_reason="현재 생산량이 목표 이하(감축 불필요)",
                    valid_moves=[],
                    violations=[],
                )
            return (
                f"[현황]\n- 대상: {target_date} {target_line}\n"
                f"- 현재: {stock['total']:,}\n- 목표: {target_qty:,}\n\n"
                f"현재 생산량이 목표 이하라 감축 조치가 필요 없습니다."
            )

    # locations (reduce)
    from_loc = f"{target_date}_{target_line}"

    # 1) AI plan (optional)
    ai_used = False
    ai_fail_reason = None
    moves = []

    if genai and GEMINI_API_KEY and constraint_info:
        ai_moves = _ai_build_moves(
            prompt=prompt,
            target_date=target_date,
            target_line=target_line,
            need_qty=int(abs(need_reduce_qty)),
            constraint_info=constraint_info,
            capa_status=capa_status,
            from_loc=from_loc,
        )
        if ai_moves:
            moves = ai_moves
            ai_used = True
        else:
            ai_fail_reason = "AI 결과 없음/파싱 실패"

    # 2) fallback
    if not moves:
        capa_copy = {k: dict(v) for k, v in capa_status.items()}
        moves = _fallback_reduce(constraint_info, capa_copy, from_loc, target_line, abs(need_reduce_qty))
        ai_used = False

    # 3) validate (with adjust)
    capa_for_validate = {k: dict(v) for k, v in capa_status.items()}
    valid_moves, violations = step6_validate_moves_with_adjust(
        moves=moves,
        constraint_info=constraint_info,
        capa_status=capa_for_validate,
        plan_df=plan_df,
        target_line=target_line
    )

    if HYBRID_REPORT_STYLE == "investigation":
        return _render_hybrid_investigation_report(
            user_prompt=prompt,
            today_str=today.strftime("%Y-%m-%d"),
            target_date=target_date,
            target_line=target_line,
            stock=stock,
            target_qty=int(target_qty),
            need_reduce_qty=int(abs(need_reduce_qty)),
            sample_qty=int(sample_qty) if sample_qty is not None else None,
            constraint_info=constraint_info,
            capa_status=capa_status,
            ai_used=ai_used,
            ai_fail_reason=ai_fail_reason,
            valid_moves=valid_moves,
            violations=violations
        )

    return _render_hybrid_simple_report(
        target_date=target_date,
        target_line=target_line,
        stock_total=int(stock["total"]),
        target_qty=int(target_qty),
        need_reduce_qty=int(abs(need_reduce_qty)),
        valid_moves=valid_moves,
        violations=violations,
        ai_used=ai_used,
        ai_failed_reason=ai_fail_reason
    )


# =============================================================================
# Entry
# =============================================================================

def route_and_answer(prompt: str) -> tuple[str, dict]:
    route, meta = classify_route(prompt)
    if route == "hybrid":
        ans = run_hybrid(prompt)
    else:
        ans = run_legacy(prompt)

    debug = {"route": route, **meta}
    return ans, debug
