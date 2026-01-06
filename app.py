import streamlit as st
from engine import route_and_answer

st.set_page_config(page_title="생산계획 AI 챗봇", page_icon="🏭", layout="wide")
st.title("🏭 생산계획 AI 챗봇")

# (선택) 디버그 정보 표시 토글
show_debug = st.sidebar.checkbox("디버그(라우팅/날짜) 표시", value=False)

if "messages" not in st.session_state:
    st.session_state.messages = []

# 기존 대화 출력
for m in st.session_state.messages:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])
        if show_debug and m.get("debug"):
            st.code(m["debug"], language="json")

# 입력
prompt = st.chat_input("예: '10월 CAPA 초과한 날?', '1/6 조립1 70%만 생산하고 싶어'")
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("분석 중..."):
            answer, debug = route_and_answer(prompt)
            st.markdown(answer)
            if show_debug:
                st.code(debug, language="json")

    st.session_state.messages.append({"role": "assistant", "content": answer, "debug": debug})
