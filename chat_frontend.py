import requests
import streamlit as st
import yaml

config = yaml.safe_load(open("./config.yaml"))

API_URL = config["rag_api_endpoint"] + "chat-response"
API_KEY = config["api_key"]


def display_response(raw_text: str):
    # Decode escaped characters like \n and \"
    decoded_text = raw_text.encode().decode("unicode_escape").replace('"', "")
    st.markdown(decoded_text)


# Streamlit App Setup
st.set_page_config(page_title="Serveless Rag Chatbot PoC", page_icon="💬")
st.title("RAG Serverless Framework")

# Session state to hold conversation history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display the chat messages
for msg in st.session_state.messages:
    role = "You" if msg["role"] == "user" else "Bot"
    with st.chat_message(msg["role"]):
        display_response(msg["content"])


# User input field
user_input = st.chat_input("Type your question here...")

if user_input:
    # Add user message to history
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        display_response(user_input)

    # Send to API
    headers = {"x-api-key": API_KEY}
    data = {"query": user_input}
    try:
        response = requests.post(API_URL, json=data, headers=headers)
        response.raise_for_status()
        bot_reply = response.text
    except Exception as e:
        bot_reply = f"Error: {e}"

    # Add bot response to history
    st.session_state.messages.append({"role": "assistant", "content": bot_reply})
    with st.chat_message("assistant"):
        display_response(bot_reply)
