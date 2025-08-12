import json
import requests
import streamlit as st
import uuid
import yaml

config = yaml.safe_load(open("./config.yaml"))

API_URL = config["rag_api_endpoint"] + "chat-response"
FEEDBACK_URL = config["rag_api_endpoint"] + "feedback"
API_KEY = config["api_key"]


def display_response(raw_text: str):
    # Decode escaped characters like \n and \"
    decoded_text = raw_text.encode().decode("unicode_escape").replace('"', "")
    st.markdown(decoded_text)


def send_feedback_callback(timestamp: int, rating: str, feedback_text: str = ""):
    """Callback function for feedback buttons."""
    if send_feedback(st.session_state.session_id, timestamp, rating, feedback_text):
        if rating in ["thumbs_up", "thumbs_down"]:
            thumb_key = f"{timestamp}_thumb"
            st.session_state.feedback_sent.add(thumb_key)
            # Store which thumb was pressed
            rating_key = f"{timestamp}_rating"
            st.session_state[rating_key] = rating
        else:
            text_key = f"{timestamp}_text"
            st.session_state.feedback_sent.add(text_key)
        st.success("Thanks for your feedback!")


def send_feedback(session_id: str, timestamp: int, rating: str, feedback_text: str = ""):
    """Send feedback to the API."""
    headers = {"x-api-key": API_KEY}
    data = {
        "session_id": session_id,
        "timestamp": timestamp,
        "rating": rating,
        "feedback_text": feedback_text
    }
    try:
        response = requests.post(FEEDBACK_URL, json=data, headers=headers)
        response.raise_for_status()
        return True
    except Exception as e:
        st.error(f"Error sending feedback: {e}")
        return False


# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "feedback_sent" not in st.session_state:
    st.session_state.feedback_sent = set()

# Streamlit App Setup
st.set_page_config(page_title="Internet2 Chatbot PoC", page_icon="💬 ")
st.title("Internet2 Chatbot PoC")

with st.sidebar:
    st.markdown("""
    *Some questions you can ask me*
    - What workloads can I run on AWS?
    - What workloads can I run on GCP?
    - What did Lee Pang say about Amazon Omics?
    - What is AWS Omics?
    - How do I convince my leadership of the importance of FinOps practices?
    - Who has a Cloud Center of Excellence?
    - How are people doing account provisioning?
    - I've got a consultant coming in to install Control Tower for us, but they don't have any higher ed experience. What questions should I be asking to make sure I don't have to redo the work later?
    - Do I have to set up a cloud networking architecture for each platform or is there a single strategy to rule them all?
    """)
    
    # Add session reset button
    if st.button("New Conversation"):
        st.session_state.messages = []
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.feedback_sent = set()
        st.rerun()


# Display the chat messages
for i, msg in enumerate(st.session_state.messages):
    role = "You" if msg["role"] == "user" else "Bot"
    with st.chat_message(msg["role"]):
        display_response(msg["content"])
        
        # Add feedback buttons for assistant messages
        if msg["role"] == "assistant" and "timestamp" in msg:
            timestamp = msg["timestamp"]
            thumb_key = f"{timestamp}_thumb"
            text_key = f"{timestamp}_text"
            rating_key = f"{timestamp}_rating"
            
            col1, col2, col3 = st.columns([1, 1, 8])
            
            with col1:
                if thumb_key in st.session_state.feedback_sent:
                    if st.session_state.get(rating_key) == "thumbs_up":
                        st.markdown('<div style="background-color: #90EE90; padding: 5px; border-radius: 5px; text-align: center;">👍</div>', unsafe_allow_html=True)
                    else:
                        st.text("👍")
                else:
                    if st.button("👍", key=f"up_{i}", on_click=lambda t=timestamp: send_feedback_callback(t, "thumbs_up")):
                        pass
            
            with col2:
                if thumb_key in st.session_state.feedback_sent:
                    if st.session_state.get(rating_key) == "thumbs_down":
                        st.markdown('<div style="background-color: #FFB6C1; padding: 5px; border-radius: 5px; text-align: center;">👎</div>', unsafe_allow_html=True)
                    else:
                        st.text("👎")
                else:
                    if st.button("👎", key=f"down_{i}", on_click=lambda t=timestamp: send_feedback_callback(t, "thumbs_down")):
                        pass
            
            # Feedback text input
            if text_key not in st.session_state.feedback_sent:
                feedback_text = st.text_input("Additional feedback (optional):", key=f"feedback_{i}")
                if st.button("Submit Feedback", key=f"submit_{i}") and feedback_text:
                    send_feedback_callback(timestamp, "text_feedback", feedback_text)
            else:
                st.text("✓ Text feedback submitted")


# User input field
user_input = st.chat_input("Type your question here...")

if user_input:
    # Add user message to history
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        display_response(user_input)

    # Send to API with session ID
    headers = {"x-api-key": API_KEY}
    data = {
        "query": user_input,
        "session_id": st.session_state.session_id
    }
    try:
        response = requests.post(API_URL, json=data, headers=headers)
        response.raise_for_status()
        response_data = json.loads(response.text)
        bot_reply = response_data.get("response", response.text)
        timestamp = response_data.get("timestamp")
        # Update session ID if provided
        if "session_id" in response_data:
            st.session_state.session_id = response_data["session_id"]
    except Exception as e:
        bot_reply = f"Error: {e}"
        timestamp = None

    # Add bot response to history with timestamp
    message_data = {"role": "assistant", "content": bot_reply}
    if timestamp:
        message_data["timestamp"] = timestamp
    
    st.session_state.messages.append(message_data)
    with st.chat_message("assistant"):
        display_response(bot_reply)
        
        # Add feedback buttons for the new response
        if timestamp:
            col1, col2, col3 = st.columns([1, 1, 8])
            
            with col1:
                if st.button("👍", key=f"up_new", on_click=lambda: send_feedback_callback(timestamp, "thumbs_up")):
                    pass
            
            with col2:
                if st.button("👎", key=f"down_new", on_click=lambda: send_feedback_callback(timestamp, "thumbs_down")):
                    pass
            
            # Feedback text input
            feedback_text = st.text_input("Additional feedback (optional):", key=f"feedback_new")
            if st.button("Submit Feedback", key=f"submit_new") and feedback_text:
                send_feedback_callback(timestamp, "text_feedback", feedback_text)
