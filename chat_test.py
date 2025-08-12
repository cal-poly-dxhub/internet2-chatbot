import json
import requests
import uuid
import yaml

config = yaml.safe_load(open("./config.yaml"))

API_URL = config["rag_api_endpoint"] + "chat-response"
FEEDBACK_URL = config["rag_api_endpoint"] + "feedback"
API_KEY = config["api_key"]

headers = {
    "x-api-key": API_KEY,
}

session_id = str(uuid.uuid4())

def format_response(raw_text: str):
    # Decode escaped characters like \n and \"
    decoded_text = raw_text.encode('utf-8').decode('utf-8').replace('"', "")
    return decoded_text

def send_feedback(session_id: str, timestamp: int, rating: str, feedback_text: str = ""):
    """Send feedback to the API."""
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
        print(f"Error sending feedback: {e}")
        return False

print("Internet2 Chatbot - Type 'quit' to exit")
print(f"Session ID: {session_id}")
print("-" * 50)

while True:
    question = input("\nAsk a question: ")
    if question.lower() == 'quit':
        break
    
    data = {
        "query": question,
        "session_id": session_id
    }
    
    try:
        response = requests.post(API_URL, json=data, headers=headers)
        response.raise_for_status()
        response_data = json.loads(response.text)
        bot_reply = response_data.get("response", response.text)
        timestamp = response_data.get("timestamp")
        print(f"\nBot: {format_response(bot_reply)}")
        
        # Ask for feedback
        if timestamp:
            feedback = input("\nRate this response (u=👍, d=👎, enter=skip): ").lower()
            if feedback == 'u':
                if send_feedback(session_id, timestamp, "thumbs_up"):
                    print("Thanks for your feedback!")
            elif feedback == 'd':
                if send_feedback(session_id, timestamp, "thumbs_down"):
                    print("Thanks for your feedback!")
                # Ask for additional feedback on thumbs down
                text_feedback = input("Any additional feedback? (optional): ")
                if text_feedback:
                    send_feedback(session_id, timestamp, "text_feedback", text_feedback)
                    
    except Exception as e:
        print(f"Error: {e}")
