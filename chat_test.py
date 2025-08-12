import json
import requests
import uuid
import yaml

config = yaml.safe_load(open("./config.yaml"))

API_URL = config["rag_api_endpoint"] + "chat-response"
API_KEY = config["api_key"]

headers = {
    "x-api-key": API_KEY,
}

session_id = str(uuid.uuid4())

def format_response(raw_text: str):
    # Decode escaped characters like \n and \"
    decoded_text = raw_text.encode().decode("unicode_escape").replace('"', "")
    return decoded_text

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
        print(f"\nBot: {format_response(bot_reply)}")
    except Exception as e:
        print(f"Error: {e}")
