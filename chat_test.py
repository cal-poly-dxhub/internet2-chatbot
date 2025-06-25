import requests
import yaml

config = yaml.safe_load(open("./config.yaml"))

API_URL = config["rag_api_endpoint"] + "chat-response"
API_KEY = config["api_key"]

headers = {
    "x-api-key": API_KEY,
}

question = input("Ask a question: ")
data = {"query": question}


def format_response(raw_text: str):
    # Decode escaped characters like \n and \"
    decoded_text = raw_text.encode().decode("unicode_escape").replace('"', "")
    return decoded_text


response = requests.post(API_URL, json=data, headers=headers)
print(response.status_code, format_response(response.text))
