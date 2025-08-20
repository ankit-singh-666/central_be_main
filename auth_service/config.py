import os
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# 🔑 OpenAI API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# 📌 Available models
OPENAI_MODELS = {
    "chat": "gpt-4o-mini",         # for chat-based completions
    "embedding": "text-embedding-3-small",  # for embeddings
    "vision": "gpt-4o",            # if you want multimodal
}

#GOOGLE_CLIENT_SECRETS = "webclient.json"
SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
REDIRECT_URI = "http://localhost:8000/auth/callback"
FRONTEND_URL = "http://localhost:3000"
SECRET_KEY = "supersecret"   # session secret

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")

GOOGLE_CLIENT_SECRET = "./webclient.json"

