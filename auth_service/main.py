import uvicorn
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from typing import Optional
import shutil
from pathlib import Path
import tempfile

import openai
import config
from multi_indexer import MultiSourceIndexer

# Import our auth/drive routers
from auth_service.auth import routes as auth_routes

#from drive import routes as drive_routes

# ===== OPENAI SETUP =====
OPENAI_CHAT_MODEL = config.OPENAI_MODELS["chat"]
openai.api_key = config.OPENAI_API_KEY

# ===== APP INIT =====
app = FastAPI(title="MultiSourceIndexer API")

# Session middleware (needed for Google OAuth)
app.add_middleware(SessionMiddleware, secret_key=config.SECRET_KEY)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.FRONTEND_URL],  # set to your frontend's origin (e.g. "http://localhost:3000")
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register auth + drive routes
app.include_router(auth_routes.router)
#app.include_router(drive_routes.router)

# ===== GLOBAL INDEXER =====
indexer = MultiSourceIndexer()

# ===== API ROUTES =====

@app.post("/build/file")
async def build_from_file(file: UploadFile = File(...), collection_name: str = Form("all_files")):
    temp_path = Path(tempfile.gettempdir()) / file.filename
    with open(temp_path, "wb") as f:
        shutil.copyfileobj(file.file, f)
    try:
        indexer.build_index_from_file(str(temp_path), collection_name=collection_name)
        return {"status": "success", "message": f"Index built from file {file.filename}"}
    finally:
        temp_path.unlink(missing_ok=True)


@app.post("/build/folder")
async def build_from_folder(folder_path: str = Form(...), collection_name: str = Form("all_files")):
    try:
        indexer.build_index_from_folder(folder_path, collection_name=collection_name)
        return {"status": "success", "message": f"Index built from folder {folder_path}"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/build/drive")
async def build_from_drive(drive_link: str = Form(...), collection_name: str = Form("all_files")):
    try:
        indexer.build_index_from_drive(drive_link, collection_name=collection_name)
        return {"status": "success", "message": f"Index built from Drive folder {drive_link}"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/build/zip")
async def build_from_zip(zip_file: UploadFile = File(...), collection_name: str = Form("zip_files")):
    temp_path = Path(tempfile.gettempdir()) / zip_file.filename
    with open(temp_path, "wb") as f:
        shutil.copyfileobj(zip_file.file, f)
    try:
        indexer.build_index_from_zip(str(temp_path), collection_name=collection_name)
        return {"status": "success", "message": f"Index built from ZIP {zip_file.filename}"}
    finally:
        temp_path.unlink(missing_ok=True)


@app.get("/query")
async def query_index(
    query_text: str,
    k: int = 3,
    source: Optional[str] = None,
    file_type: Optional[str] = None
):
    try:
        context = indexer.query(query_text, k=k, source=source, file_type=file_type)

        print("<=============================>")
        print("The context is ============> " , context)
        print("<=============================>")

        # Build a prompt for OpenAI
        user_prompt = f"""
        You are given the following context from indexed documents:

        {context}

        Now answer the following query based only on the context above:
        {query_text}
        """

        # Call OpenAI
        response = openai.chat.completions.create(
            model=OPENAI_CHAT_MODEL,
            messages=[
                {"role": "system", "content": "You are a helpful assistant that answers based only on provided context."},
                {"role": "user", "content": user_prompt}
            ]
        )

        return {
            "status": "success",
            "openai_answer": response
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/hello")
async def hello():
    return {"message": "Hello, welcome to the MultiSourceIndexer API with Google Drive integration!"}


# ===== RUN APP =====
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
