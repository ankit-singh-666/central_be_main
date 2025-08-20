from fastapi import APIRouter, Request,HTTPException
from . import service

router = APIRouter(prefix="/auth", tags=["auth"])

@router.get("/login")
def login(request: Request):
    return service.login(request)

@router.get("/callback")
def callback(request: Request):
    return service.callback(request)

@router.get("/me")
def me(request: Request):
    creds = request.session.get("credentials")
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated")
    # Optionally fetch user info from Google here or return token info
    return {"token": creds["token"]}
