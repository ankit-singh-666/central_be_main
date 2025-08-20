from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List
from . import plat_service

router = APIRouter(prefix="/platform", tags=["platform"])


@router.get("/files")
def list_drive_files(request: Request):
    creds = request.session.get("credentials")
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return plat_service.list_drive_files(creds)


class FileDownloadRequest(BaseModel):
    fileIds: List[str]


@router.post("/files/download")
async def download_files(request: Request, files_request: FileDownloadRequest):
    creds = request.session.get("credentials")
    if not creds:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return await plat_service.download_files(creds, files_request.fileIds)
