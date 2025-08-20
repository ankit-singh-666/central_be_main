from fastapi.responses import StreamingResponse
import google_auth_oauthlib.flow
import googleapiclient.discovery
import google.oauth2.credentials
import config
import os
import io
import zipfile
from typing import List


def list_drive_files(credentials_dict, imageFetch=False):
    # Rebuild Credentials object
    credentials = google.oauth2.credentials.Credentials(
        token=credentials_dict["token"],
        refresh_token=credentials_dict.get("refresh_token"),
        token_uri=credentials_dict.get("token_uri"),
        client_id=credentials_dict.get("client_id"),
        client_secret=credentials_dict.get("client_secret"),
        scopes=credentials_dict.get("scopes")
    )

    drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)

    files = []
    page_token = None

    # Base query: not trashed
    q = "trashed = false"

    # Exclude images if imageFetch is False
    if not imageFetch:
        # Exclude common image mime types, add others as needed
        exclude_images_query = " and not mimeType contains 'image/'"
        q += exclude_images_query

    try:
        while True:
            response = drive_service.files().list(
                q=q,
                spaces='drive',
                fields="nextPageToken, files(id, name, mimeType, parents)",
                pageToken=page_token
            ).execute()

            files.extend(response.get('files', []))
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
    except Exception as e:
        raise Exception(f"Failed to fetch drive files: {e}")

    return {"files": files}


async def download_files(credentials_dict, file_ids: List[str]):
    # Rebuild Credentials object
    credentials = google.oauth2.credentials.Credentials(
        token=credentials_dict["token"],
        refresh_token=credentials_dict.get("refresh_token"),
        token_uri=credentials_dict.get("token_uri"),
        client_id=credentials_dict.get("client_id"),
        client_secret=credentials_dict.get("client_secret"),
        scopes=credentials_dict.get("scopes")
    )

    drive_service = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        for file_id in file_ids:
            meta = drive_service.files().get(fileId=file_id, fields="name,mimeType").execute()
            filename = meta.get("name", f"{file_id}")
            mime_type = meta.get("mimeType")

            if mime_type.startswith("application/vnd.google-apps"):
                # Export Google Docs/Sheets/Slides to suitable format
                export_mimetypes = {
                    "application/vnd.google-apps.document": "application/pdf",
                    "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "application/vnd.google-apps.presentation": "application/pdf",
                }
                export_mime = export_mimetypes.get(mime_type, "application/pdf")
                request = drive_service.files().export_media(fileId=file_id, mimeType=export_mime)
                file_data = request.execute()

                if export_mime == "application/pdf":
                    filename += ".pdf"
                elif export_mime == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
                    filename += ".xlsx"
                elif export_mime == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
                    filename += ".pptx"

            else:
                request = drive_service.files().get_media(fileId=file_id)
                file_data = request.execute()

            zf.writestr(filename, file_data)

    zip_buffer.seek(0)

    headers = {
        "Content-Disposition": 'attachment; filename="drive_files.zip"'
    }

    return StreamingResponse(zip_buffer, media_type="application/zip", headers=headers)
