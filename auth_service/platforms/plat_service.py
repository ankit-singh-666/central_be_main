from fastapi.responses import StreamingResponse
import googleapiclient.discovery
import google.oauth2.credentials
import io
import zipfile
from typing import List
from kafka_producer import send_message   # ✅ Import Kafka producer


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
    q = "trashed = false"

    if not imageFetch:
        q += " and not mimeType contains 'image/'"

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
    """
    Downloads files from Google Drive, zips them,
    and sends file IDs to Kafka for async processing.
    """
    credentials = google.oauth2.credentials.Credentials(
        token=credentials_dict["token"],
        refresh_token=credentials_dict.get("refresh_token"),
        token_uri=credentials_dict.get("token_uri"),
        client_id=credentials_dict.get("client_id"),
        client_secret=credentials_dict.get("client_secret"),
        scopes=credentials_dict.get("scopes")
    )

    drive_service = googleapiclient.discovery.build("drive", "v3", credentials=credentials)

    # Debug: Check authenticated user
    about = drive_service.about().get(fields="user").execute()
    print("Authenticated as:", about["user"]["emailAddress"])
    print("Using scopes:", credentials.scopes)

    # ✅ Send file IDs to Kafka
    for file_id in file_ids:
        await send_message(key="file_id", value={"file_id": file_id})
        print(f"📤 File ID {file_id} sent to Kafka")

    # Continue with zipping & returning response
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        for file_id in file_ids:
            try:
                meta = drive_service.files().get(
                    fileId=file_id,
                    fields="name,mimeType",
                    supportsAllDrives=True
                ).execute()

                filename = meta.get("name", f"{file_id}")
                mime_type = meta.get("mimeType")

                if mime_type.startswith("application/vnd.google-apps"):
                    export_mimetypes = {
                        "application/vnd.google-apps.document": "application/pdf",
                        "application/vnd.google-apps.spreadsheet": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "application/vnd.google-apps.presentation": "application/pdf",
                    }
                    export_mime = export_mimetypes.get(mime_type, "application/pdf")
                    request = drive_service.files().export_media(
                        fileId=file_id, mimeType=export_mime
                    )
                    file_data = request.execute()

                    if export_mime == "application/pdf":
                        filename += ".pdf"
                    elif export_mime.endswith(".spreadsheetml.sheet"):
                        filename += ".xlsx"
                    elif export_mime.endswith(".presentationml.presentation"):
                        filename += ".pptx"
                else:
                    request = drive_service.files().get_media(
                        fileId=file_id, supportsAllDrives=True
                    )
                    file_data = request.execute()

                zf.writestr(filename, file_data)

            except Exception as e:
                print(f"❌ Error downloading {file_id}: {e}")
                continue

    zip_buffer.seek(0)
    headers = {"Content-Disposition": 'attachment; filename="drive_files.zip"'}

    return StreamingResponse(zip_buffer, media_type="application/zip", headers=headers)
