from fastapi import Request
from fastapi.responses import RedirectResponse
import google_auth_oauthlib.flow
import config
import os
def login(request: Request):
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        config.GOOGLE_CLIENT_SECRET,
        scopes=config.SCOPES
    )
    flow.redirect_uri = config.REDIRECT_URI

    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true"
    )
    request.session["state"] = state
    return RedirectResponse(authorization_url)

def callback(request: Request):
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'  # if local dev only!

    state = request.session.get("state")
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        config.GOOGLE_CLIENT_SECRET,
        scopes=config.SCOPES,
        state=state
    )
    flow.redirect_uri = config.REDIRECT_URI

    flow.fetch_token(authorization_response=str(request.url))

    credentials = flow.credentials
    request.session["credentials"] = {
        "token": credentials.token,
        "refresh_token": credentials.refresh_token,
        "token_uri": credentials.token_uri,
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "scopes": credentials.scopes
    }

    # Redirect to frontend home or dashboard (no token in URL)
    return RedirectResponse(config.FRONTEND_URL)