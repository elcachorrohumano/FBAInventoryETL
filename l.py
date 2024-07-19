import os
import csv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

SPREADSHEET_ID = '1hdElXUaJPAXHTvUEmdLqKc_kScezkTx1t_eVjOgr0Gc'
TOKEN_PATH = 'keys/google_token.json'
CREDENTIALS_PATH = 'keys/google_credentials.json'

def load_df(sheet: str, df: pd.DataFrame):
    credentials = None
    if os.path.exists(TOKEN_PATH):
        credentials = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_PATH, SCOPES)
            credentials = flow.run_local_server(port=0)
        with open(TOKEN_PATH, 'w') as token:
            token.write(credentials.to_json())    
    try:
        service = build("sheets", "v4", credentials=credentials)

        df = df.fillna('')
        values = [df.columns.to_list()] + df.values.tolist()
        body = {"values": values}

        range = f'{sheet}!A2'
        
        result = (
            service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=SPREADSHEET_ID,
                    valueInputOption='USER_ENTERED',
                    range=range,
                    body=body,
                )
                .execute()
        )

        print(f"{result.get('updatedCells')} cells updated on sheet {sheet}.")

        return result
    


    except HttpError as error:
        print(f"An error occurred: {error}")
        return error