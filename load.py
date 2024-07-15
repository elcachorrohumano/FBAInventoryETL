import os
import csv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

SPREADSHEET_ID = '1hdElXUaJPAXHTvUEmdLqKc_kScezkTx1t_eVjOgr0Gc'

def main():
    credentials = None
    if os.path.exists('token.json'):
        credentials = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            credentials = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(credentials.to_json())
    ########        
    try:
        service = build("sheets", "v4", credentials=credentials)

        csv_file_path1 = 'reports/data.csv'
        with open(csv_file_path1, mode='r', encoding='utf-8-sig') as file:
            csv_reader = csv.reader(file)
            values = [row for row in csv_reader]

        # Prepare the request body
        body = {"values": values}

        # Execute the update request
        result1 = (
            service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=SPREADSHEET_ID,
                    valueInputOption='USER_ENTERED',
                    range='Raw!A1',
                    body=body,
                )
                .execute()
        )

        print(f"{result1.get('updatedCells')} cells updated for the raw data.")

        csv_file_path2 = 'reports/clean_data.csv'
        with open(csv_file_path2, mode='r', encoding='utf-8-sig') as file:
            csv_reader = csv.reader(file)
            values = [row for row in csv_reader]

        # Prepare the request body
        body = {"values": values}

        # Execute the update request
        result2 = (
            service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=SPREADSHEET_ID,
                    valueInputOption='USER_ENTERED',
                    range='Clean!A1',
                    body=body,
                )
                .execute()
        )

        print(f"{result2.get('updatedCells')} cells updated for the clean data.")

        return result1, result2
    


    except HttpError as error:
        print(f"An error occurred: {error}")
        return error

if __name__ == '__main__':
    main()