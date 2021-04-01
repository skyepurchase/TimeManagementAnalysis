from __future__ import print_function
import datetime
import pickle
import os.path

import pandas as pd
import numpy as np

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']


def stripDatetime(series):
    if "date" in series['start']:
        start = datetime.datetime.strptime(series['start']['date'], '%Y-%m-%d')
        end = datetime.datetime.strptime(series['start']['date'], '%Y-%m-%d')
    else:
        start = datetime.datetime.strptime(series['start']['dateTime'][:19], '%Y-%m-%dT%H:%M:%S').time()
        end = datetime.datetime.strptime(series['end']['dateTime'][:19], '%Y-%m-%dT%H:%M:%S').time()

    return pd.Series([start, end])


def inRange(time_stamp, events):
    if events is None:
        return pd.Series(np.zeros(1, dtype=np.int64))

    time = datetime.datetime.strptime(time_stamp[0], "%H:%M:%S").time()

    occurrences = (events.start <= time) & (events.end > time)
    count = np.count_nonzero(np.array(occurrences.values))

    return pd.Series([count])


class DataAnalyser:
    def __init__(self):
        self.credentials = None
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                self.credentials = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not self.credentials or not self.credentials.valid:
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                self.credentials = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(self.credentials, token)

        self.service = build('calendar', 'v3', credentials=self.credentials)

        self.calendar_info = {'Generic': 'primary',
                              'Lecture Work': 'c_krn5r1k7ppkaoatkonrnaup63o@group.calendar.google.com',
                              'Leisure': 'c_46544nas4bq1j0iubthl8m5bi8@group.calendar.google.com',
                              'Menial Tasks': 'c_8sa66nob86sbnni235f5dfk1hg@group.calendar.google.com',
                              'Supervisions': 'c_81fnb8n9bf3hau131nb02p35gc@group.calendar.google.com',
                              'Lectures': 'a77nfv4okg843lato7vm58n3lnv3skan@import.calendar.google.com'}

    def getRawCalendarData(self, calendar: str, start: datetime, end: datetime):
        if calendar not in self.calendar_info:
            return None

        ID = self.calendar_info.get(calendar)

        events_result = self.service.events().list(calendarId=ID,
                                                   timeMin=start.isoformat() + 'Z',
                                                   timeMax=end.isoformat() + 'Z',
                                                   singleEvents=True,
                                                   orderBy='startTime').execute()

        raw_data = pd.DataFrame.from_dict(events_result.get('items', []))

        if raw_data.empty:
            return None

        raw_data = raw_data[['start', 'end']]
        raw_data = raw_data.apply(lambda x: stripDatetime(x), axis=1)

        return raw_data.rename(columns={0: 'start', 1: 'end'})

    def getSplitCalendarData(self, calendar: str, start: datetime, end: datetime, split=5):
        if calendar not in self.calendar_info:
            return None

        raw_data = self.getRawCalendarData(calendar, start, end)
        time_stamps = pd.date_range(start=start, periods=288, freq=f'{split}T').strftime('%H:%M:%S').to_frame()
        time_stamps = time_stamps.apply(lambda x: inRange(x, raw_data), axis=1).reset_index()
        time_stamps.columns = ['Time', calendar]

        return time_stamps

    def getDayData(self, day: datetime):
        data = pd.DataFrame()

        for calendar in self.calendar_info:
            new_data = self.getSplitCalendarData(calendar, day, day + datetime.timedelta(1))

            if new_data is not None:
                if data.empty:
                    data = new_data
                else:
                    data = data.merge(new_data, how='left', on='Time')

        return data

    def getCalendarDensity(self, calendar: str, start: datetime, end: datetime):
        if calendar not in self.calendar_info:
            return None

        data = pd.DataFrame()

        timestamp = start
        count = 0
        while timestamp < end:
            new_data = self.getSplitCalendarData(calendar, timestamp, timestamp + datetime.timedelta(1))
            new_data.set_index('Time', inplace=True)

            if new_data is not None:
                if data.empty:
                    data = new_data
                else:
                    data[calendar] = data[calendar].values + new_data[calendar].values

            timestamp += datetime.timedelta(1)
            count += 1

        data[calendar] = data[calendar] / count
        data = data.reset_index()
        data.columns = ['Time', calendar]

        return data
