from __future__ import print_function
import datetime
import pickle
import os.path

import pandas as pd

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/calendar.readonly']


def stripDatetime(series):
    start = datetime.datetime.strptime(series['start']['dateTime'], '%Y-%m-%dT%H:%M:%SZ')
    end = datetime.datetime.strptime(series['end']['dateTime'], '%Y-%m-%dT%H:%M:%SZ')
    return pd.Series([start, end])


def inRange(time_stamp, events):
    test = (events.start <= time_stamp) & (events.end >= time_stamp)
    return test.mean() > 0  # It works so I don't care


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

    def getRawCalendarDay(self, calendar: str, day: datetime):
        start = day
        end = day + datetime.timedelta(1)

        if calendar not in self.calendar_info:
            return None

        ID = self.calendar_info.get(calendar)

        events_result = self.service.events().list(calendarId=ID,
                                                   timeMin=start.isoformat() + 'Z',
                                                   timeMax=end.isoformat() + 'Z',
                                                   singleEvents=True,
                                                   orderBy='startTime').execute()

        raw_data = pd.DataFrame.from_dict(events_result.get('items', []))
        raw_data = raw_data[['start', 'end']]
        raw_data = raw_data.apply(lambda x: stripDatetime(x), axis=1)

        return raw_data.rename(columns={0: 'start', 1: 'end'})

    def getSplitCalendarDay(self, calendar: str, day: datetime, split=5):
        raw_data = self.getRawCalendarDay(calendar, day)

        if raw_data is None:
            return None

        time_stamps = pd.date_range(start=day, periods=288, freq=f'{split}T').to_frame()
        time_stamps = time_stamps.apply(lambda x: 1 if inRange(x[0], raw_data) else 0, axis=1).to_frame()

        return time_stamps

    def getDayDate(self, start: datetime):

        end = start + datetime.timedelta(1)
        raw_data = {}

        for calendar, ID in self.calendar_info.items():
            events_result = self.service.events().list(calendarId=ID,
                                                       timeMin=start.isoformat() + 'Z',
                                                       timeMax=end.isoformat() + 'Z',
                                                       singleEvents=True,
                                                       orderBy='startTime').execute()
            raw_data[calendar] = events_result.get('items', [])

        useful_data = {}

        for calendar in raw_data.keys():
            for event in raw_data[calendar]:
                if 'dateTime' in event.get('start').keys():
                    curr = datetime.datetime.strptime(event.get('start').get('dateTime'), '%Y-%m-%dT%H:%M:%SZ')
                    end = datetime.datetime.strptime(event.get('end').get('dateTime'), '%Y-%m-%dT%H:%M:%SZ')

                    while curr < end:
                        new_event = {}
                        key = str(curr)[11:19]
                        new_event['calendar'] = calendar
                        new_event['weekday'] = curr.weekday()

                        useful_data[key] = new_event

                        curr += datetime.timedelta(minutes=5)

        return useful_data

    def getCalendarData(self, calendar, start_date, days):
        ID = self.calendar_info[calendar]
        raw_data = {}

        for day in range(days):
            start = start_date + datetime.timedelta(day)
            end = start + datetime.timedelta(1)

            events_result = self.service.events().list(calendarId=ID,
                                                       timeMin=start.isoformat() + 'Z',
                                                       timeMax=end.isoformat() + 'Z',
                                                       singleEvents=True,
                                                       orderBy='startTime').execute()

            raw_data[day + 1] = events_result.get('items', [])

        useful_data = {}

        for day in raw_data.keys():
            for event in raw_data.get(day):
                if 'dateTime' in event.get('start').keys():
                    curr = datetime.datetime.strptime(event.get('start').get('dateTime'), '%Y-%m-%dT%H:%M:%SZ')
                    end = datetime.datetime.strptime(event.get('end').get('dateTime'), '%Y-%m-%dT%H:%M:%SZ')

                    while curr < end:
                        new_event = {}
                        key = str(curr)[11:19]

                        if key in useful_data.keys():
                            useful_data[key] += 1
                        else:
                            useful_data[key] = 1

                        curr += datetime.timedelta(minutes=5)

        for time in useful_data.keys():
            useful_data[time] /= days

        return useful_data
