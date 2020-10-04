from datetime import datetime
import ntplib

def getLocalTime(client = None):
    if client != None:
        response = client.request('localhost')
        return response.tx_time

    return datetime.now().total_seconds()
