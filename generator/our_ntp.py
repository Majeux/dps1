from datetime import datetime
import ntplib

def getLocalTime(ntp = None):
    if ntp != None:
        client, address = ntp
        response = client.request(address)
        return response.recv_time

    return datetime.now()
