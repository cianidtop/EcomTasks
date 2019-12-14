import telnetlib
import os


class CallManager:

    def __init__(self, ip, port, username, secret):
        self.username = username
        self.secret = secret
        self.ip = ip
        self.port = port
        self.tn = telnetlib.Telnet()

    def connect(self):
        self.tn.open(self.ip, self.port)
        try:
            self.tn.write(b"Action:login\n")
            self.tn.write(b"Username:" + self.username)
            self.tn.write(b"Secret:" + self.secret)
        except OSError:
            print("ERROR callManager:connection closed")

    def ping(self):
        try:
            self.tn.write(b"Action: Ping\r")
        except OSError:
            print("ERROR callManager:connection closed")

    def get_channels_status(self):
        try:
            callsDict = []
            self.tn.write(b"sip show channels\n")
            data = self.tn.read_until(b">", 5).splitlines(False)
            for i in range(1, len(data) - 2):
                parts = data[i].split()
                callsDict.append(dict)
                callsDict[i]["Peer1"] = parts[0]
                callsDict[i]["User/ANR"] = parts[1]
                callsDict[i]["Call_ID"] = parts[2]
                callsDict[i]["Format"] = parts[3]
                callsDict[i]["Hold"] = parts[4]
                callsDict[i]["Last_Message"] = parts[5]
                callsDict[i]["Expiry"] = parts[6]
                callsDict[i]["Peer2"] = parts[7]
            return callsDict

        except EOFError:
            print("ERROR callManager:connection closed and no cooked data available")
        except OSError:
            print("ERROR callManager:connection closed")

    def close(self):
        try:
            self.tn.write(b"Action: Logoff\r")
        except OSError:
            print("ERROR callManager:connection already closed")
        self.tn.close()
