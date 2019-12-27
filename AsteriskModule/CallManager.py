import telnetlib


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
            self.tn.write(b"Action:login\r\n")
            self.tn.write(b"Username:" + self.username + b"\r\n")
            self.tn.write(b"Secret:" + self.secret + b"\r\n\r\n")
        except OSError:
            print("ERROR callManager:connection closed")

    def ping(self):
        try:
            self.tn.write(b"Action: Ping\r\n\r\n")
        except OSError:
            print("ERROR callManager:connection closed")

    def get_queue_status(self, queue):
        try:
            queueDict = {}
            self.tn.write(b"Action: QueueStatus\r\n\r\n")
            self.tn.read_until(b"Message: Queue status will follow\r\n\r\n", 10)
            data = self.tn.read_until(b"Event: QueueStatusComplete", 10).decode().split("\r\n\r\n")
            for block in data:
                lines = block.splitlines()
                if lines[0] == "Event: QueueParams" and lines[1] == "Queue: " + queue:
                    lines.pop(0)
                    for line in lines:
                        line = line.split(": ")
                        queueDict[line[0]] = line[1]
                    return queueDict
        except OSError:
            print("ERROR callManager:connection closed")
        except EOFError:
            print("ERROR callManager:connection closed and no cooked data available")

    # def get_channels_status(self):
    #     try:
    #         callsDict = []
    #         self.tn.write(b"sip show channels\n")
    #         data = self.tn.read_until(b">", 5).splitlines(False)
    #         for i in range(1, len(data) - 2):
    #             parts = data[i].split()
    #             callsDict.append(dict)
    #             callsDict[i]["Peer1"] = parts[0]
    #             callsDict[i]["User/ANR"] = parts[1]
    #             callsDict[i]["Call_ID"] = parts[2]
    #             callsDict[i]["Format"] = parts[3]
    #             callsDict[i]["Hold"] = parts[4]
    #             callsDict[i]["Last_Message"] = parts[5]
    #             callsDict[i]["Expiry"] = parts[6]
    #             callsDict[i]["Peer2"] = parts[7]
    #         return callsDict
    #
    #     except EOFError:
    #         print("ERROR callManager:connection closed and no cooked data available")
    #     except OSError:
    #         print("ERROR callManager:connection closed")

    def close(self):
        try:
            self.tn.write(b"Action: Logoff\r\n\r\n")
        except OSError:
            print("ERROR callManager:connection already closed")
        self.tn.close()
