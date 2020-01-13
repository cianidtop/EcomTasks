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
            self.tn.write(b"Action:Ping\r\n\r\n")
            self.tn.read_until(b"Timestamp")
            return float(self.tn.read_until(b"\r\n")[2:-4].decode())
        except OSError:
            print("ERROR callManager:connection closed")

    def get_queue_status(self, queue):
        try:
            queue_dict = []
            queue_index = 0
            self.tn.write(b"Action: QueueStatus\r\n\r\n")
            self.tn.read_until(b"Message: Queue status will follow\r\n\r\n", 10)
            data = self.tn.read_until(b"Event: QueueStatusComplete", 10).decode().split("\r\n\r\n")
            for block in data:
                lines = block.splitlines()
                if len(lines) >= 2 and lines[1] == "Queue: " + queue:
                    queue_dict.append({})
                    for line in lines:
                        line = line.split(": ")
                        queue_dict[queue_index][line[0]] = line[1]
                    queue_index += 1
            return queue_dict
        except OSError:
            print("ERROR callManager:connection closed")
        except EOFError:
            print("ERROR callManager:connection closed and no cooked data available")

    def get_channels_status(self):
        try:
            calls_dict = []
            self.tn.write(b"Action: Command\r\nCommand: sip show channels\r\n\r\n")
            self.tn.read_until(b"Response: Follows\r\n", 10)
            data = self.tn.read_until(b"END COMMAND", 10).decode().splitlines(False)
            for i in range(2, len(data) - 2):
                calls_dict.append({})
                calls_dict[i - 2]["Peer1"] = data[i][:16].rstrip()
                calls_dict[i - 2]["User/ANR"] = data[i][17:33].rstrip()
                calls_dict[i - 2]["Call_ID"] = data[i][34:50].rstrip()
                calls_dict[i - 2]["Format"] = data[i][51:67].rstrip()
                calls_dict[i - 2]["Hold"] = data[i][68:76].rstrip()
                calls_dict[i - 2]["Last_Message"] = data[i][77:92].rstrip()
                calls_dict[i - 2]["Expiry"] = data[i][93:103].rstrip()
                calls_dict[i - 2]["Peer2"] = data[i][104:].rstrip()
            return calls_dict

        except EOFError:
            print("ERROR callManager:connection closed and no cooked data available")
        except OSError:
            print("ERROR callManager:connection closed")

    def close(self):
        try:
            self.tn.write(b"Action: Logoff\r\n\r\n")
        except OSError:
            print("ERROR callManager:connection already closed")
        self.tn.close()
