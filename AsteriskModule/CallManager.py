import telnetlib


class CallManager:

    def __init__(self,host, port, username, secret):
        self.username = username.encode('ascii')
        self.secret = secret.encode('ascii')
        self.host = host
        self.port = port
        self.tn = telnetlib.Telnet()
        self.queue_status = {}
        self.channel_status = []

    def connect(self):
        self.tn.open(self.host, self.port)
        self.tn.write(b"Action:login\r\n")
        self.tn.write(b"Username:" + self.username + b"\r\n")
        self.tn.write(b"Secret:" + self.secret + b"\r\n\r\n")
        self.tn.write(b"Events: OFF\r\n\r\n")

    def ping(self):
        self.tn.write(b"Action:Ping\r\n\r\n")
        self.tn.read_until(b"Timestamp")
        return float(self.tn.read_until(b"\r\n")[2:-4].decode())

    def update_queue_status(self):
        self.queue_status = {}
        self.tn.write(b"Action: QueueStatus\r\n\r\n")
        self.tn.read_until(b"Message: Queue status will follow\r\n\r\n", 10)
        data = self.tn.read_until(b"Event: QueueStatusComplete", 10).decode().split("\r\n\r\n")
        for block in data:
            lines = block.splitlines()
            if len(lines) >= 2 and lines[1].split()[0] == "Queue:":
                queue = lines[1].split()[1]
                if queue not in self.queue_status.keys():
                    self.queue_status[queue] = []
                self.queue_status[queue].append({})
                for line in lines:
                    line = line.split(": ")
                    self.queue_status[queue][-1][line[0]] = line[1]

    def update_channels_status(self):
        self.channel_status = []
        self.tn.write(b"Action: Command\r\nCommand: sip show channels\r\n\r\n")
        self.tn.read_until(b"Response: Follows\r\n", 10)
        data = self.tn.read_until(b"END COMMAND", 10).decode().splitlines(False)
        for i in range(2, len(data) - 2):
            self.channel_status.append({})
            self.channel_status[i - 2]["Peer1"] = data[i][:16].rstrip()
            self.channel_status[i - 2]["User/ANR"] = data[i][17:33].rstrip()
            self.channel_status[i - 2]["Call_ID"] = data[i][34:50].rstrip()
            self.channel_status[i - 2]["Format"] = data[i][51:67].rstrip()
            self.channel_status[i - 2]["Hold"] = data[i][68:76].rstrip()
            self.channel_status[i - 2]["Last_Message"] = data[i][77:92].rstrip()
            self.channel_status[i - 2]["Expiry"] = data[i][93:103].rstrip()
            self.channel_status[i - 2]["Peer2"] = data[i][104:].rstrip()

    def get_queue_status(self, queue):
        self.update_queue_status()
        return self.queue_status[queue]

    def get_channels_status(self):
        self.update_channels_status()
        return self.channel_status

    def close(self):
        self.tn.write(b"Action: Logoff\r\n\r\n")
        self.tn.close()
