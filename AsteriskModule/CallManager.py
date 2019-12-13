import telnetlib
import os


class CallManager:

    def ___init___(self, ip, username, secret):
        self.username = username
        self.secret = secret
        self.ip = ip

    def connect(self):
        self.tn = telnetlib.Telnet(self.ip, 5038)
        self.tn.write("Action:login")
        self.tn.write("Username:" + self.username)
        self.tn.write("Secret:" + self.secret)

    def ping(self):
        response = os.system("ping -c 1 " + self.ip)
        if response == 0:
            return True
        else:
            return False

    def close(self):
        self.tn.close()
