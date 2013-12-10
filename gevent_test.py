import gevent
from gevent.queue import Queue
from gevent import Greenlet, spawn

class Actor(gevent.Greenlet):

    def __init__(self):
        self.inbox = Queue()
        Greenlet.__init__(self)

    def receive(self, message):
        """
        Define in your subclass.
        """
        raise NotImplemented()

    def _run(self):
        self.running = True

        while self.running:
            message = self.inbox.get()
            self.receive(message)




class Pinger(Actor):
    def receive(self, message):
        print(message)
        pong.inbox.put('ping')

class Ponger(Actor):
    def receive(self, message):
        print(message)
        ping.inbox.put('pong')

ping = Pinger()
pong = Ponger()

ping.start()
pong.start()

ping.inbox.put('start')

def run():
    while 1:
        gevent.sleep(0)
spawn(run).join()