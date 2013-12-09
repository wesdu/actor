import zmq.green as Zmq
from gevent import spawn
import gevent
import uuid

class ActorManager(object):
    def __init__(self, msg_filter=''):
        self.db = {}
        self.uuid = str(uuid.uuid4())[:16]
        ctx = Zmq.Context()
        self.ctx = ctx
        sub = ctx.socket(Zmq.SUB)
        sub.connect("tcp://localhost:5563")
        sub.setsockopt(Zmq.SUBSCRIBE, msg_filter)
        self.sub = sub
        dealer = ctx.socket(Zmq.DEALER)
        dealer.setsockopt(Zmq.IDENTITY, self.uuid )
        dealer.connect('tcp://localhost:5570')
        self.dealer = dealer

    def add(self, sid, obj):
        sid = str(sid)
        self.db[sid] = obj

    def find(self, sid):
        sid = str(sid)
        return self.db.get(sid)

    def loop(self):
        spawn(self.__loop)
        self.send('', Zmq.SNDMORE)
        self.send('')
        print 'looping'
    def __loop(self):
        poll = Zmq.Poller()
        dealer = self.dealer
        sub = self.sub
        poll.register(dealer, Zmq.POLLIN)
        poll.register(sub, Zmq.POLLIN)
        while True:
            polldict = dict(poll.poll(100))
            if sub in polldict:
                if polldict[sub] == Zmq.POLLIN:
                    [sid, contents] = sub.recv_multipart()
                    a = self.find(sid)
                    if a:
                        a << contents
            if dealer in polldict:
                if polldict[dealer] == Zmq.POLLIN:
                    sid, contents = [dealer.recv() for i in xrange(2)]
                    print 'dealer', sid, contents




    def send(self, *arg):
        self.dealer.send(*arg)

    def destroy(self):
        self.sub.close()
        self.dealer.close()
        self.ctx.term()


class Actor(object):
    manager = ActorManager()

    def __init__(self, sid):
        """

        @rtype : Actor
        """
        sid = str(sid)
        Actor.manager.add(sid, self)
        self.sid = sid

    def receive(self, contents):
        sid = self.sid
        print "[%s] %s\n" % (str(sid), contents)

    def __lshift__(self, other):
        self.receive(other)

    def __send(self, *arg):
        Actor.manager.send(*arg)

    def send(self, *arg):
        spawn(self.__send, *arg)

    def __rshift__(self, other):
        self.send(other)

Actor.manager.loop()

for i in xrange(0, 1000):
    #Too many open files
    a = Actor(i)


if __name__ == '__main__':
    def run():
        while True:
            gevent.sleep(1)
    spawn(run).join()
