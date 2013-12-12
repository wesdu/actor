import zmq.green as Zmq
from gevent import spawn
import gevent
import uuid


class ActorBroker(object):
    def __init__(self, msg_filter=''):
        self.db = {}
        self.uuid = str(uuid.uuid4())[:16]
        ctx = Zmq.Context()
        self.ctx = ctx

        xsub = ctx.socket(Zmq.XSUB)
        xsub.connect('tcp://localhost:5574')
        xpub = ctx.socket(Zmq.XPUB)
        xpub.bind('inproc://'+self.uuid)
        self.xpub = xpub
        self.xsub = xsub

        dealer = ctx.socket(Zmq.DEALER)
        dealer.setsockopt(Zmq.IDENTITY, self.uuid)
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
        print 'looping'

    def __loop(self):
        Zmq.device(Zmq.QUEUE, self.xpub, self.xsub)
        poll = Zmq.Poller()
        dealer = self.dealer
        poll.register(dealer, Zmq.POLLIN)
        while True:
            poll_dict = dict(poll.poll())
            if dealer in poll_dict and poll_dict[dealer] == Zmq.POLLIN:
                sid, contents = [dealer.recv() for i in xrange(2)]
                if sid:
                    a = self.find(sid)
                    if a:
                        a << contents

    def send(self, *args):
        dealer = self.dealer
        dealer.send(*args)

    def destroy(self):
        self.dealer.close()
        self.ctx.term()


class Actor(object):
    broker = ActorBroker()
    def __init__(self, sid):
        """

        @rtype : Actor
        """
        sid = str(sid)
        Actor.broker.add(sid, self)
        print '[', sid, ']', 'start'
        self.sid = sid
        sub = Actor.broker.ctx.socket(Zmq.SUB)
        sub.connect('inproc://' + Actor.broker.uuid)
        sub.setsockopt(Zmq.SUBSCRIBE, '')
        self.sub = sub
        self.loop()

    def __loop(self):
        sub = self.sub
        poll = Zmq.Poller()
        poll.register(sub, Zmq.POLLIN)
        while True:
            poll_dict = dict(poll.poll())
            if sub in poll_dict and poll_dict[sub] == Zmq.POLLIN:
                [sid, contents] = sub.recv_multipart()
                print sid, contents

    def loop(self):
        spawn(self.__loop)
        print 'actor %s looping' % self.sid

    def receive(self, contents):
        #todo
        sid = self.sid
        print "[%s] %s\n" % (str(sid), contents)

    def __lshift__(self, other):
        self.receive(other)

    def send(self, *frames):
        send = Actor.broker.send
        send(self.sid, Zmq.SNDMORE)
        for frame in frames[:-1]:
            send(frame, Zmq.SNDMORE)
        send(frames[-1])

    def __rshift__(self, other):
        self.send(other)


Actor.broker.loop()


if __name__ == '__main__':
    import random
    for i in xrange(0, 300):
        #Too many open files
        a = Actor(i)
        a >> str(random.choice(xrange(0, 5)))
    def run():
        while True:
            gevent.sleep(0)
    spawn(run).join()
