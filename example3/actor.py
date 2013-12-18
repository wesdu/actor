import gevent
import zmq.green as Zmq
import uuid
from gevent import spawn

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
        print 'ActorBroker looping'

    def __loop(self):
        spawn(Zmq.device, Zmq.QUEUE, self.xpub, self.xsub)
        poll = Zmq.Poller()
        dealer = self.dealer
        poll.register(dealer, Zmq.POLLIN)
        while True:
            poll_dict = dict(poll.poll())
            if dealer in poll_dict and poll_dict[dealer] == Zmq.POLLIN:
                scene_sid, actor_sid, contents = dealer.recv_multipart()
                if actor_sid:
                    a = self.find(actor_sid)
                    if a:
                        a << [scene_sid, contents]

    def send(self, *args):
        dealer = self.dealer
        dealer.send_multipart(*args)

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
        sub.setsockopt(Zmq.SUBSCRIBE, sid)
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
                print '[%s] recive broadcast `%s`' % (sid, contents)

    def loop(self):
        spawn(self.__loop)
        print 'actor %s looping' % self.sid

    def receive(self, contents):
        #todo
        sid = self.sid
        scene_sid, msg = contents
        print '[%s] recive `%s` from scene %s' % (sid, msg, scene_sid)

    def __lshift__(self, other):
        self.receive(other)

    def send(self, frames):
        send = Actor.broker.send
        send([self.sid] + frames)

    def __rshift__(self, other):
        self.send(other)


Actor.broker.loop()


if __name__ == '__main__':
    import random
    for i in xrange(0, 10):
        #Too many open files
        scene_sid = str(random.choice(xrange(1, 3)))
        msg = str(random.choice(['a', 'b', 'c']))
        a = Actor(i)
        a >> [scene_sid, msg]
    def run():
        while True:
            gevent.sleep(0)
    spawn(run).join()
