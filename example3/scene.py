import gevent
import zmq.green as zmq
import uuid

context = zmq.Context()

def autoUU():
    return str(uuid.uuid4())[:16]

class Scene(object):
    def __init__(self, sid):
        if not sid:
            raise Exception('must define sid!')
        self.actors = []
        self.sid = str(sid)
        gevent.spawn(self.link_publisher)
        gevent.spawn(self.link_dealer)

    def link_publisher(self):
        publisher = context.socket(zmq.PUB)
        publisher.connect("tcp://localhost:5576")
        self.publisher = publisher

    def link_dealer(self):
        dealer = context.socket(zmq.DEALER)
        dealer.setsockopt(zmq.IDENTITY, self.sid)
        dealer.connect('tcp://localhost:5572')
        print 'Scene %s started' % self.sid
        poll = zmq.Poller()
        poll.register(dealer, zmq.POLLIN)
        while 1:
            dealers = dict(poll.poll(100))
            if dealer in dealers:
                if dealers[dealer] == zmq.POLLIN:
                    actor_broker_uuid, actor_sid, msg = dealer.recv_multipart()
                    print 'Scene %s received %s from %s' % (self.sid, msg, actor_sid)
                    if actor_sid not in self.actors:
                        self.actors.append(actor_sid)
                    dealer.send_multipart([actor_broker_uuid, actor_sid, msg])

    def pub(self, actor=0, msg='', broadcast=True):
        p = self.publisher
        if broadcast:
            for a in self.actors:
                p.send_multipart([str(a), str(msg)])
        else:
            p.send_multipart([str(actor), str(msg)])

    def destory(self):
        self.publisher.close()

if __name__ == "__main__":
    scene1 = Scene(1)
    scene2 = Scene('2')
    def run():
        while 1:
            scene1.pub(msg="this is a broadcast from scene1")
            gevent.sleep(5)
            scene2.pub(msg="this is a broadcast from scene2")
            gevent.sleep(5)
    gevent.spawn(run).join()