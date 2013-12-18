import gevent
import zmq.green as zmq
from gevent import sleep
import uuid

def autoUU():
    return str(uuid.uuid4())[:16]

class Scene(object):
    def __init__(self, sid):
        if not sid:
            raise Exception('must define sid!')
        self.actors = []
        self.sid = str(sid)
        self.link_publisher()

    def link_publisher(self):
        context = zmq.Context(1)
        publisher = context.socket(zmq.PUB)
        publisher.connect("tcp://localhost:5576")
        self.publisher = publisher

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