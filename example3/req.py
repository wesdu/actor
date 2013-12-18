import zmq.green as zmq

from random import choice, random
import gevent

context = zmq.Context()


def ServerWorker(context, i):
        id = str(i)
        worker = context.socket(zmq.DEALER)
        worker.setsockopt(zmq.IDENTITY, id)
        worker.connect('tcp://localhost:5572')
        print 'Worker %s started' % id
        poll = zmq.Poller()
        poll.register(worker, zmq.POLLIN)
        reqs = 0
        while 1:
            workers = dict(poll.poll(100))
            if worker in workers:
                if workers[worker] == zmq.POLLIN:
                    actor_broker_uuid, actor_sid, msg = worker.recv_multipart()
                    print 'Worker %s received %s from %s' % (id, msg, actor_sid)
                    worker.send_multipart([actor_broker_uuid, actor_sid, msg])

def main():
    """main function"""
    workers = []
    for i in xrange(5):
        worker = gevent.spawn(ServerWorker, context, i)
        workers.append(worker)
    gevent.joinall(workers)

if __name__ == "__main__":
    main()