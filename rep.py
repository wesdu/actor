import zmq.green as zmq

from random import choice, random
import gevent

context = zmq.Context()


def broker():
        frontend = context.socket(zmq.ROUTER)
        frontend.bind('tcp://*:5570')

        backend = context.socket(zmq.DEALER)
        backend.bind('inproc://backend')

        poll = zmq.Poller()
        poll.register(frontend, zmq.POLLIN)
        poll.register(backend,  zmq.POLLIN)

        while True:
            sockets = dict(poll.poll())
            if frontend in sockets:
                if sockets[frontend] == zmq.POLLIN:
                    uuid, sid, msg = [frontend.recv() for i in xrange(3)]
                    print 'frontend -> (%s) (%s) (%s)' % (uuid, sid, msg)
                    backend.send(uuid, zmq.SNDMORE)
                    backend.send(sid, zmq.SNDMORE)
                    backend.send(msg)
            if backend in sockets:
                if sockets[backend] == zmq.POLLIN:
                    uuid = backend.recv()
                    sid = backend.recv()
                    msg = backend.recv()
                    print 'backend <- (%s) (%s) (%s)' % (uuid, sid, msg)
                    frontend.send(uuid, zmq.SNDMORE)
                    frontend.send(sid, zmq.SNDMORE)
                    frontend.send(msg)


def ServerWorker(context):
        id = random() * 100
        worker = context.socket(zmq.DEALER)
        worker.connect('inproc://backend')
        print 'Worker %d started' % id
        poll = zmq.Poller()
        poll.register(worker, zmq.POLLIN)
        reqs = 0
        while 1:
            workers = dict(poll.poll(100))
            if worker in workers:
                if workers[worker] == zmq.POLLIN:
                    uuid = worker.recv()
                    sid = worker.recv()
                    msg = worker.recv()
                    print 'Worker %d received %s from %s' % (id, msg, uuid)
                    #gevent.sleep(1/choice(range(1,10)))
                    worker.send(uuid, zmq.SNDMORE)
                    worker.send(sid, zmq.SNDMORE)
                    worker.send(msg)


def main():
    """main function"""
    workers = []
    for i in xrange(5):
        worker = gevent.spawn(ServerWorker, context)
        workers.append(worker)
    server = gevent.spawn(broker)
    server.join()

if __name__ == "__main__":
    main()