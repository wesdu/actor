import zmq.green as zmq
import gevent

context = zmq.Context()
ctx = context


def rrbroker():
        frontend = context.socket(zmq.ROUTER)
        frontend.bind('tcp://*:5570')

        backend = context.socket(zmq.ROUTER)
        #backend.bind('inproc://backend')
        backend.bind('tcp://*:5572')
        poll = zmq.Poller()
        poll.register(frontend, zmq.POLLIN)
        poll.register(backend,  zmq.POLLIN)

        while True:
            sockets = dict(poll.poll())
            if frontend in sockets:
                if sockets[frontend] == zmq.POLLIN:
                    uuid, sid, msg = [frontend.recv() for i in xrange(3)]
                    print 'frontend -> (%s) (%s) (%s)' % (uuid, sid, msg)
                    backend.send(msg, zmq.SNDMORE)
                    backend.send(sid, zmq.SNDMORE)
                    backend.send(uuid)
            if backend in sockets:
                if sockets[backend] == zmq.POLLIN:
                    uuid = backend.recv()
                    sid = backend.recv()
                    msg = backend.recv()
                    print 'backend <- (%s) (%s) (%s)' % (uuid, sid, msg)
                    frontend.send(msg, zmq.SNDMORE)
                    frontend.send(sid, zmq.SNDMORE)
                    frontend.send(uuid)


def xxbroker():
    xpub = ctx.socket(zmq.XPUB)
    xpub.bind('tcp://*:5574')
    xsub = ctx.socket(zmq.XSUB)
    xsub.bind('tcp://*:5576')
    zmq.device(zmq.QUEUE, xsub, xpub)


def main():
    """main function"""
    rr = gevent.spawn(rrbroker)
    xx = gevent.spawn(xxbroker)
    gevent.joinall([rr, xx])

if __name__ == "__main__":
    main()