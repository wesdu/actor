import zmq.green as zmq
import gevent

context = zmq.Context()
ctx = context


def rrbroker():
        frontend = context.socket(zmq.ROUTER)
        frontend.bind('tcp://*:5570')

        backend = context.socket(zmq.ROUTER)
        backend.bind('tcp://*:5572')
        poll = zmq.Poller()
        poll.register(frontend, zmq.POLLIN)
        poll.register(backend,  zmq.POLLIN)

        while True:
            sockets = dict(poll.poll())
            if frontend in sockets:
                if sockets[frontend] == zmq.POLLIN:
                    actor_broker_uuid, actor_sid, scene_sid, msg = frontend.recv_multipart()
                    print 'backend -> (%s, %s, %s, %s)' % (actor_broker_uuid, actor_sid, scene_sid, msg)
                    backend.send(scene_sid, zmq.SNDMORE)
                    backend.send(actor_broker_uuid, zmq.SNDMORE)
                    backend.send(actor_sid, zmq.SNDMORE)
                    backend.send(msg)
            if backend in sockets:
                if sockets[backend] == zmq.POLLIN:
                    scene_sid, actor_broker_uuid, actor_sid, msg = backend.recv_multipart()
                    print 'frontend <- (%s, %s, %s, %s)' % (actor_broker_uuid, actor_sid, scene_sid, msg)
                    frontend.send(actor_broker_uuid, zmq.SNDMORE)
                    frontend.send(scene_sid, zmq.SNDMORE)
                    frontend.send(actor_sid, zmq.SNDMORE)
                    frontend.send(msg)


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