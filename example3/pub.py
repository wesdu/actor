import gevent
import zmq.green as zmq
from gevent import sleep
class Worker(object):
    pass

def main():
    """ main method """

    # Prepare our context and publisher
    context   = zmq.Context(1)
    publisher = context.socket(zmq.PUB)
    publisher.connect("tcp://localhost:5576")

    while True:
        # Write two messages, each with an envelope and content
        publisher.send_multipart(["1", "111111"])
        publisher.send_multipart(["2", "222222222222"])
        sleep(5)

    # We never get here but clean up anyhow
    publisher.close()
    context.term()

if __name__ == "__main__":
    main()