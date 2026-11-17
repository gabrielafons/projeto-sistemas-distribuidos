import zmq

ctx = zmq.Context()

xsub = ctx.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

xpub = ctx.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("Proxy Pub/Sub: publishers em :5557 (XSUB), subscribers em :5558 (XPUB)", flush=True)
zmq.proxy(xsub, xpub)
