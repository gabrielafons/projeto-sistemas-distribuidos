import zmq

ctx = zmq.Context()

frontend = ctx.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")

backend = ctx.socket(zmq.DEALER)
backend.bind("tcp://*:5556")

print("Broker: clientes em :5555, servidores em :5556", flush=True)
zmq.proxy(frontend, backend)
