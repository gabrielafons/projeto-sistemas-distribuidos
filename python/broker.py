"""Broker do Request-Reply (ROUTER/DEALER).

Clientes (REQ) conectam na porta 5555 (ROUTER) e servidores (REP) conectam
na porta 5556 (DEALER). O DEALER distribui as requisições entre os servidores
conectados em round-robin (balanceamento de carga).
"""
import zmq

ctx = zmq.Context()

frontend = ctx.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")

backend = ctx.socket(zmq.DEALER)
backend.bind("tcp://*:5556")

print("Broker: clientes em :5555, servidores em :5556", flush=True)
zmq.proxy(frontend, backend)
