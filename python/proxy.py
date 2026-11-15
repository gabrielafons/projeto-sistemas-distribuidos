"""Proxy do Publish-Subscribe (XSUB/XPUB).

Publishers (servidores) conectam na porta 5557 (XSUB) e subscribers (clientes)
conectam na porta 5558 (XPUB). O proxy repassa as mensagens publicadas e
encaminha as inscrições dos subscribers para os publishers.
"""
import zmq

ctx = zmq.Context()

xsub = ctx.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

xpub = ctx.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("Proxy Pub/Sub: publishers em :5557 (XSUB), subscribers em :5558 (XPUB)", flush=True)
zmq.proxy(xsub, xpub)
