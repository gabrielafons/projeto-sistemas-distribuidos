"""Serviço de referência (Partes 3 e 4).

Só conversa com os servidores (REQ/REP na porta 5559). Responsabilidades:
  - GET_RANK:     informa o rank do servidor (e o cadastra, sem repetir nomes);
  - LIST_SERVERS: devolve nome e rank dos servidores disponíveis;
  - HEARTBEAT:    mantém o servidor na lista; se um servidor para de enviar o
                  heartbeat, é removido da lista.

A partir da Parte 4 a hora correta vem do coordenador eleito entre os servidores,
então a resposta do heartbeat não traz mais a hora.
"""
import os
import time

import zmq

import chat_pb2 as pb
from common import Messenger, encode

PORT = os.environ.get("REF_PORT", "5559")
TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", "30"))  # segundos sem heartbeat


def main():
    msgr = Messenger("reference")
    ranks = {}     # nome -> rank (o rank de um nome não muda, mesmo que ele volte depois)
    enderecos = {} # nome -> endereço para falar com o servidor
    ativos = {}    # nome -> hora (time.time()) do último sinal de vida

    ctx = zmq.Context()
    rep = ctx.socket(zmq.REP)
    rep.bind(f"tcp://*:{PORT}")
    print(f"[reference] escutando em :{PORT} (timeout de heartbeat: {TIMEOUT}s)", flush=True)

    def remover_inativos():
        for nome, visto in list(ativos.items()):
            if time.time() - visto > TIMEOUT:
                del ativos[nome]
                print(f"[reference] servidor {nome} removido da lista (sem heartbeat há {TIMEOUT:.0f}s)", flush=True)

    def erro(type_, texto):
        return msgr.build(type_, status=pb.ERROR, error=texto)

    def tratar(env):
        nome = env.sender
        if env.type == pb.GET_RANK:
            if nome not in ranks:
                ranks[nome] = len(ranks) + 1
            enderecos[nome] = env.address
            ativos[nome] = time.time()
            return msgr.build(env.type, status=pb.OK, rank=ranks[nome])

        if env.type == pb.LIST_SERVERS:
            lista = [pb.ServerInfo(name=n, rank=ranks[n], address=enderecos[n]) for n in sorted(ativos, key=ranks.get)]
            return msgr.build(env.type, status=pb.OK, servers=lista)

        if env.type == pb.HEARTBEAT:
            if nome not in ranks:
                return erro(env.type, "servidor não cadastrado: peça o rank primeiro")
            ativos[nome] = time.time()  # volta para a lista se tinha sido removido
            return msgr.build(env.type, status=pb.OK)

        return erro(env.type, "tipo de mensagem desconhecido")

    while True:
        dados = rep.recv()
        remover_inativos()
        try:
            env = msgr.parse(dados)
        except Exception:
            resp = erro(pb.UNKNOWN, "mensagem inválida (não é um Envelope Protobuf)")
            rep.send(encode(resp))
            continue
        msgr.show("RECEBE", env, env.sender)
        resp = tratar(env)
        rep.send(encode(resp))
        msgr.show("ENVIA", resp, env.sender)


if __name__ == "__main__":
    main()
