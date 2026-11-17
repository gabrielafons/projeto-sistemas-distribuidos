import os
import time

import zmq

import chat_pb2 as pb
from common import Mensageiro, codificar

PORTA = os.environ.get("REF_PORT", "5559")
TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT", "30"))


def main():
    mensageiro = Mensageiro("reference")
    ranks = {}
    enderecos = {}
    ativos = {}

    contexto = zmq.Context()
    atendimento = contexto.socket(zmq.REP)
    atendimento.bind(f"tcp://*:{PORTA}")
    print(f"[reference] escutando em :{PORTA} (timeout de heartbeat: {TIMEOUT}s)", flush=True)

    def remover_inativos():
        for nome, visto in list(ativos.items()):
            if time.time() - visto > TIMEOUT:
                del ativos[nome]
                print(f"[reference] servidor {nome} removido da lista (sem heartbeat há {TIMEOUT:.0f}s)", flush=True)

    def erro(tipo, texto):
        return mensageiro.montar(tipo, status=pb.ERRO, erro=texto)

    def tratar(envelope):
        nome = envelope.remetente
        if envelope.tipo == pb.OBTER_RANK:
            if nome not in ranks:
                ranks[nome] = len(ranks) + 1
            enderecos[nome] = envelope.endereco
            ativos[nome] = time.time()
            return mensageiro.montar(envelope.tipo, status=pb.OK, rank=ranks[nome])

        if envelope.tipo == pb.LISTAR_SERVIDORES:
            lista = [pb.InfoServidor(nome=n, rank=ranks[n], endereco=enderecos[n]) for n in sorted(ativos, key=ranks.get)]
            return mensageiro.montar(envelope.tipo, status=pb.OK, servidores=lista)

        if envelope.tipo == pb.BATIMENTO:
            if nome not in ranks:
                return erro(envelope.tipo, "servidor não cadastrado: peça o rank primeiro")
            ativos[nome] = time.time()
            return mensageiro.montar(envelope.tipo, status=pb.OK)

        return erro(envelope.tipo, "tipo de mensagem desconhecido")

    while True:
        dados = atendimento.recv()
        remover_inativos()
        try:
            envelope = mensageiro.ler(dados)
        except Exception:
            resposta = erro(pb.DESCONHECIDO, "mensagem inválida (não é um Envelope Protobuf)")
            atendimento.send(codificar(resposta))
            continue
        mensageiro.exibir("RECEBE", envelope, envelope.remetente)
        resposta = tratar(envelope)
        atendimento.send(codificar(resposta))
        mensageiro.exibir("ENVIA", resposta, envelope.remetente)


if __name__ == "__main__":
    main()
