import os
import random
import time

import zmq

import chat_pb2 as pb
from common import Messenger, codificar, hora

BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
PROXY_HOST = os.environ.get("PROXY_HOST", "proxy")
TIMEOUT_MS = 5000

FRASES = ["bom dia", "alguém aí?", "olá, pessoal", "testando o canal", "mensagem aleatória",
          "tudo bem por aí?", "sistemas distribuídos são legais", "ping", "mais uma mensagem",
          "o que vocês acham?"]


class Bot:
    def __init__(self):
        self.nome = os.environ.get("BOT_NAME") or f"py-bot-{random.randint(1000, 9999)}"
        self.mensageiro = Messenger(self.nome)
        self.contexto = zmq.Context()
        self.req = None
        self._conectar()
        self.sub = self.contexto.socket(zmq.SUB)
        self.sub.connect(f"tcp://{PROXY_HOST}:5558")
        self.inscritos = set()

    def _conectar(self):
        if self.req is not None:
            self.req.close(linger=0)
        self.req = self.contexto.socket(zmq.REQ)
        self.req.connect(f"tcp://{BROKER_HOST}:5555")

    def requisitar(self, envelope):
        while True:
            self.req.send(codificar(envelope))
            self.mensageiro.exibir("ENVIA", envelope, "servidor")
            if self.req.poll(TIMEOUT_MS):
                resposta = self.mensageiro.ler(self.req.recv())
                self.mensageiro.exibir("RECEBE", resposta, resposta.remetente)
                return resposta
            print(f"[{self.nome}] sem resposta em {TIMEOUT_MS} ms, tentando de novo", flush=True)
            self._conectar()

    def login(self):
        while True:
            resposta = self.requisitar(self.mensageiro.montar(pb.LOGIN, usuario=self.nome))
            if resposta.status == pb.OK:
                return
            time.sleep(1)
            self.nome = f"py-bot-{random.randint(1000, 9999)}"
            self.mensageiro.nome = self.nome

    def listar_canais(self):
        resposta = self.requisitar(self.mensageiro.montar(pb.LISTAR_CANAIS))
        return list(resposta.canais) if resposta.status == pb.OK else []

    def criar_canal(self, nome):
        resposta = self.requisitar(self.mensageiro.montar(pb.CRIAR_CANAL, canal=nome))
        return resposta.status == pb.OK

    def inscrever(self, canal):
        self.sub.setsockopt(zmq.SUBSCRIBE, canal.encode())
        self.inscritos.add(canal)
        print(f"[{self.nome}] inscrito no canal #{canal} ({len(self.inscritos)} inscrições)", flush=True)

    def publicar(self, canal, texto):
        resposta = self.requisitar(self.mensageiro.montar(pb.PUBLICAR, canal=canal, texto=texto))
        return resposta.status == pb.OK

    def receber(self):
        topico, dados = self.sub.recv_multipart()
        canal = topico.decode()
        if canal not in self.inscritos:
            return
        envelope = self.mensageiro.ler(dados)
        self.mensageiro.exibir("RECEBE", envelope, f"#{canal}", extra=f"  | recebida em {hora()}")

    def esperar(self, segundos):
        fim = time.time() + segundos
        while True:
            resto = fim - time.time()
            if resto <= 0:
                return
            if self.sub.poll(int(resto * 1000) + 1):
                self.receber()


def main():
    bot = Bot()
    bot.login()
    while True:
        canais = bot.listar_canais()
        if len(canais) < 5:
            bot.criar_canal(f"canal-{random.randint(1000, 9999)}")
            canais = bot.listar_canais()
        if len(bot.inscritos) < 3:
            disponiveis = [c for c in canais if c not in bot.inscritos]
            if disponiveis:
                bot.inscrever(random.choice(disponiveis))
        if canais:
            canal = random.choice(canais)
            for _ in range(10):
                bot.publicar(canal, random.choice(FRASES))
                bot.esperar(1)
        else:
            bot.esperar(1)


if __name__ == "__main__":
    main()
