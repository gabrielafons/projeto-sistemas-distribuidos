"""Cliente (bot) em Python - Partes 1 a 5: login, canais, inscrição, publicação e histórico."""
import os
import random
import time

import zmq

import chat_pb2 as pb
from common import Messenger, encode, hora

BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
PROXY_HOST = os.environ.get("PROXY_HOST", "proxy")
TIMEOUT_MS = 5000

FRASES = ["bom dia", "alguém aí?", "olá, pessoal", "testando o canal", "mensagem aleatória",
          "tudo bem por aí?", "sistemas distribuídos são legais", "ping", "mais uma mensagem",
          "o que vocês acham?"]


class Bot:
    def __init__(self):
        self.name = os.environ.get("BOT_NAME") or f"py-bot-{random.randint(1000, 9999)}"
        self.msgr = Messenger(self.name)
        self.ctx = zmq.Context()
        self.req = None
        self._conectar()
        # inscrições: o bot é subscriber e conecta na saída (XPUB) do proxy
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(f"tcp://{PROXY_HOST}:5558")
        self.inscritos = set()

    def _conectar(self):
        if self.req is not None:
            self.req.close(linger=0)
        self.req = self.ctx.socket(zmq.REQ)
        self.req.connect(f"tcp://{BROKER_HOST}:5555")

    def requisitar(self, env):
        """Envia a requisição e espera a resposta. Se o serviço não responder
        no prazo, recria o socket REQ e tenta de novo."""
        while True:
            self.req.send(encode(env))
            self.msgr.show("ENVIA", env, "servidor")
            if self.req.poll(TIMEOUT_MS):
                resp = self.msgr.parse(self.req.recv())
                self.msgr.show("RECEBE", resp, resp.sender)
                return resp
            print(f"[{self.name}] sem resposta em {TIMEOUT_MS} ms, tentando de novo", flush=True)
            self._conectar()

    def login(self):
        """Repete o login até ser bem sucedido (em erro, tenta com outro nome)."""
        while True:
            resp = self.requisitar(self.msgr.build(pb.LOGIN, user=self.name))
            if resp.status == pb.OK:
                return
            time.sleep(1)
            self.name = f"py-bot-{random.randint(1000, 9999)}"
            self.msgr.name = self.name

    def listar_canais(self):
        resp = self.requisitar(self.msgr.build(pb.LIST_CHANNELS))
        return list(resp.channels) if resp.status == pb.OK else []

    def criar_canal(self, nome):
        resp = self.requisitar(self.msgr.build(pb.CREATE_CHANNEL, channel=nome))
        return resp.status == pb.OK

    def inscrever(self, canal):
        # o filtro do ZeroMQ casa o início do tópico; o filtro exato é feito em receber()
        self.sub.setsockopt(zmq.SUBSCRIBE, canal.encode())
        self.inscritos.add(canal)
        print(f"[{self.name}] inscrito no canal #{canal} ({len(self.inscritos)} inscrições)", flush=True)

    def publicar(self, canal, texto):
        resp = self.requisitar(self.msgr.build(pb.PUBLISH, channel=canal, text=texto))
        return resp.status == pb.OK

    def historico(self, canal):
        """Pede as últimas mensagens do canal. O servidor que responde (round-robin) tem
        todas as mensagens por causa da replicação."""
        resp = self.requisitar(self.msgr.build(pb.HISTORY, channel=canal))
        if resp.status != pb.OK:
            return
        print(f"[{self.name}] histórico de #{canal}: {resp.total} mensagens no total "
              f"(respondido por {resp.sender}); últimas {len(resp.entries)}:", flush=True)
        for e in resp.entries:
            print(f"    [L={e.clock}] {e.user}: {e.text}  (recebida por {e.origin})", flush=True)

    def receber(self):
        """Lê (sem bloquear) uma publicação recebida e a exibe."""
        topico, dados = self.sub.recv_multipart()
        canal = topico.decode()
        if canal not in self.inscritos:  # filtro exato do tópico
            return
        env = self.msgr.parse(dados)
        self.msgr.show("RECEBE", env, f"#{canal}", extra=f"  | recebida em {hora()}")

    def esperar(self, segundos):
        """Espera `segundos` segundos sem deixar de ler as publicações dos canais."""
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
        # 1) se existirem menos de 5 canais, cria um novo
        if len(canais) < 5:
            bot.criar_canal(f"canal-{random.randint(1000, 9999)}")
            canais = bot.listar_canais()
        # 2) se estiver inscrito em menos de 3 canais, se inscreve em mais um (aleatório)
        if len(bot.inscritos) < 3:
            disponiveis = [c for c in canais if c not in bot.inscritos]
            if disponiveis:
                bot.inscrever(random.choice(disponiveis))
        # 3) escolhe um canal e envia 10 mensagens aleatórias com 1 s de intervalo
        if canais:
            canal = random.choice(canais)
            for _ in range(10):
                bot.publicar(canal, random.choice(FRASES))
                bot.esperar(1)
            bot.historico(canal)
        else:
            bot.esperar(1)


if __name__ == "__main__":
    main()
