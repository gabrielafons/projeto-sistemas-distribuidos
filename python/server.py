"""Servidor em Python - Partes 1 a 5: login, canais, publicações, relógios,
heartbeat, eleição do coordenador, sincronização do relógio e replicação dos dados."""
import os
import random
import time

import zmq

import chat_pb2 as pb
from common import Messenger, PhysicalClock, encode, entry_from_proto, entry_to_proto
from store import Store, NAME_RE

NAME = os.environ.get("SERVER_NAME", "homem-de-ferro-python-1")
BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
PROXY_HOST = os.environ.get("PROXY_HOST", "proxy")
REF_HOST = os.environ.get("REF_HOST", "reference")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
PEER_PORT = os.environ.get("PEER_PORT", "6000")  # porta em que outros servidores falam com este
ADDRESS = os.environ.get("ADVERTISE_ADDR", f"tcp://{NAME}:{PEER_PORT}")
CLOCK_SKEW = float(os.environ.get("CLOCK_SKEW", "0"))  # só para testes: relógio que anda errado

HEARTBEAT_A_CADA = 10   # mensagens de clientes recebidas entre dois heartbeats
SINCRONIZA_A_CADA = 15  # mensagens de clientes recebidas entre duas atualizações do relógio
TIMEOUT_REQ = 2.0       # segundos esperando a resposta da referência
TIMEOUT_SERVIDOR = 1.0  # segundos esperando a resposta de outro servidor
ESPERA_ANUNCIO = 5.0    # segundos esperando o anúncio do novo coordenador depois de um OK
ANTI_ENTROPIA = 20.0    # segundos entre duas sincronizações (pull) dos dados com outro servidor
TIMEOUT_SYNC = 3.0      # segundos esperando a resposta de um SYNC (traz todas as entradas)
HISTORICO = 10          # quantidade de mensagens devolvidas no HISTORY

TOPICO_SERVIDORES = "servers"  # anúncios de coordenador
TOPICO_REPLICA = "replica"     # novas entradas replicadas entre os servidores
RESERVADOS = {TOPICO_SERVIDORES, TOPICO_REPLICA}


class Servidor:
    def __init__(self):
        self.msgr = Messenger(NAME, PhysicalClock(CLOCK_SKEW))
        self.relogio = self.msgr.physical
        self.store = Store(DATA_DIR, NAME)
        self.rank = None
        self.n_msgs = 0                # mensagens de clientes recebidas
        self.coordinator = None        # nome do servidor eleito coordenador
        self.servidores = {}           # nome -> ServerInfo (última lista da referência)
        self.precisa_eleicao = False   # outro servidor pediu eleição / coordenador caiu
        self.proxima_eleicao = 0.0     # enquanto não há coordenador, quando tentar de novo
        self.proxima_sync = time.time() + ANTI_ENTROPIA  # próximo pull de dados

        self.ctx = zmq.Context()
        # requisições dos clientes (via broker)
        self.rep = self.ctx.socket(zmq.REP)
        self.rep.connect(f"tcp://{BROKER_HOST}:5556")
        # requisições de outros servidores (eleição e relógio)
        self.peer = self.ctx.socket(zmq.REP)
        self.peer.bind(f"tcp://*:{PEER_PORT}")
        # publicações: o servidor é publisher e conecta na entrada (XSUB) do proxy
        self.pub = self.ctx.socket(zmq.PUB)
        self.pub.connect(f"tcp://{PROXY_HOST}:5557")
        # anúncios de coordenador: subscriber do tópico "servers" na saída (XPUB) do proxy
        self.sub = self.ctx.socket(zmq.SUB)
        self.sub.connect(f"tcp://{PROXY_HOST}:5558")
        self.sub.setsockopt(zmq.SUBSCRIBE, TOPICO_SERVIDORES.encode())
        # replicação: as novas entradas dos outros servidores chegam no tópico "replica"
        self.sub.setsockopt(zmq.SUBSCRIBE, TOPICO_REPLICA.encode())

        self.poller = zmq.Poller()
        for s in (self.rep, self.peer, self.sub):
            self.poller.register(s, zmq.POLLIN)

    # ---------- comunicação com outros serviços (REQ/REP) ----------

    def pedir(self, endereco, env, timeout=TIMEOUT_REQ):
        """Envia uma requisição e espera a resposta. Devolve None se não responder.
        Enquanto espera, continua atendendo os pedidos de outros servidores (assim
        dois servidores que pedem algo um ao outro ao mesmo tempo não se travam)."""
        s = self.ctx.socket(zmq.REQ)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(endereco)
        espera = zmq.Poller()
        espera.register(s, zmq.POLLIN)
        espera.register(self.peer, zmq.POLLIN)
        try:
            s.send(encode(env))
            self.msgr.show("ENVIA", env, endereco.split("//")[1])
            fim = time.time() + timeout
            while True:
                resto = fim - time.time()
                if resto <= 0:
                    print(f"[{NAME}] sem resposta de {endereco} em {timeout}s", flush=True)
                    return None
                eventos = dict(espera.poll(int(resto * 1000) + 1))
                if self.peer in eventos:
                    self.atender_peer()
                if s in eventos:
                    resp = self.msgr.parse(s.recv())
                    self.msgr.show("RECEBE", resp, resp.sender)
                    return resp
        finally:
            s.close()

    def referencia(self, env):
        return self.pedir(f"tcp://{REF_HOST}:5559", env)

    def registrar(self):
        """Pede o rank ao serviço de referência (repete até conseguir)."""
        while True:
            resp = self.referencia(self.msgr.build(pb.GET_RANK, address=ADDRESS))
            if resp and resp.status == pb.OK:
                self.rank = resp.rank
                print(f"[{NAME}] rank {self.rank}, endereço {ADDRESS}", flush=True)
                return
            time.sleep(2)

    def atualizar_servidores(self):
        """Pede à referência a lista de servidores disponíveis."""
        lista = self.referencia(self.msgr.build(pb.LIST_SERVERS))
        if lista and lista.status == pb.OK:
            self.servidores = {s.name: s for s in lista.servers}
            nomes = ", ".join(f"{s.name}(rank {s.rank})" for s in lista.servers)
            print(f"[{NAME}] servidores disponíveis: {nomes}", flush=True)
        return self.servidores

    def heartbeat(self):
        """Avisa a referência que continua funcionando."""
        resp = self.referencia(self.msgr.build(pb.HEARTBEAT, address=ADDRESS))
        if resp is None:
            return
        if resp.status != pb.OK:  # a referência não conhece este servidor (reiniciou?)
            self.registrar()
        self.atualizar_servidores()

    # ---------- coordenador: eleição (algoritmo do valentão) e relógio ----------

    def eleicao(self):
        """Pede eleição aos servidores de rank maior. Se algum responder OK, ele assume a
        eleição e vai anunciar o coordenador; se nenhum responder, este servidor vence."""
        self.precisa_eleicao = False
        print(f"[{NAME}] iniciando eleição (rank {self.rank})", flush=True)
        self.atualizar_servidores()
        superiores = sorted((s for s in self.servidores.values() if s.rank > self.rank),
                            key=lambda s: -s.rank)
        for s in superiores:
            resp = self.pedir(s.address, self.msgr.build(pb.ELECTION, rank=self.rank), TIMEOUT_SERVIDOR)
            if resp and resp.status == pb.OK:
                print(f"[{NAME}] {s.name} (rank {s.rank}) respondeu OK: aguardando o anúncio", flush=True)
                self.coordinator = None
                self.proxima_eleicao = time.time() + ESPERA_ANUNCIO
                return
        self.virar_coordenador()

    def virar_coordenador(self):
        self.coordinator = NAME
        anuncio = self.msgr.build(pb.COORDINATOR, coordinator=NAME, rank=self.rank)
        self.pub.send_multipart([TOPICO_SERVIDORES.encode(), encode(anuncio)])
        self.msgr.show("ENVIA", anuncio, f"#{TOPICO_SERVIDORES}")
        print(f"[{NAME}] eleito coordenador", flush=True)

    def sincronizar_relogio(self):
        """Atualiza o relógio com a hora do coordenador (o coordenador é a referência)."""
        if self.coordinator is None:
            return
        if self.coordinator == NAME:
            print(f"[{NAME}] sou o coordenador: meu relógio é a referência", flush=True)
            return
        info = self.servidores.get(self.coordinator) or self.atualizar_servidores().get(self.coordinator)
        resp = None
        if info:
            t0 = self.relogio.raw()
            resp = self.pedir(info.address, self.msgr.build(pb.CLOCK), TIMEOUT_SERVIDOR)
            t1 = self.relogio.raw()
        if resp is None or resp.status != pb.OK:
            print(f"[{NAME}] coordenador {self.coordinator} indisponível: pedindo eleição", flush=True)
            self.coordinator = None
            self.precisa_eleicao = True
            return
        offset = self.relogio.sync(resp.time, t0, t1)
        print(f"[{NAME}] relógio ajustado pelo coordenador {self.coordinator}: correção de {offset:+.3f}s", flush=True)

    def atender_peer(self):
        """Pedidos de outros servidores: eleição e hora do coordenador."""
        dados = self.peer.recv()
        try:
            env = self.msgr.parse(dados)
        except Exception:
            self.peer.send(encode(self.erro(pb.UNKNOWN, "mensagem inválida")))
            return
        self.msgr.show("RECEBE", env, env.sender)
        if env.type == pb.ELECTION:
            resp = self.msgr.build(env.type, status=pb.OK, rank=self.rank)
            self.precisa_eleicao = True  # quem responde OK assume a eleição (feita no laço principal)
        elif env.type == pb.SYNC:
            resp = self.msgr.build(env.type, status=pb.OK,
                                   entries=[entry_to_proto(e) for e in self.store.entries])
        elif env.type == pb.CLOCK:
            if self.coordinator == NAME:
                resp = self.msgr.build(env.type, status=pb.OK, time=self.relogio.now())
            else:
                resp = self.erro(env.type, "este servidor não é o coordenador")
        else:
            resp = self.erro(env.type, "tipo de mensagem desconhecido")
        self.peer.send(encode(resp))
        self.msgr.show("ENVIA", resp, env.sender)

    def receber_do_sub(self):
        """Mensagens dos tópicos "servers" (novo coordenador) e "replica" (novas entradas)."""
        topico, dados = self.sub.recv_multipart()
        topico = topico.decode()
        if topico not in (TOPICO_SERVIDORES, TOPICO_REPLICA):  # filtro exato do tópico
            return
        env = self.msgr.parse(dados)
        if env.sender == NAME:  # o servidor também recebe o que ele mesmo publica
            return
        self.msgr.show("RECEBE", env, f"#{topico}")
        if topico == TOPICO_REPLICA:
            self.aplicar_entradas(env.entries, env.sender)
            return
        self.coordinator = env.coordinator
        print(f"[{NAME}] coordenador agora é {self.coordinator}", flush=True)
        if self.rank is not None and env.rank < self.rank:
            self.precisa_eleicao = True  # eu tenho rank maior: devo assumir

    # ---------- replicação dos dados (consistência eventual) ----------

    def gravar(self, entrada):
        """Grava uma escrita feita por um cliente e a envia (push) para os outros servidores."""
        if self.store.add(entrada):
            self.replicar([entrada])

    def replicar(self, entradas):
        """Push: publica as entradas novas no tópico "replica"."""
        msg = self.msgr.build(pb.REPLICATE, entries=[entry_to_proto(e) for e in entradas])
        self.pub.send_multipart([TOPICO_REPLICA.encode(), encode(msg)])
        self.msgr.show("ENVIA", msg, f"#{TOPICO_REPLICA}")

    def aplicar_entradas(self, entradas, origem):
        """Grava as entradas recebidas de outro servidor. O id evita duplicar o que já temos."""
        novas = sum(1 for le in entradas if self.store.add(entry_from_proto(le)))
        if novas:
            print(f"[{NAME}] replicação: {novas} entrada(s) nova(s) recebida(s) de {origem}", flush=True)
        return novas

    def puxar_de(self, info):
        """Pull: pede todas as entradas a outro servidor e grava as que faltam."""
        resp = self.pedir(info.address, self.msgr.build(pb.SYNC), TIMEOUT_SYNC)
        if resp and resp.status == pb.OK:
            self.aplicar_entradas(resp.entries, info.name)
            return True
        return False

    def anti_entropia(self, todos=False):
        """Recupera o que o push perdeu: ao iniciar (de todos os servidores) e de tempos em
        tempos (de um servidor sorteado), pois o Pub/Sub pode perder mensagens."""
        self.proxima_sync = time.time() + ANTI_ENTROPIA
        outros = [s for s in self.atualizar_servidores().values() if s.name != NAME]
        if not todos:
            outros = random.sample(outros, 1) if outros else []
        for info in outros:
            self.puxar_de(info)

    # ---------- tratamento das requisições dos clientes ----------

    def erro(self, type_, texto):
        return self.msgr.build(type_, status=pb.ERROR, error=texto)

    def evento(self, kind, **campos):
        return self.store.new_entry(kind, ts=self.relogio.now(), clock=self.msgr.clock, **campos)

    def tratar(self, env):
        if env.type == pb.LOGIN:
            if not NAME_RE.match(env.user):
                return self.erro(env.type, "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)")
            self.gravar(self.evento("login", user=env.user))
            return self.msgr.build(env.type, status=pb.OK, user=env.user)

        if env.type == pb.CREATE_CHANNEL:
            if not NAME_RE.match(env.channel):
                return self.erro(env.type, "nome de canal inválido (use 1 a 32 letras, números, _ ou -)")
            if env.channel in RESERVADOS:
                return self.erro(env.type, f"'{env.channel}' é um nome reservado")
            if self.store.has_channel(env.channel):
                return self.erro(env.type, f"o canal '{env.channel}' já existe")
            self.gravar(self.evento("channel", user=env.sender, channel=env.channel))
            return self.msgr.build(env.type, status=pb.OK, channel=env.channel)

        if env.type == pb.PUBLISH:
            # o canal pode ter sido criado em outro servidor e a replicação é eventual (ainda
            # pode não ter chegado), então só validamos o nome; o canal é o tópico da publicação
            if not NAME_RE.match(env.channel):
                return self.erro(env.type, "nome de canal inválido")
            if env.channel in RESERVADOS:
                return self.erro(env.type, f"'{env.channel}' é um nome reservado")
            if not env.text:
                return self.erro(env.type, "mensagem vazia")
            publicacao = self.msgr.build(pb.PUBLICATION, user=env.sender, channel=env.channel, text=env.text)
            self.gravar(self.store.new_entry("message", ts=publicacao.timestamp, clock=publicacao.clock,
                                             user=env.sender, channel=env.channel, text=env.text))
            # o nome do canal é o tópico (primeiro frame); o segundo frame é o Envelope
            self.pub.send_multipart([env.channel.encode(), encode(publicacao)])
            self.msgr.show("ENVIA", publicacao, f"#{env.channel}")
            return self.msgr.build(env.type, status=pb.OK, channel=env.channel)

        if env.type == pb.LIST_CHANNELS:
            return self.msgr.build(env.type, status=pb.OK, channels=self.store.channel_names())

        if env.type == pb.HISTORY:
            if not NAME_RE.match(env.channel):
                return self.erro(env.type, "nome de canal inválido")
            msgs = self.store.messages(env.channel)
            return self.msgr.build(env.type, status=pb.OK, channel=env.channel, total=len(msgs),
                                   entries=[entry_to_proto(e) for e in msgs[-HISTORICO:]])

        return self.erro(env.type, "tipo de mensagem desconhecido")

    def atender_cliente(self):
        dados = self.rep.recv()
        try:
            env = self.msgr.parse(dados)
        except Exception:
            resp = self.erro(pb.UNKNOWN, "mensagem inválida (não é um Envelope Protobuf)")
            self.rep.send(encode(resp))
            self.msgr.show("ENVIA", resp)
            return
        self.msgr.show("RECEBE", env, env.sender)
        resp = self.tratar(env)
        self.rep.send(encode(resp))
        self.msgr.show("ENVIA", resp, env.sender)
        self.n_msgs += 1
        if self.n_msgs % HEARTBEAT_A_CADA == 0:
            self.heartbeat()
        if self.n_msgs % SINCRONIZA_A_CADA == 0:
            self.sincronizar_relogio()

    # ---------- laço principal ----------

    def run(self):
        print(f"[{NAME}] iniciado. {len(self.store.channels)} canais carregados de {self.store.path}", flush=True)
        self.registrar()
        self.anti_entropia(todos=True)  # entra no sistema já com os dados dos outros servidores
        while True:
            eventos = dict(self.poller.poll(1000))
            if self.rep in eventos:
                self.atender_cliente()
            if self.peer in eventos:
                self.atender_peer()
            if self.sub in eventos:
                self.receber_do_sub()
            if time.time() >= self.proxima_sync:
                self.anti_entropia()
            # sem coordenador (início, ou o coordenador caiu) ou alguém pediu eleição
            if self.precisa_eleicao or (self.coordinator is None and time.time() >= self.proxima_eleicao):
                self.eleicao()


if __name__ == "__main__":
    Servidor().run()
