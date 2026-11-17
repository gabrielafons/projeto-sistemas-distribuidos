import os
import random
import time

import zmq

import chat_pb2 as pb
from common import Mensageiro, RelogioFisico, codificar, entrada_de_proto, entrada_para_proto
from store import Armazenamento, NOME_RE

NOME = os.environ.get("SERVER_NAME", "homem-de-ferro-python-1")
BROKER_HOST = os.environ.get("BROKER_HOST", "broker")
PROXY_HOST = os.environ.get("PROXY_HOST", "proxy")
REF_HOST = os.environ.get("REF_HOST", "reference")
DATA_DIR = os.environ.get("DATA_DIR", "/data")
PEER_PORT = os.environ.get("PEER_PORT", "6000")
ENDERECO = os.environ.get("ADVERTISE_ADDR", f"tcp://{NOME}:{PEER_PORT}")
DESVIO_RELOGIO = float(os.environ.get("CLOCK_SKEW", "0"))

HEARTBEAT_A_CADA = 10
SINCRONIZA_A_CADA = 15
TIMEOUT_REQ = 2.0
TIMEOUT_SERVIDOR = 1.0
ESPERA_ANUNCIO = 5.0
ANTI_ENTROPIA = 20.0
TIMEOUT_SYNC = 3.0

TOPICO_SERVIDORES = "servers"
TOPICO_REPLICA = "replica"
RESERVADOS = {TOPICO_SERVIDORES, TOPICO_REPLICA}


class Servidor:
    def __init__(self):
        self.mensageiro = Mensageiro(NOME, RelogioFisico(DESVIO_RELOGIO))
        self.relogio = self.mensageiro.fisico
        self.armazenamento = Armazenamento(DATA_DIR, NOME)
        self.rank = None
        self.mensagens_recebidas = 0
        self.coordenador = None
        self.servidores = {}
        self.precisa_eleicao = False
        self.proxima_eleicao = 0.0
        self.proxima_sync = time.time() + ANTI_ENTROPIA

        self.contexto = zmq.Context()
        self.atendimento = self.contexto.socket(zmq.REP)
        self.atendimento.connect(f"tcp://{BROKER_HOST}:5556")
        self.par = self.contexto.socket(zmq.REP)
        self.par.bind(f"tcp://*:{PEER_PORT}")
        self.publicador = self.contexto.socket(zmq.PUB)
        self.publicador.connect(f"tcp://{PROXY_HOST}:5557")
        self.assinante = self.contexto.socket(zmq.SUB)
        self.assinante.connect(f"tcp://{PROXY_HOST}:5558")
        self.assinante.setsockopt(zmq.SUBSCRIBE, TOPICO_SERVIDORES.encode())
        self.assinante.setsockopt(zmq.SUBSCRIBE, TOPICO_REPLICA.encode())

        self.sondador = zmq.Poller()
        for socket in (self.atendimento, self.par, self.assinante):
            self.sondador.register(socket, zmq.POLLIN)

    def pedir(self, endereco, envelope, timeout=TIMEOUT_REQ):
        requisicao = self.contexto.socket(zmq.REQ)
        requisicao.setsockopt(zmq.LINGER, 0)
        requisicao.connect(endereco)
        espera = zmq.Poller()
        espera.register(requisicao, zmq.POLLIN)
        espera.register(self.par, zmq.POLLIN)
        try:
            requisicao.send(codificar(envelope))
            self.mensageiro.exibir("ENVIA", envelope, endereco.split("//")[1])
            fim = time.time() + timeout
            while True:
                resto = fim - time.time()
                if resto <= 0:
                    print(f"[{NOME}] sem resposta de {endereco} em {timeout}s", flush=True)
                    return None
                eventos = dict(espera.poll(int(resto * 1000) + 1))
                if self.par in eventos:
                    self.atender_par()
                if requisicao in eventos:
                    resposta = self.mensageiro.ler(requisicao.recv())
                    self.mensageiro.exibir("RECEBE", resposta, resposta.remetente)
                    return resposta
        finally:
            requisicao.close()

    def referencia(self, envelope):
        return self.pedir(f"tcp://{REF_HOST}:5559", envelope)

    def registrar(self):
        while True:
            resposta = self.referencia(self.mensageiro.montar(pb.OBTER_RANK, endereco=ENDERECO))
            if resposta and resposta.status == pb.OK:
                self.rank = resposta.rank
                print(f"[{NOME}] rank {self.rank}, endereço {ENDERECO}", flush=True)
                return
            time.sleep(2)

    def atualizar_servidores(self):
        lista = self.referencia(self.mensageiro.montar(pb.LISTAR_SERVIDORES))
        if lista and lista.status == pb.OK:
            self.servidores = {s.nome: s for s in lista.servidores}
            nomes = ", ".join(f"{s.nome}(rank {s.rank})" for s in lista.servidores)
            print(f"[{NOME}] servidores disponíveis: {nomes}", flush=True)
        return self.servidores

    def batimento(self):
        resposta = self.referencia(self.mensageiro.montar(pb.BATIMENTO, endereco=ENDERECO))
        if resposta is None:
            return
        if resposta.status != pb.OK:
            self.registrar()
        self.atualizar_servidores()

    def eleicao(self):
        self.precisa_eleicao = False
        print(f"[{NOME}] iniciando eleição (rank {self.rank})", flush=True)
        self.atualizar_servidores()
        superiores = sorted((s for s in self.servidores.values() if s.rank > self.rank),
                            key=lambda s: -s.rank)
        for s in superiores:
            resposta = self.pedir(s.endereco, self.mensageiro.montar(pb.ELEICAO, rank=self.rank), TIMEOUT_SERVIDOR)
            if resposta and resposta.status == pb.OK:
                print(f"[{NOME}] {s.nome} (rank {s.rank}) respondeu OK: aguardando o anúncio", flush=True)
                self.coordenador = None
                self.proxima_eleicao = time.time() + ESPERA_ANUNCIO
                return
        self.virar_coordenador()

    def virar_coordenador(self):
        self.coordenador = NOME
        anuncio = self.mensageiro.montar(pb.COORDENADOR, coordenador=NOME, rank=self.rank)
        self.publicador.send_multipart([TOPICO_SERVIDORES.encode(), codificar(anuncio)])
        self.mensageiro.exibir("ENVIA", anuncio, f"#{TOPICO_SERVIDORES}")
        print(f"[{NOME}] eleito coordenador", flush=True)

    def sincronizar_relogio(self):
        if self.coordenador is None:
            return
        if self.coordenador == NOME:
            print(f"[{NOME}] sou o coordenador: meu relógio é a referência", flush=True)
            return
        info = self.servidores.get(self.coordenador) or self.atualizar_servidores().get(self.coordenador)
        resposta = None
        if info:
            t0 = self.relogio.bruto()
            resposta = self.pedir(info.endereco, self.mensageiro.montar(pb.RELOGIO), TIMEOUT_SERVIDOR)
            t1 = self.relogio.bruto()
        if resposta is None or resposta.status != pb.OK:
            print(f"[{NOME}] coordenador {self.coordenador} indisponível: pedindo eleição", flush=True)
            self.coordenador = None
            self.precisa_eleicao = True
            return
        correcao = self.relogio.sincronizar(resposta.hora, t0, t1)
        print(f"[{NOME}] relógio ajustado pelo coordenador {self.coordenador}: correção de {correcao:+.3f}s", flush=True)

    def atender_par(self):
        dados = self.par.recv()
        try:
            envelope = self.mensageiro.ler(dados)
        except Exception:
            self.par.send(codificar(self.erro(pb.DESCONHECIDO, "mensagem inválida")))
            return
        self.mensageiro.exibir("RECEBE", envelope, envelope.remetente)
        if envelope.tipo == pb.ELEICAO:
            resposta = self.mensageiro.montar(envelope.tipo, status=pb.OK, rank=self.rank)
            self.precisa_eleicao = True
        elif envelope.tipo == pb.SINCRONIZAR:
            resposta = self.mensageiro.montar(envelope.tipo, status=pb.OK,
                                              entradas=[entrada_para_proto(e) for e in self.armazenamento.entradas])
        elif envelope.tipo == pb.RELOGIO:
            if self.coordenador == NOME:
                resposta = self.mensageiro.montar(envelope.tipo, status=pb.OK, hora=self.relogio.agora())
            else:
                resposta = self.erro(envelope.tipo, "este servidor não é o coordenador")
        else:
            resposta = self.erro(envelope.tipo, "tipo de mensagem desconhecido")
        self.par.send(codificar(resposta))
        self.mensageiro.exibir("ENVIA", resposta, envelope.remetente)

    def receber_do_sub(self):
        topico, dados = self.assinante.recv_multipart()
        topico = topico.decode()
        if topico not in (TOPICO_SERVIDORES, TOPICO_REPLICA):
            return
        envelope = self.mensageiro.ler(dados)
        if envelope.remetente == NOME:
            return
        self.mensageiro.exibir("RECEBE", envelope, f"#{topico}")
        if topico == TOPICO_REPLICA:
            self.aplicar_entradas(envelope.entradas, envelope.remetente)
            return
        self.coordenador = envelope.coordenador
        print(f"[{NOME}] coordenador agora é {self.coordenador}", flush=True)
        if self.rank is not None and envelope.rank < self.rank:
            self.precisa_eleicao = True

    def gravar(self, entrada):
        if self.armazenamento.adicionar(entrada):
            self.replicar([entrada])

    def replicar(self, entradas):
        mensagem = self.mensageiro.montar(pb.REPLICAR, entradas=[entrada_para_proto(e) for e in entradas])
        self.publicador.send_multipart([TOPICO_REPLICA.encode(), codificar(mensagem)])
        self.mensageiro.exibir("ENVIA", mensagem, f"#{TOPICO_REPLICA}")

    def aplicar_entradas(self, entradas, origem):
        novas = sum(1 for le in entradas if self.armazenamento.adicionar(entrada_de_proto(le)))
        if novas:
            print(f"[{NOME}] replicação: {novas} entrada(s) nova(s) recebida(s) de {origem}", flush=True)
        return novas

    def puxar_de(self, info):
        resposta = self.pedir(info.endereco, self.mensageiro.montar(pb.SINCRONIZAR), TIMEOUT_SYNC)
        if resposta and resposta.status == pb.OK:
            self.aplicar_entradas(resposta.entradas, info.nome)
            return True
        return False

    def anti_entropia(self, todos=False):
        self.proxima_sync = time.time() + ANTI_ENTROPIA
        outros = [s for s in self.atualizar_servidores().values() if s.nome != NOME]
        if not todos:
            outros = random.sample(outros, 1) if outros else []
        for info in outros:
            self.puxar_de(info)

    def erro(self, tipo, texto):
        return self.mensageiro.montar(tipo, status=pb.ERRO, erro=texto)

    def evento(self, tipo, **campos):
        return self.armazenamento.nova_entrada(tipo, ts=self.relogio.agora(), clock=self.mensageiro.relogio, **campos)

    def tratar(self, envelope):
        if envelope.tipo == pb.LOGIN:
            if not NOME_RE.match(envelope.usuario):
                return self.erro(envelope.tipo, "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)")
            self.gravar(self.evento("login", user=envelope.usuario))
            return self.mensageiro.montar(envelope.tipo, status=pb.OK, usuario=envelope.usuario)

        if envelope.tipo == pb.CRIAR_CANAL:
            if not NOME_RE.match(envelope.canal):
                return self.erro(envelope.tipo, "nome de canal inválido (use 1 a 32 letras, números, _ ou -)")
            if envelope.canal in RESERVADOS:
                return self.erro(envelope.tipo, f"'{envelope.canal}' é um nome reservado")
            if self.armazenamento.tem_canal(envelope.canal):
                return self.erro(envelope.tipo, f"o canal '{envelope.canal}' já existe")
            self.gravar(self.evento("channel", user=envelope.remetente, channel=envelope.canal))
            return self.mensageiro.montar(envelope.tipo, status=pb.OK, canal=envelope.canal)

        if envelope.tipo == pb.PUBLICAR:
            if not NOME_RE.match(envelope.canal):
                return self.erro(envelope.tipo, "nome de canal inválido")
            if envelope.canal in RESERVADOS:
                return self.erro(envelope.tipo, f"'{envelope.canal}' é um nome reservado")
            if not envelope.texto:
                return self.erro(envelope.tipo, "mensagem vazia")
            publicacao = self.mensageiro.montar(pb.PUBLICACAO, usuario=envelope.remetente, canal=envelope.canal,
                                                texto=envelope.texto)
            self.gravar(self.armazenamento.nova_entrada("message", ts=publicacao.instante, clock=publicacao.relogio,
                                                        user=envelope.remetente, channel=envelope.canal,
                                                        text=envelope.texto))
            self.publicador.send_multipart([envelope.canal.encode(), codificar(publicacao)])
            self.mensageiro.exibir("ENVIA", publicacao, f"#{envelope.canal}")
            return self.mensageiro.montar(envelope.tipo, status=pb.OK, canal=envelope.canal)

        if envelope.tipo == pb.LISTAR_CANAIS:
            return self.mensageiro.montar(envelope.tipo, status=pb.OK, canais=self.armazenamento.nomes_canais())

        return self.erro(envelope.tipo, "tipo de mensagem desconhecido")

    def atender_cliente(self):
        dados = self.atendimento.recv()
        try:
            envelope = self.mensageiro.ler(dados)
        except Exception:
            resposta = self.erro(pb.DESCONHECIDO, "mensagem inválida (não é um Envelope Protobuf)")
            self.atendimento.send(codificar(resposta))
            self.mensageiro.exibir("ENVIA", resposta)
            return
        self.mensageiro.exibir("RECEBE", envelope, envelope.remetente)
        resposta = self.tratar(envelope)
        self.atendimento.send(codificar(resposta))
        self.mensageiro.exibir("ENVIA", resposta, envelope.remetente)
        self.mensagens_recebidas += 1
        if self.mensagens_recebidas % HEARTBEAT_A_CADA == 0:
            self.batimento()
        if self.mensagens_recebidas % SINCRONIZA_A_CADA == 0:
            self.sincronizar_relogio()

    def executar(self):
        print(f"[{NOME}] iniciado. {len(self.armazenamento.canais)} canais carregados de {self.armazenamento.caminho}", flush=True)
        self.registrar()
        self.anti_entropia(todos=True)
        while True:
            eventos = dict(self.sondador.poll(1000))
            if self.atendimento in eventos:
                self.atender_cliente()
            if self.par in eventos:
                self.atender_par()
            if self.assinante in eventos:
                self.receber_do_sub()
            if time.time() >= self.proxima_sync:
                self.anti_entropia()
            if self.precisa_eleicao or (self.coordenador is None and time.time() >= self.proxima_eleicao):
                self.eleicao()


if __name__ == "__main__":
    Servidor().executar()
