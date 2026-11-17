import time

from google.protobuf import text_format

import chat_pb2 as pb


def codificar(envelope):
    return envelope.SerializeToString()


def decodificar(dados):
    envelope = pb.Envelope()
    envelope.ParseFromString(dados)
    return envelope


def entrada_para_proto(e):
    return pb.LogEntry(id=e["id"], origem=e["origin"], tipo=e["kind"], instante=e["ts"],
                         relogio=e.get("clock", 0), usuario=e["user"], canal=e["channel"], texto=e["text"])


def entrada_de_proto(le):
    return {"id": le.id, "origin": le.origem, "kind": le.tipo, "ts": le.instante, "clock": le.relogio,
            "user": le.usuario, "channel": le.canal, "text": le.texto}


def hora():
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + f".{int((t % 1) * 1000):03d}"


class RelogioFisico:
    def __init__(self, desvio=0.0):
        self.desvio = desvio
        self.correcao = 0.0

    def bruto(self):
        return time.time() + self.desvio

    def agora(self):
        return self.bruto() + self.correcao

    def sincronizar(self, hora_remota, t0, t1):
        estimada = hora_remota + (t1 - t0) / 2
        self.correcao = estimada - t1
        return self.correcao


class Mensageiro:
    def __init__(self, nome, fisico=None):
        self.nome = nome
        self.fisico = fisico or RelogioFisico()
        self.relogio = 0

    def montar(self, tipo, **campos):
        self.relogio += 1
        return pb.Envelope(tipo=tipo, remetente=self.nome, instante=self.fisico.agora(),
                           relogio=self.relogio, **campos)

    def ler(self, dados):
        envelope = decodificar(dados)
        self.relogio = max(self.relogio, envelope.relogio)
        return envelope

    def exibir(self, direcao, envelope, outro="", extra=""):
        corpo = text_format.MessageToString(envelope, as_one_line=True)
        seta = "->" if direcao == "ENVIA" else "<-"
        print(f"{hora()} [{self.nome}|L={self.relogio}] {direcao:<7} {seta} {outro:<14} {corpo}{extra}", flush=True)
