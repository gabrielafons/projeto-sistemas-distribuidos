"""Funções comuns a todos os processos em Python: relógios, serialização e exibição."""
import time

from google.protobuf import text_format

import chat_pb2 as pb


def encode(env):
    """Serializa o Envelope em binário (Protobuf)."""
    return env.SerializeToString()


def decode(data):
    """Reconstrói o Envelope a partir dos bytes recebidos."""
    env = pb.Envelope()
    env.ParseFromString(data)
    return env


def entry_to_proto(e):
    """Entrada do log (dict) -> LogEntry (Protobuf)."""
    return pb.LogEntry(id=e["id"], origin=e["origin"], kind=e["kind"], timestamp=e["ts"],
                       clock=e.get("clock", 0), user=e["user"], channel=e["channel"], text=e["text"])


def entry_from_proto(le):
    """LogEntry (Protobuf) -> entrada do log (dict)."""
    return {"id": le.id, "origin": le.origin, "kind": le.kind, "ts": le.timestamp, "clock": le.clock,
            "user": le.user, "channel": le.channel, "text": le.text}


def hora():
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + f".{int((t % 1) * 1000):03d}"


class PhysicalClock:
    """Relógio físico do processo.

    `skew` simula um relógio que anda errado (variável CLOCK_SKEW, só para testes) e
    `offset` é a correção aplicada pela sincronização com o serviço de referência.
    """

    def __init__(self, skew=0.0):
        self.skew = skew
        self.offset = 0.0

    def raw(self):
        """Hora do relógio local, sem correção."""
        return time.time() + self.skew

    def now(self):
        """Hora corrigida, usada nos timestamps das mensagens."""
        return self.raw() + self.offset

    def sync(self, hora_remota, t0, t1):
        """Ajusta o relógio com a hora informada por outro processo.

        t0 e t1 são as horas locais (raw) do envio do pedido e do recebimento da
        resposta. A hora remota chegou depois de metade do tempo de ida e volta.
        """
        estimada = hora_remota + (t1 - t0) / 2
        self.offset = estimada - t1
        return self.offset


class Messenger:
    """Cria, lê e exibe as mensagens de um processo e mantém o seu relógio lógico."""

    def __init__(self, name, physical=None):
        self.name = name
        self.physical = physical or PhysicalClock()
        self.clock = 0  # relógio lógico (Lamport)

    def build(self, type_, **fields):
        """Monta um Envelope para envio: incrementa o relógio lógico e coloca o
        remetente, o timestamp (relógio físico) e o valor do contador."""
        self.clock += 1
        return pb.Envelope(type=type_, sender=self.name, timestamp=self.physical.now(),
                           clock=self.clock, **fields)

    def parse(self, data):
        """Lê a mensagem recebida e atualiza o relógio lógico: max(local, recebido)."""
        env = decode(data)
        self.clock = max(self.clock, env.clock)
        return env

    def show(self, direction, env, other="", extra=""):
        """Exibe todo o conteúdo da mensagem enviada/recebida."""
        corpo = text_format.MessageToString(env, as_one_line=True)
        seta = "->" if direction == "ENVIA" else "<-"
        print(f"{hora()} [{self.name}|L={self.clock}] {direction:<7} {seta} {other:<14} {corpo}{extra}", flush=True)
