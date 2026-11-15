"""Persistência em disco: log de eventos em JSON Lines (um evento por linha).

Cada servidor tem o seu próprio arquivo (/data/<nome>.jsonl). Todo evento
(login, criação de canal e mensagens) é acrescentado
ao final do arquivo. Ao iniciar, o arquivo é lido de novo e o estado é
reconstruído a partir dos eventos gravados.
"""
import json
import os
import re
import time
import uuid

NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


class Store:
    def __init__(self, data_dir, server_name):
        os.makedirs(data_dir, exist_ok=True)
        self.path = os.path.join(data_dir, f"{server_name}.jsonl")
        self.server_name = server_name
        self.entries = []
        self.ids = set()
        self.channels = {}  # nome do canal -> entrada que o criou
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        self._apply(json.loads(line))
        self.file = open(self.path, "a", encoding="utf-8")

    def _apply(self, e):
        self.entries.append(e)
        self.ids.add(e["id"])
        if e["kind"] == "channel" and e["channel"] not in self.channels:
            self.channels[e["channel"]] = e

    def new_entry(self, kind, ts=None, **fields):
        e = {"id": uuid.uuid4().hex, "origin": self.server_name, "kind": kind,
             "ts": ts if ts is not None else time.time(),
             "clock": 0, "user": "", "channel": "", "text": ""}
        e.update(fields)
        return e

    def add(self, e):
        """Grava o evento. Retorna False se ele já existia (mesmo id)."""
        if e["id"] in self.ids:
            return False
        self._apply(e)
        self.file.write(json.dumps(e, ensure_ascii=False) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())
        return True

    def messages(self, channel):
        """Mensagens do canal na mesma ordem em todos os servidores: pelo relógio
        lógico, e em caso de empate pelo servidor de origem e pelo id."""
        msgs = [e for e in self.entries if e["kind"] == "message" and e["channel"] == channel]
        return sorted(msgs, key=lambda e: (e.get("clock", 0), e["origin"], e["id"]))

    def has_channel(self, name):
        return name in self.channels

    def channel_names(self):
        return sorted(self.channels)
