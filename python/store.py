import json
import os
import re
import time
import uuid

NOME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


class Store:
    def __init__(self, pasta_dados, nome_servidor):
        os.makedirs(pasta_dados, exist_ok=True)
        self.caminho = os.path.join(pasta_dados, f"{nome_servidor}.jsonl")
        self.nome_servidor = nome_servidor
        self.entradas = []
        self.ids = set()
        self.canais = {}
        if os.path.exists(self.caminho):
            with open(self.caminho, encoding="utf-8") as arquivo:
                for linha in arquivo:
                    linha = linha.strip()
                    if linha:
                        self._aplicar(json.loads(linha))
        self.arquivo = open(self.caminho, "a", encoding="utf-8")

    def _aplicar(self, e):
        self.entradas.append(e)
        self.ids.add(e["id"])
        if e["kind"] == "channel" and e["channel"] not in self.canais:
            self.canais[e["channel"]] = e

    def nova_entrada(self, tipo, ts=None, **campos):
        e = {"id": uuid.uuid4().hex, "origin": self.nome_servidor, "kind": tipo,
             "ts": ts if ts is not None else time.time(),
             "clock": 0, "user": "", "channel": "", "text": ""}
        e.update(campos)
        return e

    def adicionar(self, e):
        if e["id"] in self.ids:
            return False
        self._aplicar(e)
        self.arquivo.write(json.dumps(e, ensure_ascii=False) + "\n")
        self.arquivo.flush()
        os.fsync(self.arquivo.fileno())
        return True

    def tem_canal(self, nome):
        return nome in self.canais

    def nomes_canais(self):
        return sorted(self.canais)
