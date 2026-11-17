import argparse
import time

import zmq

import chat_pb2 as pb
from client import Robo
from common import hora


class ClienteManual(Robo):
    def requisitar(self, envelope):
        self.requisicao.send(envelope.SerializeToString())
        if not self.requisicao.poll(10000):
            self._conectar()
            raise TimeoutError("Sem resposta em 10 s. Confira os servidores; a operacao pode ter sido aceita.")
        return self.mensageiro.ler(self.requisicao.recv())


def conferir(resposta):
    if resposta.status != pb.OK:
        raise RuntimeError(resposta.erro or "Servidor recusou a operacao")
    return resposta


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("modo", choices=["receber", "enviar"])
    parser.add_argument("--nome", default="gabriel")
    parser.add_argument("--canal", default="apresentacao")
    parser.add_argument("--todos", action="store_true", help="Receber publicacoes de todos os canais")
    args = parser.parse_args()
    bot = ClienteManual()
    bot.nome = args.nome
    bot.mensageiro.nome = args.nome
    try:
        resposta = conferir(bot.requisitar(bot.mensageiro.montar(pb.LOGIN, usuario=bot.nome)))
        print(f"Login OK: {bot.nome} | servidor: {resposta.remetente}", flush=True)
        resposta = conferir(bot.requisitar(bot.mensageiro.montar(pb.LISTAR_CANAIS)))
        if args.canal not in resposta.canais:
            resposta = bot.requisitar(bot.mensageiro.montar(pb.CRIAR_CANAL, canal=args.canal))
            if resposta.status != pb.OK:
                canais = conferir(bot.requisitar(bot.mensageiro.montar(pb.LISTAR_CANAIS)))
                if args.canal not in canais.canais:
                    conferir(resposta)

        if args.modo == "receber":
            if args.todos:
                bot.assinante.setsockopt(zmq.SUBSCRIBE, b"")
            else:
                bot.inscrever(args.canal)
            time.sleep(1)
            print("PRONTO PARA RECEBER em todos os canais. Ctrl+C encerra." if args.todos else f"PRONTO PARA RECEBER no canal #{args.canal}. Ctrl+C encerra.", flush=True)
            while True:
                if not bot.assinante.poll(500):
                    continue
                quadros = bot.assinante.recv_multipart()
                if len(quadros) != 2:
                    continue
                canal = quadros[0].decode()
                if not args.todos and canal != args.canal:
                    continue
                envelope = bot.mensageiro.ler(quadros[1])
                if envelope.tipo != pb.PUBLICACAO:
                    continue
                print(f"\nRECEBIDO | {envelope.usuario or envelope.remetente} -> #{canal}: {envelope.texto}", flush=True)
                print(f"Servidor: {envelope.remetente} | envio: {envelope.instante:.3f} (Unix) | recebimento: {hora()} | L={bot.mensageiro.relogio}", flush=True)
        else:
            print(f"Canal #{args.canal}. Digite uma mensagem e Enter.", flush=True)
            print("/sair encerra.", flush=True)
            while True:
                texto = input(f"{bot.nome}> ").strip()
                if texto == "/sair":
                    break
                if not texto:
                    continue
                try:
                    resposta = conferir(bot.requisitar(bot.mensageiro.montar(pb.PUBLICAR, canal=args.canal, texto=texto)))
                    print(f"Servidor {resposta.remetente} confirmou OK | L={bot.mensageiro.relogio}")
                except (TimeoutError, RuntimeError) as exc:
                    print(f"AVISO: {exc}", flush=True)
    except (KeyboardInterrupt, EOFError):
        print("\nDemonstracao encerrada.", flush=True)
    except (TimeoutError, RuntimeError) as exc:
        print(f"ERRO: {exc}", flush=True)
        raise SystemExit(1)
    finally:
        bot.requisicao.close(linger=0)
        bot.assinante.close(linger=0)
        bot.contexto.term()


if __name__ == "__main__":
    main()
