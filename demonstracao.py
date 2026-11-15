"""Cliente manual opcional para demonstrar o BBS existente, sem alterar os bots."""
import argparse
import time

import zmq

import chat_pb2 as pb
from client import Bot
from common import hora


class ClienteManual(Bot):
    def requisitar(self, env):
        # Sem repeticao automatica: uma publicacao pode ter sido aceita antes
        # de uma resposta se perder. Evita duplicar a mensagem sem avisar.
        self.req.send(env.SerializeToString())
        if not self.req.poll(10000):
            self._conectar()
            raise TimeoutError("Sem resposta em 10 s. Confira os servidores; a operacao pode ter sido aceita.")
        return self.msgr.parse(self.req.recv())


def conferir(resp):
    if resp.status != pb.OK:
        raise RuntimeError(resp.error or "Servidor recusou a operacao")
    return resp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("modo", choices=["receber", "enviar"])
    parser.add_argument("--nome", default="gabriel")
    parser.add_argument("--canal", default="apresentacao")
    parser.add_argument("--todos", action="store_true", help="Receber publicacoes de todos os canais")
    args = parser.parse_args()
    bot = ClienteManual()
    bot.name = args.nome
    bot.msgr.name = args.nome
    try:
        resp = conferir(bot.requisitar(bot.msgr.build(pb.LOGIN, user=bot.name)))
        print(f"Login OK: {bot.name} | servidor: {resp.sender}", flush=True)
        resp = conferir(bot.requisitar(bot.msgr.build(pb.LIST_CHANNELS)))
        if args.canal not in resp.channels:
            resp = bot.requisitar(bot.msgr.build(pb.CREATE_CHANNEL, channel=args.canal))
            # Outro cliente pode ter criado o mesmo canal simultaneamente.
            if resp.status != pb.OK:
                canais = conferir(bot.requisitar(bot.msgr.build(pb.LIST_CHANNELS)))
                if args.canal not in canais.channels:
                    conferir(resp)

        if args.modo == "receber":
            if args.todos:
                # Assina todos os topicos, exibindo somente PUBLICATION abaixo.
                bot.sub.setsockopt(zmq.SUBSCRIBE, b"")
            else:
                bot.inscrever(args.canal)
            time.sleep(1)
            print("PRONTO PARA RECEBER em todos os canais. Ctrl+C encerra." if args.todos else f"PRONTO PARA RECEBER no canal #{args.canal}. Ctrl+C encerra.", flush=True)
            while True:
                if not bot.sub.poll(500):
                    continue
                frames = bot.sub.recv_multipart()
                if len(frames) != 2:
                    continue
                canal = frames[0].decode()
                if not args.todos and canal != args.canal:
                    continue
                env = bot.msgr.parse(frames[1])
                if env.type != pb.PUBLICATION:
                    continue
                print(f"\nRECEBIDO | {env.user or env.sender} -> #{canal}: {env.text}", flush=True)
                print(f"Servidor: {env.sender} | envio: {env.timestamp:.3f} (Unix) | recebimento: {hora()} | L={bot.msgr.clock}", flush=True)
        else:
            print(f"Canal #{args.canal}. Digite uma mensagem e Enter.", flush=True)
            print("/historico mostra as ultimas mensagens; /sair encerra.", flush=True)
            while True:
                texto = input(f"{bot.name}> ").strip()
                if texto == "/sair":
                    break
                if not texto:
                    continue
                try:
                    if texto == "/historico":
                        resp = conferir(bot.requisitar(bot.msgr.build(pb.HISTORY, channel=args.canal)))
                        print(f"Historico respondido por {resp.sender}: {resp.total} mensagens")
                        for e in resp.entries:
                            print(f"  {e.user}: {e.text} | origem: {e.origin} | L={e.clock}")
                    else:
                        resp = conferir(bot.requisitar(bot.msgr.build(pb.PUBLISH, channel=args.canal, text=texto)))
                        print(f"Servidor {resp.sender} confirmou OK | L={bot.msgr.clock}")
                except (TimeoutError, RuntimeError) as exc:
                    print(f"AVISO: {exc}", flush=True)
    except (KeyboardInterrupt, EOFError):
        print("\nDemonstracao encerrada.", flush=True)
    except (TimeoutError, RuntimeError) as exc:
        print(f"ERRO: {exc}", flush=True)
        raise SystemExit(1)
    finally:
        bot.req.close(linger=0)
        bot.sub.close(linger=0)
        bot.ctx.term()


if __name__ == "__main__":
    main()
