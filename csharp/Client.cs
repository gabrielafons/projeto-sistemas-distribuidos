using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

namespace Bbs
{
    internal sealed class Client
    {
        private static readonly string BrokerHost = Util.Env("BROKER_HOST", "broker");
        private static readonly string ProxyHost = Util.Env("PROXY_HOST", "proxy");
        private const int TimeoutMs = 5000;

        private static readonly string[] Frases =
        {
            "bom dia", "alguém aí?", "olá, pessoal", "testando o canal", "mensagem aleatória",
            "tudo bem por aí?", "sistemas distribuídos são legais", "ping", "mais uma mensagem",
            "o que vocês acham?"
        };

        private readonly Random random = new Random();
        private readonly Messenger msgr;
        private readonly SubscriberSocket sub = new SubscriberSocket();
        private readonly HashSet<string> inscritos = new HashSet<string>();
        private RequestSocket req;
        private string nome;

        public Client()
        {
            string bot = Util.Env("BOT_NAME", "");
            nome = bot.Length > 0 ? bot : NomeAleatorio();
            msgr = new Messenger(nome, new PhysicalClock(0));
            sub.Connect("tcp://" + ProxyHost + ":5558");
            Conectar();
        }

        private string NomeAleatorio()
        {
            return "cs-bot-" + random.Next(1000, 10000);
        }

        private void Conectar()
        {
            if (req != null)
            {
                req.Options.Linger = TimeSpan.Zero;
                req.Dispose();
            }
            req = new RequestSocket();
            req.Options.Linger = TimeSpan.Zero;
            req.Connect("tcp://" + BrokerHost + ":5555");
        }

        private Envelope Requisitar(Envelope env)
        {
            while (true)
            {
                if (req.TrySendFrame(TimeSpan.FromMilliseconds(TimeoutMs), env.ToByteArray()))
                {
                    msgr.Show("ENVIA", env, "servidor");
                    if (req.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(TimeoutMs), out byte[] dados)
                        && msgr.TryParse(dados, out Envelope resp))
                    {
                        msgr.Show("RECEBE", resp, resp.Sender);
                        return resp;
                    }
                }
                Console.WriteLine($"[{nome}] sem resposta em {TimeoutMs} ms, tentando de novo");
                Conectar();
            }
        }

        private void Login()
        {
            while (true)
            {
                Envelope pedido = msgr.Build(Type.Login);
                pedido.User = nome;
                if (Requisitar(pedido).Status == Status.Ok)
                {
                    return;
                }
                Util.Dormir(1000);
                nome = NomeAleatorio();
                msgr.Name = nome;
            }
        }

        private List<string> ListarCanais()
        {
            Envelope resp = Requisitar(msgr.Build(Type.ListChannels));
            return resp.Status == Status.Ok ? resp.Channels.ToList() : new List<string>();
        }

        private bool CriarCanal(string canal)
        {
            Envelope pedido = msgr.Build(Type.CreateChannel);
            pedido.Channel = canal;
            return Requisitar(pedido).Status == Status.Ok;
        }

        private void Inscrever(string canal)
        {
            sub.Subscribe(canal);
            inscritos.Add(canal);
            Console.WriteLine($"[{nome}] inscrito no canal #{canal} ({inscritos.Count} inscrições)");
        }

        private bool Publicar(string canal, string texto)
        {
            Envelope pedido = msgr.Build(Type.Publish);
            pedido.Channel = canal;
            pedido.Text = texto;
            return Requisitar(pedido).Status == Status.Ok;
        }

        private void Historico(string canal)
        {
            Envelope pedido = msgr.Build(Type.History);
            pedido.Channel = canal;
            Envelope resp = Requisitar(pedido);
            if (resp.Status != Status.Ok)
            {
                return;
            }
            Console.WriteLine($"[{nome}] histórico de #{canal}: {resp.Total} mensagens no total (respondido por {resp.Sender}); últimas {resp.Entries.Count}:");
            foreach (LogEntry e in resp.Entries)
            {
                Console.WriteLine($"    [L={e.Clock}] {e.User}: {e.Text}  (recebida por {e.Origin})");
            }
        }

        private void Receber(byte[] canalBytes, bool mais)
        {
            byte[] dados = null;
            while (mais)
            {
                if (!sub.TryReceiveFrameBytes(TimeSpan.FromSeconds(1), out dados, out mais))
                {
                    return;
                }
            }
            string canal = Encoding.UTF8.GetString(canalBytes);
            if (dados == null || !inscritos.Contains(canal))
            {
                return;
            }
            if (msgr.TryParse(dados, out Envelope env))
            {
                msgr.Show("RECEBE", env, "#" + canal, "  | recebida em " + Util.Hora());
            }
        }

        private void Esperar(int milissegundos)
        {
            long fim = Util.AgoraMs() + milissegundos;
            while (true)
            {
                long resto = fim - Util.AgoraMs();
                if (resto <= 0)
                {
                    return;
                }
                if (sub.TryReceiveFrameBytes(out byte[] topico, out bool mais))
                {
                    Receber(topico, mais);
                }
                else
                {
                    Util.Dormir((int)Math.Min(resto, 2));
                }
            }
        }

        public void Run()
        {
            Login();
            while (true)
            {
                List<string> canais = ListarCanais();
                if (canais.Count < 5)
                {
                    CriarCanal("canal-" + random.Next(1000, 10000));
                    canais = ListarCanais();
                }
                if (inscritos.Count < 3)
                {
                    List<string> disponiveis = canais.Where(c => !inscritos.Contains(c)).ToList();
                    if (disponiveis.Count > 0)
                    {
                        Inscrever(disponiveis[random.Next(disponiveis.Count)]);
                    }
                }
                if (canais.Count > 0)
                {
                    string canal = canais[random.Next(canais.Count)];
                    for (int i = 0; i < 10; i++)
                    {
                        Publicar(canal, Frases[random.Next(Frases.Length)]);
                        Esperar(1000);
                    }
                    Historico(canal);
                }
                else
                {
                    Esperar(1000);
                }
            }
        }
    }
}
