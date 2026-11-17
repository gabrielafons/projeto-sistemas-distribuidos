using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

namespace Bbs
{
    internal sealed class Cliente
    {
        private static readonly string HostBroker = Util.Variavel("BROKER_HOST", "broker");
        private static readonly string HostProxy = Util.Variavel("PROXY_HOST", "proxy");
        private const int TempoLimiteMs = 5000;

        private static readonly string[] Frases =
        {
            "bom dia", "alguém aí?", "olá, pessoal", "testando o canal", "mensagem aleatória",
            "tudo bem por aí?", "sistemas distribuídos são legais", "ping", "mais uma mensagem",
            "o que vocês acham?"
        };

        private readonly Random sorteio = new Random();
        private readonly Mensageiro mensageiro;
        private readonly SubscriberSocket assinante = new SubscriberSocket();
        private readonly HashSet<string> inscritos = new HashSet<string>();
        private RequestSocket requisicao;
        private string nome;

        public Cliente()
        {
            string bot = Util.Variavel("BOT_NAME", "");
            nome = bot.Length > 0 ? bot : NomeAleatorio();
            mensageiro = new Mensageiro(nome, new RelogioFisico(0));
            assinante.Connect("tcp://" + HostProxy + ":5558");
            Conectar();
        }

        private string NomeAleatorio()
        {
            return "cs-bot-" + sorteio.Next(1000, 10000);
        }

        private void Conectar()
        {
            if (requisicao != null)
            {
                requisicao.Options.Linger = TimeSpan.Zero;
                requisicao.Dispose();
            }
            requisicao = new RequestSocket();
            requisicao.Options.Linger = TimeSpan.Zero;
            requisicao.Connect("tcp://" + HostBroker + ":5555");
        }

        private Envelope Requisitar(Envelope envelope)
        {
            while (true)
            {
                if (requisicao.TrySendFrame(TimeSpan.FromMilliseconds(TempoLimiteMs), envelope.ToByteArray()))
                {
                    mensageiro.Exibir("ENVIA", envelope, "servidor");
                    if (requisicao.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(TempoLimiteMs), out byte[] dados)
                        && mensageiro.TentarLer(dados, out Envelope resposta))
                    {
                        mensageiro.Exibir("RECEBE", resposta, resposta.Remetente);
                        return resposta;
                    }
                }
                Console.WriteLine($"[{nome}] sem resposta em {TempoLimiteMs} ms, tentando de novo");
                Conectar();
            }
        }

        private void Login()
        {
            while (true)
            {
                Envelope pedido = mensageiro.Montar(Tipo.Login);
                pedido.Usuario = nome;
                if (Requisitar(pedido).Status == Status.Ok)
                {
                    return;
                }
                Util.Dormir(1000);
                nome = NomeAleatorio();
                mensageiro.Nome = nome;
            }
        }

        private List<string> ListarCanais()
        {
            Envelope resposta = Requisitar(mensageiro.Montar(Tipo.ListarCanais));
            return resposta.Status == Status.Ok ? resposta.Canais.ToList() : new List<string>();
        }

        private bool CriarCanal(string canal)
        {
            Envelope pedido = mensageiro.Montar(Tipo.CriarCanal);
            pedido.Canal = canal;
            return Requisitar(pedido).Status == Status.Ok;
        }

        private void Inscrever(string canal)
        {
            assinante.Subscribe(canal);
            inscritos.Add(canal);
            Console.WriteLine($"[{nome}] inscrito no canal #{canal} ({inscritos.Count} inscrições)");
        }

        private bool Publicar(string canal, string texto)
        {
            Envelope pedido = mensageiro.Montar(Tipo.Publicar);
            pedido.Canal = canal;
            pedido.Texto = texto;
            return Requisitar(pedido).Status == Status.Ok;
        }

        private void Receber(byte[] canalBytes, bool mais)
        {
            byte[] dados = null;
            while (mais)
            {
                if (!assinante.TryReceiveFrameBytes(TimeSpan.FromSeconds(1), out dados, out mais))
                {
                    return;
                }
            }
            string canal = Encoding.UTF8.GetString(canalBytes);
            if (dados == null || !inscritos.Contains(canal))
            {
                return;
            }
            if (mensageiro.TentarLer(dados, out Envelope envelope))
            {
                mensageiro.Exibir("RECEBE", envelope, "#" + canal, "  | recebida em " + Util.Hora());
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
                if (assinante.TryReceiveFrameBytes(out byte[] topico, out bool mais))
                {
                    Receber(topico, mais);
                }
                else
                {
                    Util.Dormir((int)Math.Min(resto, 2));
                }
            }
        }

        public void Executar()
        {
            Login();
            while (true)
            {
                List<string> canais = ListarCanais();
                if (canais.Count < 5)
                {
                    CriarCanal("canal-" + sorteio.Next(1000, 10000));
                    canais = ListarCanais();
                }
                if (inscritos.Count < 3)
                {
                    List<string> disponiveis = canais.Where(c => !inscritos.Contains(c)).ToList();
                    if (disponiveis.Count > 0)
                    {
                        Inscrever(disponiveis[sorteio.Next(disponiveis.Count)]);
                    }
                }
                if (canais.Count > 0)
                {
                    string canal = canais[sorteio.Next(canais.Count)];
                    for (int i = 0; i < 10; i++)
                    {
                        Publicar(canal, Frases[sorteio.Next(Frases.Length)]);
                        Esperar(1000);
                    }
                }
                else
                {
                    Esperar(1000);
                }
            }
        }
    }
}
