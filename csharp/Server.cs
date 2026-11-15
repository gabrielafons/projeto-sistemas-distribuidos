using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Google.Protobuf;
using NetMQ;
using NetMQ.Sockets;

namespace Bbs
{
    internal sealed class Server
    {
        private static readonly string Nome = Util.Env("SERVER_NAME", "viuva-negra-csharp-1");
        private static readonly string BrokerHost = Util.Env("BROKER_HOST", "broker");
        private static readonly string ProxyHost = Util.Env("PROXY_HOST", "proxy");
        private static readonly string RefHost = Util.Env("REF_HOST", "reference");
        private static readonly string DataDir = Util.Env("DATA_DIR", "/data");
        private static readonly string PeerPort = Util.Env("PEER_PORT", "6000");
        private static readonly string Endereco = Util.Env("ADVERTISE_ADDR", "tcp://" + Nome + ":" + PeerPort);
        private static readonly double ClockSkew = double.Parse(Util.Env("CLOCK_SKEW", "0"), CultureInfo.InvariantCulture);

        private const int HeartbeatACadaN = 10;
        private const int SincronizaACadaN = 15;
        private const int TimeoutReq = 2000;
        private const int TimeoutServidor = 1000;
        private const int EsperaAnuncio = 5000;
        private const int AntiEntropiaMs = 20000;
        private const int TimeoutSync = 3000;
        private const int Historico = 10;

        private const string TopicoServidores = "servers";
        private const string TopicoReplica = "replica";

        private readonly PhysicalClock relogio;
        private readonly Messenger msgr;
        private readonly Store store;
        private readonly ResponseSocket rep = new ResponseSocket();
        private readonly ResponseSocket peer = new ResponseSocket();
        private readonly PublisherSocket pub = new PublisherSocket();
        private readonly SubscriberSocket sub = new SubscriberSocket();
        private readonly Random random = new Random();

        private int rank = 0;
        private long nMsgs = 0;
        private string coordinator = null;
        private Dictionary<string, ServerInfo> servidores = new Dictionary<string, ServerInfo>();
        private bool precisaEleicao = false;
        private long proximaEleicao = 0;
        private long proximaSync = Util.AgoraMs() + AntiEntropiaMs;

        public Server()
        {
            relogio = new PhysicalClock(ClockSkew);
            msgr = new Messenger(Nome, relogio);
            store = new Store(DataDir, Nome);

            rep.Connect("tcp://" + BrokerHost + ":5556");
            peer.Bind("tcp://*:" + PeerPort);
            pub.Connect("tcp://" + ProxyHost + ":5557");
            sub.Connect("tcp://" + ProxyHost + ":5558");
            sub.Subscribe(TopicoServidores);
            sub.Subscribe(TopicoReplica);
        }

        private static bool Reservado(string nome)
        {
            return nome == TopicoServidores || nome == TopicoReplica;
        }

        public void Run()
        {
            Console.WriteLine($"[{Nome}] iniciado. {store.ChannelNames().Count} canais carregados de {store.Path}");
            Registrar();
            AntiEntropia(true);
            while (true)
            {
                bool ativo = false;
                if (rep.TryReceiveFrameBytes(out byte[] pedidoCliente))
                {
                    AtenderCliente(pedidoCliente);
                    ativo = true;
                }
                if (peer.TryReceiveFrameBytes(out byte[] pedidoPeer))
                {
                    AtenderPeer(pedidoPeer);
                    ativo = true;
                }
                if (sub.TryReceiveFrameBytes(out byte[] topico, out bool mais))
                {
                    ReceberDoSub(topico, mais);
                    ativo = true;
                }
                if (!ativo)
                {
                    Util.Dormir(2);
                }
                if (Util.AgoraMs() >= proximaSync)
                {
                    AntiEntropia(false);
                }
                if (precisaEleicao || (coordinator == null && Util.AgoraMs() >= proximaEleicao))
                {
                    Eleicao();
                }
            }
        }

        private void Publicar(string topico, Envelope env)
        {
            pub.SendMoreFrame(topico).SendFrame(env.ToByteArray());
        }

        private Envelope Pedir(string endereco, Envelope env, int timeoutMs)
        {
            RequestSocket s = null;
            try
            {
                s = new RequestSocket();
                s.Options.Linger = TimeSpan.Zero;
                s.Connect(endereco);
                if (!s.TrySendFrame(TimeSpan.FromMilliseconds(timeoutMs), env.ToByteArray()))
                {
                    SemResposta(endereco, timeoutMs);
                    return null;
                }
                int pos = endereco.IndexOf("//", StringComparison.Ordinal);
                msgr.Show("ENVIA", env, pos < 0 ? endereco : endereco.Substring(pos + 2));
                long fim = Util.AgoraMs() + timeoutMs;
                while (true)
                {
                    if (peer.TryReceiveFrameBytes(out byte[] pedidoPeer))
                    {
                        AtenderPeer(pedidoPeer);
                    }
                    if (s.TryReceiveFrameBytes(out byte[] resposta))
                    {
                        if (!msgr.TryParse(resposta, out Envelope resp))
                        {
                            return null;
                        }
                        msgr.Show("RECEBE", resp, resp.Sender);
                        return resp;
                    }
                    if (Util.AgoraMs() >= fim)
                    {
                        SemResposta(endereco, timeoutMs);
                        return null;
                    }
                    Util.Dormir(2);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"[{Nome}] falha ao falar com {endereco}: {e.Message}");
                return null;
            }
            finally
            {
                s?.Dispose();
            }
        }

        private static void SemResposta(string endereco, int timeoutMs)
        {
            Console.WriteLine($"[{Nome}] sem resposta de {endereco} em {(timeoutMs / 1000.0).ToString(CultureInfo.InvariantCulture)}s");
        }

        private Envelope Referencia(Envelope env)
        {
            return Pedir("tcp://" + RefHost + ":5559", env, TimeoutReq);
        }

        private void Registrar()
        {
            while (true)
            {
                Envelope req = msgr.Build(Type.GetRank);
                req.Address = Endereco;
                Envelope resp = Referencia(req);
                if (resp != null && resp.Status == Status.Ok)
                {
                    rank = resp.Rank;
                    Console.WriteLine($"[{Nome}] rank {rank}, endereço {Endereco}");
                    return;
                }
                Util.Dormir(2000);
            }
        }

        private void AtualizarServidores()
        {
            Envelope lista = Referencia(msgr.Build(Type.ListServers));
            if (lista != null && lista.Status == Status.Ok)
            {
                servidores = lista.Servers.ToDictionary(s => s.Name, s => s);
                string nomes = string.Join(", ", lista.Servers.Select(s => $"{s.Name}(rank {s.Rank})"));
                Console.WriteLine($"[{Nome}] servidores disponíveis: {nomes}");
            }
        }

        private void Heartbeat()
        {
            Envelope req = msgr.Build(Type.Heartbeat);
            req.Address = Endereco;
            Envelope resp = Referencia(req);
            if (resp == null)
            {
                return;
            }
            if (resp.Status != Status.Ok)
            {
                Registrar();
            }
            AtualizarServidores();
        }

        private void Eleicao()
        {
            precisaEleicao = false;
            Console.WriteLine($"[{Nome}] iniciando eleição (rank {rank})");
            AtualizarServidores();
            List<ServerInfo> superiores = servidores.Values.Where(s => s.Rank > rank).OrderByDescending(s => s.Rank).ToList();
            foreach (ServerInfo s in superiores)
            {
                Envelope req = msgr.Build(Type.Election);
                req.Rank = rank;
                Envelope resp = Pedir(s.Address, req, TimeoutServidor);
                if (resp != null && resp.Status == Status.Ok)
                {
                    Console.WriteLine($"[{Nome}] {s.Name} (rank {s.Rank}) respondeu OK: aguardando o anúncio");
                    coordinator = null;
                    proximaEleicao = Util.AgoraMs() + EsperaAnuncio;
                    return;
                }
            }
            VirarCoordenador();
        }

        private void VirarCoordenador()
        {
            coordinator = Nome;
            Envelope anuncio = msgr.Build(Type.Coordinator);
            anuncio.Coordinator = Nome;
            anuncio.Rank = rank;
            Publicar(TopicoServidores, anuncio);
            msgr.Show("ENVIA", anuncio, "#" + TopicoServidores);
            Console.WriteLine($"[{Nome}] eleito coordenador");
        }

        private void SincronizarRelogio()
        {
            if (coordinator == null)
            {
                return;
            }
            if (coordinator == Nome)
            {
                Console.WriteLine($"[{Nome}] sou o coordenador: meu relógio é a referência");
                return;
            }
            if (!servidores.ContainsKey(coordinator))
            {
                AtualizarServidores();
            }
            Envelope resp = null;
            double t0 = 0;
            double t1 = 0;
            if (servidores.TryGetValue(coordinator, out ServerInfo info))
            {
                t0 = relogio.Raw();
                resp = Pedir(info.Address, msgr.Build(Type.Clock), TimeoutServidor);
                t1 = relogio.Raw();
            }
            if (resp == null || resp.Status != Status.Ok)
            {
                Console.WriteLine($"[{Nome}] coordenador {coordinator} indisponível: pedindo eleição");
                coordinator = null;
                precisaEleicao = true;
                return;
            }
            double offset = relogio.Sync(resp.Time, t0, t1);
            Console.WriteLine($"[{Nome}] relógio ajustado pelo coordenador {coordinator}: correção de {offset.ToString("+0.000;-0.000", CultureInfo.InvariantCulture)}s");
        }

        private Envelope Erro(Type tipo, string texto)
        {
            Envelope e = msgr.Build(tipo);
            e.Status = Status.Error;
            e.Error = texto;
            return e;
        }

        private void AtenderPeer(byte[] dados)
        {
            if (!msgr.TryParse(dados, out Envelope env))
            {
                peer.SendFrame(Erro(Type.Unknown, "mensagem inválida").ToByteArray());
                return;
            }
            msgr.Show("RECEBE", env, env.Sender);
            Envelope resp;
            switch (env.Type)
            {
                case Type.Election:
                    resp = msgr.Build(env.Type);
                    resp.Status = Status.Ok;
                    resp.Rank = rank;
                    precisaEleicao = true;
                    break;
                case Type.Sync:
                    resp = msgr.Build(env.Type);
                    resp.Status = Status.Ok;
                    foreach (Entry e in store.Entries)
                    {
                        resp.Entries.Add(e.ToProto());
                    }
                    break;
                case Type.Clock:
                    if (coordinator == Nome)
                    {
                        resp = msgr.Build(env.Type);
                        resp.Status = Status.Ok;
                        resp.Time = relogio.Now();
                    }
                    else
                    {
                        resp = Erro(env.Type, "este servidor não é o coordenador");
                    }
                    break;
                default:
                    resp = Erro(env.Type, "tipo de mensagem desconhecido");
                    break;
            }
            peer.SendFrame(resp.ToByteArray());
            msgr.Show("ENVIA", resp, env.Sender);
        }

        private void ReceberDoSub(byte[] topicoBytes, bool mais)
        {
            byte[] dados = null;
            while (mais)
            {
                if (!sub.TryReceiveFrameBytes(TimeSpan.FromSeconds(1), out dados, out mais))
                {
                    return;
                }
            }
            if (dados == null)
            {
                return;
            }
            string topico = Encoding.UTF8.GetString(topicoBytes);
            if (topico != TopicoServidores && topico != TopicoReplica)
            {
                return;
            }
            if (!msgr.TryParse(dados, out Envelope env))
            {
                return;
            }
            if (env.Sender == Nome)
            {
                return;
            }
            msgr.Show("RECEBE", env, "#" + topico);
            if (topico == TopicoReplica)
            {
                AplicarEntradas(env.Entries, env.Sender);
                return;
            }
            coordinator = env.Coordinator;
            Console.WriteLine($"[{Nome}] coordenador agora é {coordinator}");
            if (rank != 0 && env.Rank < rank)
            {
                precisaEleicao = true;
            }
        }

        private void Gravar(Entry entrada)
        {
            if (store.Add(entrada))
            {
                Envelope msg = msgr.Build(Type.Replicate);
                msg.Entries.Add(entrada.ToProto());
                Publicar(TopicoReplica, msg);
                msgr.Show("ENVIA", msg, "#" + TopicoReplica);
            }
        }

        private int AplicarEntradas(IEnumerable<LogEntry> entradas, string origem)
        {
            int novas = 0;
            foreach (LogEntry le in entradas)
            {
                if (store.Add(Entry.FromProto(le)))
                {
                    novas++;
                }
            }
            if (novas > 0)
            {
                Console.WriteLine($"[{Nome}] replicação: {novas} entrada(s) nova(s) recebida(s) de {origem}");
            }
            return novas;
        }

        private void PuxarDe(ServerInfo info)
        {
            Envelope resp = Pedir(info.Address, msgr.Build(Type.Sync), TimeoutSync);
            if (resp != null && resp.Status == Status.Ok)
            {
                AplicarEntradas(resp.Entries, info.Name);
            }
        }

        private void AntiEntropia(bool todos)
        {
            proximaSync = Util.AgoraMs() + AntiEntropiaMs;
            AtualizarServidores();
            List<ServerInfo> outros = servidores.Values.Where(s => s.Name != Nome).ToList();
            if (!todos && outros.Count > 0)
            {
                outros = new List<ServerInfo> { outros[random.Next(outros.Count)] };
            }
            foreach (ServerInfo info in outros)
            {
                PuxarDe(info);
            }
        }

        private Entry Evento(string kind, string user, string channel, string text)
        {
            return store.NewEntry(kind, relogio.Now(), msgr.Clock, user, channel, text);
        }

        private Envelope Tratar(Envelope env)
        {
            switch (env.Type)
            {
                case Type.Login:
                {
                    if (!Util.NomeValido(env.User))
                    {
                        return Erro(env.Type, "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)");
                    }
                    Gravar(Evento("login", env.User, "", ""));
                    Envelope r = msgr.Build(env.Type);
                    r.Status = Status.Ok;
                    r.User = env.User;
                    return r;
                }
                case Type.CreateChannel:
                {
                    if (!Util.NomeValido(env.Channel))
                    {
                        return Erro(env.Type, "nome de canal inválido (use 1 a 32 letras, números, _ ou -)");
                    }
                    if (Reservado(env.Channel))
                    {
                        return Erro(env.Type, $"'{env.Channel}' é um nome reservado");
                    }
                    if (store.HasChannel(env.Channel))
                    {
                        return Erro(env.Type, $"o canal '{env.Channel}' já existe");
                    }
                    Gravar(Evento("channel", env.Sender, env.Channel, ""));
                    Envelope r = msgr.Build(env.Type);
                    r.Status = Status.Ok;
                    r.Channel = env.Channel;
                    return r;
                }
                case Type.Publish:
                {
                    if (!Util.NomeValido(env.Channel))
                    {
                        return Erro(env.Type, "nome de canal inválido");
                    }
                    if (Reservado(env.Channel))
                    {
                        return Erro(env.Type, $"'{env.Channel}' é um nome reservado");
                    }
                    if (env.Text.Length == 0)
                    {
                        return Erro(env.Type, "mensagem vazia");
                    }
                    Envelope publicacao = msgr.Build(Type.Publication);
                    publicacao.User = env.Sender;
                    publicacao.Channel = env.Channel;
                    publicacao.Text = env.Text;
                    Gravar(store.NewEntry("message", publicacao.Timestamp, publicacao.Clock, env.Sender, env.Channel, env.Text));
                    Publicar(env.Channel, publicacao);
                    msgr.Show("ENVIA", publicacao, "#" + env.Channel);
                    Envelope r = msgr.Build(env.Type);
                    r.Status = Status.Ok;
                    r.Channel = env.Channel;
                    return r;
                }
                case Type.ListChannels:
                {
                    Envelope r = msgr.Build(env.Type);
                    r.Status = Status.Ok;
                    r.Channels.AddRange(store.ChannelNames());
                    return r;
                }
                case Type.History:
                {
                    if (!Util.NomeValido(env.Channel))
                    {
                        return Erro(env.Type, "nome de canal inválido");
                    }
                    List<Entry> msgs = store.Messages(env.Channel);
                    Envelope r = msgr.Build(env.Type);
                    r.Status = Status.Ok;
                    r.Channel = env.Channel;
                    r.Total = msgs.Count;
                    foreach (Entry e in msgs.Skip(Math.Max(0, msgs.Count - Historico)))
                    {
                        r.Entries.Add(e.ToProto());
                    }
                    return r;
                }
                default:
                    return Erro(env.Type, "tipo de mensagem desconhecido");
            }
        }

        private void AtenderCliente(byte[] dados)
        {
            if (!msgr.TryParse(dados, out Envelope env))
            {
                Envelope invalida = Erro(Type.Unknown, "mensagem inválida (não é um Envelope Protobuf)");
                rep.SendFrame(invalida.ToByteArray());
                msgr.Show("ENVIA", invalida, "");
                return;
            }
            msgr.Show("RECEBE", env, env.Sender);
            Envelope resp = Tratar(env);
            rep.SendFrame(resp.ToByteArray());
            msgr.Show("ENVIA", resp, env.Sender);
            nMsgs++;
            if (nMsgs % HeartbeatACadaN == 0)
            {
                Heartbeat();
            }
            if (nMsgs % SincronizaACadaN == 0)
            {
                SincronizarRelogio();
            }
        }
    }
}
