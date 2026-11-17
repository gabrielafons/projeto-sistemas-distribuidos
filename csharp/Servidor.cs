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
    internal sealed class Servidor
    {
        private static readonly string Nome = Util.Variavel("SERVER_NAME", "viuva-negra-csharp-1");
        private static readonly string HostBroker = Util.Variavel("BROKER_HOST", "broker");
        private static readonly string HostProxy = Util.Variavel("PROXY_HOST", "proxy");
        private static readonly string HostReferencia = Util.Variavel("REF_HOST", "reference");
        private static readonly string DiretorioDados = Util.Variavel("DATA_DIR", "/data");
        private static readonly string PortaPar = Util.Variavel("PEER_PORT", "6000");
        private static readonly string Endereco = Util.Variavel("ADVERTISE_ADDR", "tcp://" + Nome + ":" + PortaPar);
        private static readonly double DesvioRelogio = double.Parse(Util.Variavel("CLOCK_SKEW", "0"), CultureInfo.InvariantCulture);

        private const int BatimentoACadaN = 10;
        private const int SincronizaACadaN = 15;
        private const int TempoLimiteReq = 2000;
        private const int TempoLimiteServidor = 1000;
        private const int EsperaAnuncio = 5000;
        private const int AntiEntropiaMs = 20000;
        private const int TempoLimiteSync = 3000;

        private const string TopicoServidores = "servers";
        private const string TopicoReplica = "replica";

        private readonly RelogioFisico relogio;
        private readonly Mensageiro mensageiro;
        private readonly Armazenamento armazenamento;
        private readonly ResponseSocket atendimento = new ResponseSocket();
        private readonly ResponseSocket par = new ResponseSocket();
        private readonly PublisherSocket publicador = new PublisherSocket();
        private readonly SubscriberSocket assinante = new SubscriberSocket();
        private readonly Random sorteio = new Random();

        private int rank = 0;
        private long mensagensRecebidas = 0;
        private string coordenador = null;
        private Dictionary<string, InfoServidor> servidores = new Dictionary<string, InfoServidor>();
        private bool precisaEleicao = false;
        private long proximaEleicao = 0;
        private long proximaSync = Util.AgoraMs() + AntiEntropiaMs;

        public Servidor()
        {
            relogio = new RelogioFisico(DesvioRelogio);
            mensageiro = new Mensageiro(Nome, relogio);
            armazenamento = new Armazenamento(DiretorioDados, Nome);

            atendimento.Connect("tcp://" + HostBroker + ":5556");
            par.Bind("tcp://*:" + PortaPar);
            publicador.Connect("tcp://" + HostProxy + ":5557");
            assinante.Connect("tcp://" + HostProxy + ":5558");
            assinante.Subscribe(TopicoServidores);
            assinante.Subscribe(TopicoReplica);
        }

        private static bool Reservado(string nome)
        {
            return nome == TopicoServidores || nome == TopicoReplica;
        }

        public void Executar()
        {
            Console.WriteLine($"[{Nome}] iniciado. {armazenamento.NomesCanais().Count} canais carregados de {armazenamento.Caminho}");
            Registrar();
            AntiEntropia(true);
            while (true)
            {
                bool ativo = false;
                if (atendimento.TryReceiveFrameBytes(out byte[] pedidoCliente))
                {
                    AtenderCliente(pedidoCliente);
                    ativo = true;
                }
                if (par.TryReceiveFrameBytes(out byte[] pedidoPar))
                {
                    AtenderPar(pedidoPar);
                    ativo = true;
                }
                if (assinante.TryReceiveFrameBytes(out byte[] topico, out bool mais))
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
                if (precisaEleicao || (coordenador == null && Util.AgoraMs() >= proximaEleicao))
                {
                    Eleicao();
                }
            }
        }

        private void Publicar(string topico, Envelope envelope)
        {
            publicador.SendMoreFrame(topico).SendFrame(envelope.ToByteArray());
        }

        private Envelope Pedir(string endereco, Envelope envelope, int tempoLimiteMs)
        {
            RequestSocket requisicao = null;
            try
            {
                requisicao = new RequestSocket();
                requisicao.Options.Linger = TimeSpan.Zero;
                requisicao.Connect(endereco);
                if (!requisicao.TrySendFrame(TimeSpan.FromMilliseconds(tempoLimiteMs), envelope.ToByteArray()))
                {
                    SemResposta(endereco, tempoLimiteMs);
                    return null;
                }
                int pos = endereco.IndexOf("//", StringComparison.Ordinal);
                mensageiro.Exibir("ENVIA", envelope, pos < 0 ? endereco : endereco.Substring(pos + 2));
                long fim = Util.AgoraMs() + tempoLimiteMs;
                while (true)
                {
                    if (par.TryReceiveFrameBytes(out byte[] pedidoPar))
                    {
                        AtenderPar(pedidoPar);
                    }
                    if (requisicao.TryReceiveFrameBytes(out byte[] resposta))
                    {
                        if (!mensageiro.TentarLer(resposta, out Envelope recebida))
                        {
                            return null;
                        }
                        mensageiro.Exibir("RECEBE", recebida, recebida.Remetente);
                        return recebida;
                    }
                    if (Util.AgoraMs() >= fim)
                    {
                        SemResposta(endereco, tempoLimiteMs);
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
                requisicao?.Dispose();
            }
        }

        private static void SemResposta(string endereco, int tempoLimiteMs)
        {
            Console.WriteLine($"[{Nome}] sem resposta de {endereco} em {(tempoLimiteMs / 1000.0).ToString(CultureInfo.InvariantCulture)}s");
        }

        private Envelope Referencia(Envelope envelope)
        {
            return Pedir("tcp://" + HostReferencia + ":5559", envelope, TempoLimiteReq);
        }

        private void Registrar()
        {
            while (true)
            {
                Envelope requisicao = mensageiro.Montar(Tipo.ObterRank);
                requisicao.Endereco = Endereco;
                Envelope resposta = Referencia(requisicao);
                if (resposta != null && resposta.Status == Status.Ok)
                {
                    rank = resposta.Rank;
                    Console.WriteLine($"[{Nome}] rank {rank}, endereço {Endereco}");
                    return;
                }
                Util.Dormir(2000);
            }
        }

        private void AtualizarServidores()
        {
            Envelope lista = Referencia(mensageiro.Montar(Tipo.ListarServidores));
            if (lista != null && lista.Status == Status.Ok)
            {
                servidores = lista.Servidores.ToDictionary(s => s.Nome, s => s);
                string nomes = string.Join(", ", lista.Servidores.Select(s => $"{s.Nome}(rank {s.Rank})"));
                Console.WriteLine($"[{Nome}] servidores disponíveis: {nomes}");
            }
        }

        private void Batimento()
        {
            Envelope requisicao = mensageiro.Montar(Tipo.Batimento);
            requisicao.Endereco = Endereco;
            Envelope resposta = Referencia(requisicao);
            if (resposta == null)
            {
                return;
            }
            if (resposta.Status != Status.Ok)
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
            List<InfoServidor> superiores = servidores.Values.Where(s => s.Rank > rank).OrderByDescending(s => s.Rank).ToList();
            foreach (InfoServidor s in superiores)
            {
                Envelope requisicao = mensageiro.Montar(Tipo.Eleicao);
                requisicao.Rank = rank;
                Envelope resposta = Pedir(s.Endereco, requisicao, TempoLimiteServidor);
                if (resposta != null && resposta.Status == Status.Ok)
                {
                    Console.WriteLine($"[{Nome}] {s.Nome} (rank {s.Rank}) respondeu OK: aguardando o anúncio");
                    coordenador = null;
                    proximaEleicao = Util.AgoraMs() + EsperaAnuncio;
                    return;
                }
            }
            VirarCoordenador();
        }

        private void VirarCoordenador()
        {
            coordenador = Nome;
            Envelope anuncio = mensageiro.Montar(Tipo.Coordenador);
            anuncio.Coordenador = Nome;
            anuncio.Rank = rank;
            Publicar(TopicoServidores, anuncio);
            mensageiro.Exibir("ENVIA", anuncio, "#" + TopicoServidores);
            Console.WriteLine($"[{Nome}] eleito coordenador");
        }

        private void SincronizarRelogio()
        {
            if (coordenador == null)
            {
                return;
            }
            if (coordenador == Nome)
            {
                Console.WriteLine($"[{Nome}] sou o coordenador: meu relógio é a referência");
                return;
            }
            if (!servidores.ContainsKey(coordenador))
            {
                AtualizarServidores();
            }
            Envelope resposta = null;
            double t0 = 0;
            double t1 = 0;
            if (servidores.TryGetValue(coordenador, out InfoServidor info))
            {
                t0 = relogio.Bruto();
                resposta = Pedir(info.Endereco, mensageiro.Montar(Tipo.Relogio), TempoLimiteServidor);
                t1 = relogio.Bruto();
            }
            if (resposta == null || resposta.Status != Status.Ok)
            {
                Console.WriteLine($"[{Nome}] coordenador {coordenador} indisponível: pedindo eleição");
                coordenador = null;
                precisaEleicao = true;
                return;
            }
            double correcao = relogio.Sincronizar(resposta.Hora, t0, t1);
            Console.WriteLine($"[{Nome}] relógio ajustado pelo coordenador {coordenador}: correção de {correcao.ToString("+0.000;-0.000", CultureInfo.InvariantCulture)}s");
        }

        private Envelope Erro(Tipo tipo, string texto)
        {
            Envelope e = mensageiro.Montar(tipo);
            e.Status = Status.Erro;
            e.Erro = texto;
            return e;
        }

        private void AtenderPar(byte[] dados)
        {
            if (!mensageiro.TentarLer(dados, out Envelope envelope))
            {
                par.SendFrame(Erro(Tipo.Desconhecido, "mensagem inválida").ToByteArray());
                return;
            }
            mensageiro.Exibir("RECEBE", envelope, envelope.Remetente);
            Envelope resposta;
            switch (envelope.Tipo)
            {
                case Tipo.Eleicao:
                    resposta = mensageiro.Montar(envelope.Tipo);
                    resposta.Status = Status.Ok;
                    resposta.Rank = rank;
                    precisaEleicao = true;
                    break;
                case Tipo.Sincronizar:
                    resposta = mensageiro.Montar(envelope.Tipo);
                    resposta.Status = Status.Ok;
                    foreach (Entrada e in armazenamento.Entradas)
                    {
                        resposta.Entradas.Add(e.ParaProto());
                    }
                    break;
                case Tipo.Relogio:
                    if (coordenador == Nome)
                    {
                        resposta = mensageiro.Montar(envelope.Tipo);
                        resposta.Status = Status.Ok;
                        resposta.Hora = relogio.Agora();
                    }
                    else
                    {
                        resposta = Erro(envelope.Tipo, "este servidor não é o coordenador");
                    }
                    break;
                default:
                    resposta = Erro(envelope.Tipo, "tipo de mensagem desconhecido");
                    break;
            }
            par.SendFrame(resposta.ToByteArray());
            mensageiro.Exibir("ENVIA", resposta, envelope.Remetente);
        }

        private void ReceberDoSub(byte[] topicoBytes, bool mais)
        {
            byte[] dados = null;
            while (mais)
            {
                if (!assinante.TryReceiveFrameBytes(TimeSpan.FromSeconds(1), out dados, out mais))
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
            if (!mensageiro.TentarLer(dados, out Envelope envelope))
            {
                return;
            }
            if (envelope.Remetente == Nome)
            {
                return;
            }
            mensageiro.Exibir("RECEBE", envelope, "#" + topico);
            if (topico == TopicoReplica)
            {
                AplicarEntradas(envelope.Entradas, envelope.Remetente);
                return;
            }
            coordenador = envelope.Coordenador;
            Console.WriteLine($"[{Nome}] coordenador agora é {coordenador}");
            if (rank != 0 && envelope.Rank < rank)
            {
                precisaEleicao = true;
            }
        }

        private void Gravar(Entrada entrada)
        {
            if (armazenamento.Adicionar(entrada))
            {
                Envelope mensagem = mensageiro.Montar(Tipo.Replicar);
                mensagem.Entradas.Add(entrada.ParaProto());
                Publicar(TopicoReplica, mensagem);
                mensageiro.Exibir("ENVIA", mensagem, "#" + TopicoReplica);
            }
        }

        private int AplicarEntradas(IEnumerable<EntradaLog> entradas, string origem)
        {
            int novas = 0;
            foreach (EntradaLog le in entradas)
            {
                if (armazenamento.Adicionar(Entrada.DeProto(le)))
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

        private void PuxarDe(InfoServidor info)
        {
            Envelope resposta = Pedir(info.Endereco, mensageiro.Montar(Tipo.Sincronizar), TempoLimiteSync);
            if (resposta != null && resposta.Status == Status.Ok)
            {
                AplicarEntradas(resposta.Entradas, info.Nome);
            }
        }

        private void AntiEntropia(bool todos)
        {
            proximaSync = Util.AgoraMs() + AntiEntropiaMs;
            AtualizarServidores();
            List<InfoServidor> outros = servidores.Values.Where(s => s.Nome != Nome).ToList();
            if (!todos && outros.Count > 0)
            {
                outros = new List<InfoServidor> { outros[sorteio.Next(outros.Count)] };
            }
            foreach (InfoServidor info in outros)
            {
                PuxarDe(info);
            }
        }

        private Entrada Evento(string tipo, string usuario, string canal, string texto)
        {
            return armazenamento.NovaEntrada(tipo, relogio.Agora(), mensageiro.Relogio, usuario, canal, texto);
        }

        private Envelope Tratar(Envelope envelope)
        {
            switch (envelope.Tipo)
            {
                case Tipo.Login:
                {
                    if (!Util.NomeValido(envelope.Usuario))
                    {
                        return Erro(envelope.Tipo, "nome de usuário inválido (use 1 a 32 letras, números, _ ou -)");
                    }
                    Gravar(Evento("login", envelope.Usuario, "", ""));
                    Envelope resultado = mensageiro.Montar(envelope.Tipo);
                    resultado.Status = Status.Ok;
                    resultado.Usuario = envelope.Usuario;
                    return resultado;
                }
                case Tipo.CriarCanal:
                {
                    if (!Util.NomeValido(envelope.Canal))
                    {
                        return Erro(envelope.Tipo, "nome de canal inválido (use 1 a 32 letras, números, _ ou -)");
                    }
                    if (Reservado(envelope.Canal))
                    {
                        return Erro(envelope.Tipo, $"'{envelope.Canal}' é um nome reservado");
                    }
                    if (armazenamento.TemCanal(envelope.Canal))
                    {
                        return Erro(envelope.Tipo, $"o canal '{envelope.Canal}' já existe");
                    }
                    Gravar(Evento("channel", envelope.Remetente, envelope.Canal, ""));
                    Envelope resultado = mensageiro.Montar(envelope.Tipo);
                    resultado.Status = Status.Ok;
                    resultado.Canal = envelope.Canal;
                    return resultado;
                }
                case Tipo.Publicar:
                {
                    if (!Util.NomeValido(envelope.Canal))
                    {
                        return Erro(envelope.Tipo, "nome de canal inválido");
                    }
                    if (Reservado(envelope.Canal))
                    {
                        return Erro(envelope.Tipo, $"'{envelope.Canal}' é um nome reservado");
                    }
                    if (envelope.Texto.Length == 0)
                    {
                        return Erro(envelope.Tipo, "mensagem vazia");
                    }
                    Envelope publicacao = mensageiro.Montar(Tipo.Publicacao);
                    publicacao.Usuario = envelope.Remetente;
                    publicacao.Canal = envelope.Canal;
                    publicacao.Texto = envelope.Texto;
                    Gravar(armazenamento.NovaEntrada("message", publicacao.Instante, publicacao.Relogio, envelope.Remetente, envelope.Canal, envelope.Texto));
                    Publicar(envelope.Canal, publicacao);
                    mensageiro.Exibir("ENVIA", publicacao, "#" + envelope.Canal);
                    Envelope resultado = mensageiro.Montar(envelope.Tipo);
                    resultado.Status = Status.Ok;
                    resultado.Canal = envelope.Canal;
                    return resultado;
                }
                case Tipo.ListarCanais:
                {
                    Envelope resultado = mensageiro.Montar(envelope.Tipo);
                    resultado.Status = Status.Ok;
                    resultado.Canais.AddRange(armazenamento.NomesCanais());
                    return resultado;
                }
                default:
                    return Erro(envelope.Tipo, "tipo de mensagem desconhecido");
            }
        }

        private void AtenderCliente(byte[] dados)
        {
            if (!mensageiro.TentarLer(dados, out Envelope envelope))
            {
                Envelope invalida = Erro(Tipo.Desconhecido, "mensagem inválida (não é um Envelope Protobuf)");
                atendimento.SendFrame(invalida.ToByteArray());
                mensageiro.Exibir("ENVIA", invalida, "");
                return;
            }
            mensageiro.Exibir("RECEBE", envelope, envelope.Remetente);
            Envelope resposta = Tratar(envelope);
            atendimento.SendFrame(resposta.ToByteArray());
            mensageiro.Exibir("ENVIA", resposta, envelope.Remetente);
            mensagensRecebidas++;
            if (mensagensRecebidas % BatimentoACadaN == 0)
            {
                Batimento();
            }
            if (mensagensRecebidas % SincronizaACadaN == 0)
            {
                SincronizarRelogio();
            }
        }
    }
}
