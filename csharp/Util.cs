using System;
using System.Linq;
using System.Threading;

namespace Bbs
{
    internal static class Util
    {
        public static string Variavel(string nome, string padrao)
        {
            string valor = Environment.GetEnvironmentVariable(nome);
            return string.IsNullOrEmpty(valor) ? padrao : valor;
        }

        public static double Agora()
        {
            return (DateTime.UtcNow - DateTime.UnixEpoch).TotalSeconds;
        }

        public static long AgoraMs()
        {
            return Environment.TickCount64;
        }

        public static string Hora()
        {
            return DateTime.Now.ToString("HH:mm:ss.fff");
        }

        public static bool NomeValido(string texto)
        {
            return texto.Length >= 1 && texto.Length <= 32 && texto.All(c => (c < 128 && char.IsLetterOrDigit(c)) || c == '_' || c == '-');
        }

        public static void Dormir(int milissegundos)
        {
            Thread.Sleep(milissegundos);
        }
    }

    internal sealed class RelogioFisico
    {
        private readonly double desvio;
        private double correcao = 0.0;

        public RelogioFisico(double desvio)
        {
            this.desvio = desvio;
        }

        public double Bruto()
        {
            return Util.Agora() + desvio;
        }

        public double Agora()
        {
            return Bruto() + correcao;
        }

        public double Sincronizar(double horaRemota, double t0, double t1)
        {
            double estimada = horaRemota + (t1 - t0) / 2.0;
            correcao = estimada - t1;
            return correcao;
        }
    }

    internal sealed class Mensageiro
    {
        public string Nome;
        public long Relogio = 0;
        private readonly RelogioFisico fisico;

        public Mensageiro(string nome, RelogioFisico fisico)
        {
            Nome = nome;
            this.fisico = fisico;
        }

        public Envelope Montar(Tipo tipo)
        {
            Relogio++;
            return new Envelope
            {
                Tipo = tipo,
                Remetente = Nome,
                Instante = fisico.Agora(),
                Relogio = Relogio
            };
        }

        public bool TentarLer(byte[] dados, out Envelope envelope)
        {
            try
            {
                envelope = Envelope.Parser.ParseFrom(dados);
                Relogio = Math.Max(Relogio, envelope.Relogio);
                return true;
            }
            catch (Google.Protobuf.InvalidProtocolBufferException)
            {
                envelope = null;
                return false;
            }
        }

        public void Exibir(string direcao, Envelope envelope, string outro, string extra = "")
        {
            string seta = direcao == "ENVIA" ? "->" : "<-";
            Console.WriteLine($"{Util.Hora()} [{Nome}|L={Relogio}] {direcao,-7} {seta} {outro,-14} {envelope}{extra}");
        }
    }
}
