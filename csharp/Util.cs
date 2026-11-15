using System;
using System.Linq;
using System.Threading;

namespace Bbs
{
    internal static class Util
    {
        public static string Env(string nome, string padrao)
        {
            string v = Environment.GetEnvironmentVariable(nome);
            return string.IsNullOrEmpty(v) ? padrao : v;
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

        public static bool NomeValido(string s)
        {
            return s.Length >= 1 && s.Length <= 32 && s.All(c => (c < 128 && char.IsLetterOrDigit(c)) || c == '_' || c == '-');
        }

        public static void Dormir(int ms)
        {
            Thread.Sleep(ms);
        }
    }

    internal sealed class PhysicalClock
    {
        private readonly double skew;
        private double offset = 0.0;

        public PhysicalClock(double skew)
        {
            this.skew = skew;
        }

        public double Raw()
        {
            return Util.Agora() + skew;
        }

        public double Now()
        {
            return Raw() + offset;
        }

        public double Sync(double horaRemota, double t0, double t1)
        {
            double estimada = horaRemota + (t1 - t0) / 2.0;
            offset = estimada - t1;
            return offset;
        }
    }

    internal sealed class Messenger
    {
        public string Name;
        public long Clock = 0;
        private readonly PhysicalClock physical;

        public Messenger(string name, PhysicalClock physical)
        {
            Name = name;
            this.physical = physical;
        }

        public Envelope Build(Type type)
        {
            Clock++;
            return new Envelope
            {
                Type = type,
                Sender = Name,
                Timestamp = physical.Now(),
                Clock = Clock
            };
        }

        public bool TryParse(byte[] data, out Envelope env)
        {
            try
            {
                env = Envelope.Parser.ParseFrom(data);
                Clock = Math.Max(Clock, env.Clock);
                return true;
            }
            catch (Google.Protobuf.InvalidProtocolBufferException)
            {
                env = null;
                return false;
            }
        }

        public void Show(string direction, Envelope env, string other, string extra = "")
        {
            string seta = direction == "ENVIA" ? "->" : "<-";
            Console.WriteLine($"{Util.Hora()} [{Name}|L={Clock}] {direction,-7} {seta} {other,-14} {env}{extra}");
        }
    }
}
