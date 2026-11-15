using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Bbs
{
    internal sealed class Entry
    {
        [JsonPropertyName("id")] public string Id { get; set; } = "";
        [JsonPropertyName("origin")] public string Origin { get; set; } = "";
        [JsonPropertyName("kind")] public string Kind { get; set; } = "";
        [JsonPropertyName("ts")] public double Ts { get; set; }
        [JsonPropertyName("clock")] public long Clock { get; set; }
        [JsonPropertyName("user")] public string User { get; set; } = "";
        [JsonPropertyName("channel")] public string Channel { get; set; } = "";
        [JsonPropertyName("text")] public string Text { get; set; } = "";

        public LogEntry ToProto()
        {
            return new LogEntry
            {
                Id = Id,
                Origin = Origin,
                Kind = Kind,
                Timestamp = Ts,
                Clock = Clock,
                User = User,
                Channel = Channel,
                Text = Text
            };
        }

        public static Entry FromProto(LogEntry le)
        {
            return new Entry
            {
                Id = le.Id,
                Origin = le.Origin,
                Kind = le.Kind,
                Ts = le.Timestamp,
                Clock = le.Clock,
                User = le.User,
                Channel = le.Channel,
                Text = le.Text
            };
        }
    }

    internal sealed class Store
    {
        private static readonly JsonSerializerOptions Opcoes = new JsonSerializerOptions
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        private readonly string serverName;
        private readonly FileStream arquivo;
        private readonly HashSet<string> ids = new HashSet<string>();
        private readonly SortedSet<string> canais = new SortedSet<string>(StringComparer.Ordinal);

        public string Path { get; }
        public List<Entry> Entries { get; } = new List<Entry>();

        public Store(string dataDir, string serverName)
        {
            this.serverName = serverName;
            Directory.CreateDirectory(dataDir);
            Path = System.IO.Path.Combine(dataDir, serverName + ".jsonl");
            if (File.Exists(Path))
            {
                foreach (string linha in File.ReadLines(Path, Encoding.UTF8))
                {
                    if (string.IsNullOrWhiteSpace(linha))
                    {
                        continue;
                    }
                    Entry e = JsonSerializer.Deserialize<Entry>(linha, Opcoes);
                    if (e != null)
                    {
                        Aplicar(e);
                    }
                }
            }
            arquivo = new FileStream(Path, FileMode.Append, FileAccess.Write, FileShare.Read);
        }

        private void Aplicar(Entry e)
        {
            Entries.Add(e);
            ids.Add(e.Id);
            if (e.Kind == "channel")
            {
                canais.Add(e.Channel);
            }
        }

        public Entry NewEntry(string kind, double ts, long clock, string user, string channel, string text)
        {
            return new Entry
            {
                Id = Guid.NewGuid().ToString("N"),
                Origin = serverName,
                Kind = kind,
                Ts = ts,
                Clock = clock,
                User = user,
                Channel = channel,
                Text = text
            };
        }

        public bool Add(Entry e)
        {
            if (ids.Contains(e.Id))
            {
                return false;
            }
            Aplicar(e);
            byte[] linha = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(e, Opcoes) + "\n");
            arquivo.Write(linha, 0, linha.Length);
            arquivo.Flush(true);
            return true;
        }

        public List<Entry> Messages(string channel)
        {
            return Entries
                .Where(e => e.Kind == "message" && e.Channel == channel)
                .OrderBy(e => e.Clock)
                .ThenBy(e => e.Origin, StringComparer.Ordinal)
                .ThenBy(e => e.Id, StringComparer.Ordinal)
                .ToList();
        }

        public bool HasChannel(string name)
        {
            return canais.Contains(name);
        }

        public List<string> ChannelNames()
        {
            return canais.ToList();
        }
    }
}
