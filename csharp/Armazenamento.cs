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
    internal sealed class Entrada
    {
        [JsonPropertyName("id")] public string Id { get; set; } = "";
        [JsonPropertyName("origin")] public string Origem { get; set; } = "";
        [JsonPropertyName("kind")] public string Tipo { get; set; } = "";
        [JsonPropertyName("ts")] public double Instante { get; set; }
        [JsonPropertyName("clock")] public long Relogio { get; set; }
        [JsonPropertyName("user")] public string Usuario { get; set; } = "";
        [JsonPropertyName("channel")] public string Canal { get; set; } = "";
        [JsonPropertyName("text")] public string Texto { get; set; } = "";

        public LogEntry ParaProto()
        {
            return new LogEntry
            {
                Id = Id,
                Origem = Origem,
                Tipo = Tipo,
                Instante = Instante,
                Relogio = Relogio,
                Usuario = Usuario,
                Canal = Canal,
                Texto = Texto
            };
        }

        public static Entrada DeProto(LogEntry le)
        {
            return new Entrada
            {
                Id = le.Id,
                Origem = le.Origem,
                Tipo = le.Tipo,
                Instante = le.Instante,
                Relogio = le.Relogio,
                Usuario = le.Usuario,
                Canal = le.Canal,
                Texto = le.Texto
            };
        }
    }

    internal sealed class Armazenamento
    {
        private static readonly JsonSerializerOptions Opcoes = new JsonSerializerOptions
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        private readonly string nomeServidor;
        private readonly FileStream arquivo;
        private readonly HashSet<string> ids = new HashSet<string>();
        private readonly SortedSet<string> canais = new SortedSet<string>(StringComparer.Ordinal);

        public string Caminho { get; }
        public List<Entrada> Entradas { get; } = new List<Entrada>();

        public Armazenamento(string pastaDados, string nomeServidor)
        {
            this.nomeServidor = nomeServidor;
            Directory.CreateDirectory(pastaDados);
            Caminho = Path.Combine(pastaDados, nomeServidor + ".jsonl");
            if (File.Exists(Caminho))
            {
                foreach (string linha in File.ReadLines(Caminho, Encoding.UTF8))
                {
                    if (string.IsNullOrWhiteSpace(linha))
                    {
                        continue;
                    }
                    Entrada e = JsonSerializer.Deserialize<Entrada>(linha, Opcoes);
                    if (e != null)
                    {
                        Aplicar(e);
                    }
                }
            }
            arquivo = new FileStream(Caminho, FileMode.Append, FileAccess.Write, FileShare.Read);
        }

        private void Aplicar(Entrada e)
        {
            Entradas.Add(e);
            ids.Add(e.Id);
            if (e.Tipo == "channel")
            {
                canais.Add(e.Canal);
            }
        }

        public Entrada NovaEntrada(string tipo, double instante, long relogio, string usuario, string canal, string texto)
        {
            return new Entrada
            {
                Id = Guid.NewGuid().ToString("N"),
                Origem = nomeServidor,
                Tipo = tipo,
                Instante = instante,
                Relogio = relogio,
                Usuario = usuario,
                Canal = canal,
                Texto = texto
            };
        }

        public bool Adicionar(Entrada e)
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

        public bool TemCanal(string nome)
        {
            return canais.Contains(nome);
        }

        public List<string> NomesCanais()
        {
            return canais.ToList();
        }
    }
}
