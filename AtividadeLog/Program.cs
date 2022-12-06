using Npgsql;
using System.Text.Json;

class Program
{
    public static List<LogInstrucaoBase> Instrucoes { get; set; } = new List<LogInstrucaoBase>();
    public static List<Transacao> Transacoes { get; set; } = new List<Transacao>();
    static string configBanco = "Host=localhost;Username=postgres;Password=postgres;Database=atividade_log";

    public static Dictionary<string, List<int>> metadado = new Dictionary<string, List<int>>();

    static void Main(string[] args)
    {
        string log = File.ReadAllText($"C:\\Users\\patri\\source\\repos\\aaaaa\\AtividadeLog\\AtividadeLog\\entradaLog");

        using (StreamReader r = new StreamReader("C:\\Users\\patri\\source\\repos\\aaaaa\\AtividadeLog\\AtividadeLog\\metadado.json"))
        {
            string json = r.ReadToEnd();
            var jsonConvertido = JsonSerializer.Deserialize<JsonViewModel>(json);
            metadado = jsonConvertido.INITIAL;
        }

        using var con = new NpgsqlConnection(configBanco);
        con.Open();
        using var cmd = new NpgsqlCommand();
        cmd.Connection = con;

        _CriarBanco(cmd);

        var logTxt = log.Trim();
        SepararInstrucoes(logTxt);

        var ultimoCheckpoint = Instrucoes.LastOrDefault(m => m.Tipo == TipoInstrucao.Checkpoint);
        var indiceCheckpoint = Instrucoes.FindLastIndex(m => m.Tipo == TipoInstrucao.Checkpoint);

        var linha = 1;
        foreach (var instrucao in Instrucoes)
        {
            //pula instrucoes anteriores que nao estao no checkpoint
            if(ultimoCheckpoint != null && (linha <= indiceCheckpoint && !ultimoCheckpoint.TransacoesUsadasCheckpoint.Contains((instrucao?.TransacaoUsada ?? instrucao?.InstrucaoUpdate?.Transacao) ?? "")))
            {
                linha++;
                continue;
            }

            if (instrucao.Tipo == TipoInstrucao.Iniciar)
            {
                var transacao = new Transacao
                {
                    IdentificadorTransacao = instrucao.TransacaoUsada,
                    Status = TipoStatusTransacao.Ativa
                };
                Transacoes.Add(transacao);
            }
            else if (instrucao.Tipo == TipoInstrucao.InstrucaoUpdate)
            {
                var transacao = Transacoes
                    .Where(m => m.IdentificadorTransacao == instrucao.InstrucaoUpdate.Transacao)
                    .Where(m => m.Status == TipoStatusTransacao.Ativa)
                    .FirstOrDefault();

                if (transacao == null)
                {
                    Console.WriteLine($"Update na linha {linha} não executado, transação informada inválida!");
                    return;
                }

                var sql = @$"
                    UPDATE adam_sandler_is_o_melhor SET ""{instrucao.InstrucaoUpdate.Coluna}"" = {instrucao.InstrucaoUpdate.ValorNovo}
                    WHERE adam_sandler_is_o_melhor.Id = {instrucao.InstrucaoUpdate.IdTupla};
                ";
                transacao.Comandos.Add(sql);
            }
            else if (instrucao.Tipo == TipoInstrucao.Comitar)
            {
                var transacao = Transacoes
                    .Where(m => m.IdentificadorTransacao == instrucao.TransacaoUsada)
                    .Where(m => m.Status == TipoStatusTransacao.Ativa)
                    .FirstOrDefault();

                if (transacao == null)
                {
                    Console.WriteLine($"Commit na linha {linha} não executado, transação informada inválida!");
                    return;
                }

                transacao.Status = TipoStatusTransacao.Commitada;
            }
            else if (instrucao.Tipo == TipoInstrucao.Checkpoint)
            {
                var transacoes = Transacoes
                    .Where(m => !instrucao.TransacoesUsadasCheckpoint.Contains(m.IdentificadorTransacao))
                    .ToList();

                foreach (var transacao in transacoes)
                {
                    transacao.Comandos = new List<string>();
                }
            }

            linha++;
        }

        foreach (var transacao in Transacoes.Where(m => m.Status == TipoStatusTransacao.Commitada))
        {
            foreach (var sql in transacao.Comandos)
            {
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }

        foreach (var transacao in Transacoes)
        {
            if(ultimoCheckpoint != null)
            {
                if(ultimoCheckpoint!.TransacoesUsadasCheckpoint.Contains(transacao!.IdentificadorTransacao) && transacao?.Status == TipoStatusTransacao.Commitada)
                {
                    Console.WriteLine($"Transação {transacao.IdentificadorTransacao} realizou REDO");
                }
                else
                {
                    Console.WriteLine($"Transação {transacao.IdentificadorTransacao} não realizou REDO");
                }
            }
            else
            {
                if(transacao?.Status == TipoStatusTransacao.Commitada)
                {
                    Console.WriteLine($"Transação {transacao.IdentificadorTransacao} realizou REDO");
                }
                else
                {
                    Console.WriteLine($"Transação {transacao.IdentificadorTransacao} não realizou REDO");
                }
            }
        }

        //Imprimir banco
        cmd.CommandText = "SELECT * FROM adam_sandler_is_o_melhor ORDER BY id";
        var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            Console.WriteLine($"Row {reader[0]} - A: {reader[1]} B: {reader[2]}");
        }
    }

    static void _CriarBanco(NpgsqlCommand cmd)
    {
        cmd.CommandText = "DROP TABLE IF EXISTS adam_sandler_is_o_melhor";
        cmd.ExecuteNonQuery();

        var itens = new List<(string, List<int>)>();
        foreach (var item in metadado)
            itens.Add(($"\"{item.Key}\"", item.Value));

        var colunas = itens.Select(m => m.Item1).ToList();
        cmd.CommandText = $"CREATE TABLE adam_sandler_is_o_melhor(Id serial PRIMARY KEY, {string.Join(",", colunas.Select(campo => $"{campo} int NULL"))})";
        cmd.ExecuteNonQuery();

        var inserts = "";
        var valores = itens.Select(m => m.Item2.ToArray()).ToArray();
        for (int coluna = 0; coluna < valores.FirstOrDefault()?.Length; coluna++)
        {
            var valuesInsert = new List<int>();
            for (int linha = 0; linha < valores.GetLength(0); linha++)
                valuesInsert.Add(valores[linha][coluna]);

            inserts += $"INSERT INTO adam_sandler_is_o_melhor({string.Join(",", colunas)}) VALUES({string.Join(",", valuesInsert)}); ";
        }

        cmd.CommandText = inserts;
        cmd.ExecuteNonQuery();
    }

    static void SepararInstrucoes(string linha)
    {
        int de = linha.IndexOf("<") + 1;
        int ate = linha.IndexOf(">");
        if (de == -1 || ate == -1)
            return;

        var result = linha.Substring(de, ate - de);
        Instrucoes.Add(new LogInstrucaoBase { Instrucao = result });

        if ((ate + 1) < linha.Length)
            SepararInstrucoes(linha.Substring(ate + 1));
    }
}

class LogInstrucaoBase
{
    public string? Instrucao { get; set; }

    public string? PalavraChave
    {
        get
        {
            return this.Instrucao?.Trim()?.Split(" ")?.FirstOrDefault();
        }
    }

    public string? TransacaoUsada
    {
        get
        {
            if (this.Tipo == TipoInstrucao.InstrucaoUpdate || this.Tipo == TipoInstrucao.Checkpoint)
                return null;

            var instrucao = this.Instrucao?.Trim();
            if (this.Tipo == TipoInstrucao.Checkpoint)
                instrucao = instrucao?.Replace("(", "").Replace(")", "")?.Trim();

            var transacao = instrucao?.Split(" ")?.LastOrDefault();
            return transacao;
        }
    }

    public List<string> TransacoesUsadasCheckpoint
    {
        get
        {
            var instrucao = this.Instrucao?.Trim();
            int de = instrucao.IndexOf("(") + 1;
            int ate = instrucao.IndexOf(")");
            var result = instrucao.Substring(de, ate - de);

            return result.Replace(" ", "").Split(",").ToList();
        }
    }

    public TipoInstrucao Tipo
    {
        get
        {
            switch (this.PalavraChave)
            {
                case "start":
                    return TipoInstrucao.Iniciar;

                case "commit":
                    return TipoInstrucao.Comitar;

                case "CKPT":
                    return TipoInstrucao.Checkpoint;

                case "crash":
                    return TipoInstrucao.Crash;

                default:
                    return TipoInstrucao.InstrucaoUpdate;
            }
        }
    }

    public LogInstrucaoUpdate? InstrucaoUpdate
    {
        get
        {
            if (this.Tipo == TipoInstrucao.InstrucaoUpdate && this.Instrucao?.Count(c => c == ',') == 4)
            {
                var instrucaoUpdate = new LogInstrucaoUpdate(this.Instrucao);
                return instrucaoUpdate;
            }
            else
            {
                return null;
            }
        }
    }
}

class LogInstrucaoUpdate
{
    public LogInstrucaoUpdate(string instrucao)
    {
        var itens = instrucao.Trim().Split(",");

        this.Transacao = itens[0].Trim();
        this.IdTupla = int.Parse(itens[1].Trim());
        this.Coluna = itens[2].Trim();
        this.ValorAntigo = int.Parse(itens[3].Trim());
        this.ValorNovo = int.Parse(itens[4].Trim());
    }

    public string? Transacao { get; set; }
    public int IdTupla { get; set; }
    public string? Coluna { get; set; }
    public int ValorAntigo { get; set; }
    public int ValorNovo { get; set; }
}

class JsonViewModel
{
    public Dictionary<string, List<int>> INITIAL { get; set; }
}

class Transacao
{
    public string? IdentificadorTransacao { get; set; }
    public TipoStatusTransacao Status { get; set; }
    public List<string> Comandos { get; set; } = new List<string>();
}

public enum TipoInstrucao
{
    Iniciar = 1,
    Comitar = 2,
    Checkpoint = 3,
    InstrucaoUpdate = 4,
    Crash = 5
}

public enum TipoStatusTransacao
{
    Ativa = 1,
    Commitada = 2
}