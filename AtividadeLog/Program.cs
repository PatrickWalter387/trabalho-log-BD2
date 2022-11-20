using Npgsql;

class Program
{
    static string log = @"
<start T1>
<T1,1, A,20,500>
<start T2>
<commit T1>
<CKPT (T2)>
<T2,2, A,20,50>
<start T3>
<start T4>
<commit T2>
<T4,1, B,55,100>
";

    public static List<LogInstrucaoBase> Instrucoes { get; set; } = new List<LogInstrucaoBase>();
    public static List<Transacao> Transacoes { get; set; } = new List<Transacao>();
    static string configBanco = "Host=localhost;Username=postgres;Password=postgres;Database=atividade_log";

    public static Dictionary<string, List<int>> metadado = new Dictionary<string, List<int>>();

    static void Main(string[] args)
    {
        metadado.Add("A", new List<int> { 20, 20, 77 });
        metadado.Add("B", new List<int> { 55, 30, 771 });
        ///////

        using var con = new NpgsqlConnection(configBanco);
        con.Open();
        using var cmd = new NpgsqlCommand();
        cmd.Connection = con;

        _CriarBanco(cmd);

        var logTxt = log.Trim();
        SepararInstrucoes(logTxt);

        var linha = 1;
        foreach (var instrucao in Instrucoes)
        {
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
                    UPDATE tabela SET {instrucao.InstrucaoUpdate.Coluna} = ${instrucao.InstrucaoUpdate.ValorNovo}
                    WHERE tabela.Id = {instrucao.InstrucaoUpdate.IdTupla};
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

                foreach (var transac in instrucao.TransacoesUsadasCheckpoint)
                    Console.WriteLine($"Transação {transac} realizou REDO");

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
                Console.WriteLine($"{transacao.IdentificadorTransacao} - RODOU SQL");
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
        var itens = instrucao.Split(",");

        this.Transacao = itens[0];
        this.IdTupla = int.Parse(itens[1]);
        this.Coluna = itens[2];
        this.ValorAntigo = int.Parse(itens[3]);
        this.ValorNovo = int.Parse(itens[4]);
    }

    public string? Transacao { get; set; }
    public int IdTupla { get; set; }
    public string? Coluna { get; set; }
    public int ValorAntigo { get; set; }
    public int ValorNovo { get; set; }
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
    InstrucaoUpdate = 4
}

public enum TipoStatusTransacao
{
    Ativa = 1,
    Commitada = 2
}