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

    static void Main(string[] args)
    {
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

                foreach (var comandoSql in transacao.Comandos)
                    Console.WriteLine("Gravar aqui no banco");

                transacao.Status = TipoStatusTransacao.Commitada;
            }
            else if (instrucao.Tipo == TipoInstrucao.Checkpoint)
            {
                var transac = instrucao.TransacaoUsada; //aqui vai terq possibilitar multiplas transaca.
                var transacoes = Transacoes
                    .Where(m => m.IdentificadorTransacao != transac)
                    .ToList();

                Console.WriteLine($"Transação {transac} realizou REDO");
                foreach (var transacao in transacoes)
                {
                    transacao.Comandos = new List<string>();
                }
            }

            linha++;
        }
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
            if (this.Tipo == TipoInstrucao.InstrucaoUpdate)
                return null;

            var instrucao = this.Instrucao?.Trim();
            if (this.Tipo == TipoInstrucao.Checkpoint)
                instrucao = instrucao?.Replace("(", "").Replace(")", "")?.Trim();

            var transacao = instrucao?.Split(" ")?.LastOrDefault();
            return transacao;
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