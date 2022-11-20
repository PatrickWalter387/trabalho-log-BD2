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

    static void Main(string[] args)
    {
        var logTxt = log.Trim();
        SepararInstrucoes(logTxt);
    }

    static void SepararInstrucoes(string linha)
    {
        int de = linha.IndexOf("<") + 1;
        int ate = linha.IndexOf(">");
        if (de == -1 || ate == -1)
            return;

        var result = linha.Substring(de, ate - de);
        Instrucoes.Add(new LogInstrucaoBase { Instrucao = result });

        if((ate + 1) < linha.Length)
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

    public enum TipoInstrucao
    {
        Iniciar = 1,
        Comitar = 2,
        Checkpoint = 3,
        InstrucaoUpdate = 4
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