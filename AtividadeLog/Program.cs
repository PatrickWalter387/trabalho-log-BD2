class Program
{
    string log = @"
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

    static void Main(string[] args)
    {
        Console.WriteLine("teste");
    }
}

class LogInstrucaoUpdate
{
    public LogInstrucao(string linha)
    {
        linha = linha.Replace("<", "").Replace(">", "").Replace(" ", "");
        var itens = linha.Split(",");

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