﻿class Program
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