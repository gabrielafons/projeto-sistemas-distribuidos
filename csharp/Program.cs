using System;

namespace Bbs
{
    internal static class Program
    {
        private static int Main(string[] args)
        {
            string modo = args.Length > 0 ? args[0] : "server";
            if (modo == "client")
            {
                new Client().Run();
            }
            else
            {
                new Server().Run();
            }
            return 0;
        }
    }
}
