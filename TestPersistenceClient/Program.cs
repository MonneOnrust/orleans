using Orleans;
using Orleans.Runtime.Configuration;
using System;
using System.Threading.Tasks;
using TestPersistenceGrainInterfaces;

namespace TestPersistenceClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press Enter to start the client...");

            Console.ReadLine();

            Init();

            Console.WriteLine("Enter s (speak), r (repeat) or q (exit)...");

            while (true)
            {
                var key = Console.ReadKey();

                if (key.Key == ConsoleKey.S)
                {
                    Speak().Wait();
                    Console.WriteLine();
                }
                if (key.Key == ConsoleKey.R)
                {
                    Repeat().Wait();
                    Console.WriteLine();
                }
                if (key.Key == ConsoleKey.Q)
                {
                    break;
                }
            }

            Console.WriteLine("Exited");
        }

        private static void Init()
        {
            GrainClient.Initialize(ClientConfiguration.LocalhostSilo());
        }

        private static async Task Speak()
        {
            var user = GrainClient.GrainFactory.GetGrain<IUserGrain>(10);
            var message = await user.Say(new Random().Next(1000).ToString());
            Console.WriteLine(message);
        }

        private static async Task Repeat()
        {
            var user = GrainClient.GrainFactory.GetGrain<IUserGrain>(10);
            var message = await user.GetLastMessage();
            Console.WriteLine(message);
        }
    }
}