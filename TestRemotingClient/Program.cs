using Orleans;
using Orleans.Runtime.Configuration;
using System;
using System.Threading.Tasks;
using TestGrainInterfaces;

namespace TestRemotingClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press Enter to start the client...");

            Console.ReadLine();

            Init();

            Console.WriteLine("Enter s (speak) or q (exit)...");

            while (true)
            {
                var key = Console.ReadKey();

                if (key.Key == ConsoleKey.S)
                {
                    Speak().Wait();
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
            var config = new ClientConfiguration();
            config.DataConnectionString = @"DefaultEndpointsProtocol=https;AccountName=testmonne;AccountKey=P6ACvcp3lacr1mZFWPV3QmYANtpkJ17iPMbFJI6Ad+gMFgG9elSvlc3qS7q9puMZSwXN0PGq3njnbIRcxo291w==;EndpointSuffix=core.windows.net";
            config.DeploymentId = "Orleans-Docker";
            config.GatewayProvider = ClientConfiguration.GatewayProviderType.AzureTable;

            GrainClient.Initialize(config);
        }

        static int counter = 1;

        private static async Task Speak()
        {
            var user = GrainClient.GrainFactory.GetGrain<IUserGrain>(counter++);
            var message = await user.Say(new Random().Next(1000).ToString());
            Console.WriteLine(message);
        }
    }
}