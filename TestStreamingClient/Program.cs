using Orleans;
using Orleans.Runtime.Configuration;
using System;
using System.Threading.Tasks;
using TestGrainInterfaces;

namespace TestStreamingClient
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Press Enter to start the client...");

            Console.ReadLine();

            Init().Wait();

            Console.WriteLine("Enter p (publish) or q (exit)...");

            while (true)
            {
                var key = Console.ReadKey();

                if(key.Key == ConsoleKey.P)
                {
                    Publish().Wait();
                    Console.WriteLine();
                }
                if (key.Key == ConsoleKey.Q)
                {
                    break;
                }
            }

            Console.WriteLine("Exited");
        }

        private static int userID = 10;
        private static Guid receiverGuid = Guid.NewGuid();
        private static Guid streamGuid = Guid.NewGuid();

        private static async Task Init()
        {
            GrainClient.Initialize(ClientConfiguration.LocalhostSilo());

            var user = GrainClient.GrainFactory.GetGrain<IUserGrain>(userID);
            await user.BecomeProducer(streamGuid, "Users", "azurequeue");

            var receiver = GrainClient.GrainFactory.GetGrain<IInlineReceiverGrain>(receiverGuid);
            await receiver.BecomeConsumer(streamGuid, "Users", "azurequeue");
        }

        private static async Task Publish()
        {
            var user = GrainClient.GrainFactory.GetGrain<IUserGrain>(userID);
            await user.Produce();
        }
    }
}