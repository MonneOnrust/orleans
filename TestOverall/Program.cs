using Microsoft.WindowsAzure.Storage;
using System;
using System.Threading.Tasks;

namespace TestOverall
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            Receive().Wait();
        }

        private static async Task Receive()
        {
            var account = CloudStorageAccount.Parse(@"DefaultEndpointsProtocol=https;AccountName=testmonne;AccountKey=P6ACvcp3lacr1mZFWPV3QmYANtpkJ17iPMbFJI6Ad+gMFgG9elSvlc3qS7q9puMZSwXN0PGq3njnbIRcxo291w==;EndpointSuffix=core.windows.net");

            var queueClient = account.CreateCloudQueueClient();

            while (true)
            {
                for (int i = 0; i <= 7; i++)
                {
                    var queue = queueClient.GetQueueReference($"mydeploymentid-azurequeue-{i}");

                    var message = await queue.GetMessageAsync();

                    if (message != null)
                    {
                        Console.WriteLine("Message received: " + message.AsBytes);
                    }
                }
            }
        }
    }
}