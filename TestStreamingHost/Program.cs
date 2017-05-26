using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using System;

namespace TestStreamingHostUser
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = ClusterConfiguration.LocalhostPrimarySilo();
            config.Globals.DeploymentId = "mydeploymentid";
            //config.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose2;
            config.AddMemoryStorageProvider("PubSubStore");
            //config.AddSimpleMessageStreamProvider("SMS");
            config.AddAzureQueueStreamProvider("azurequeue", @"DefaultEndpointsProtocol=https;AccountName=testmonne;AccountKey=P6ACvcp3lacr1mZFWPV3QmYANtpkJ17iPMbFJI6Ad+gMFgG9elSvlc3qS7q9puMZSwXN0PGq3njnbIRcxo291w==;EndpointSuffix=core.windows.net");
            //config.AddAzureQueueStreamProviderV2();


            var siloHost = new SiloHost("MySilo", config);
            siloHost.InitializeOrleansSilo();
            siloHost.StartOrleansSilo();

            Console.WriteLine("Silo host started");
            Console.WriteLine("Press enter to stop the silo host");
            Console.ReadLine();

            siloHost.StopOrleansSilo();

            Console.WriteLine("Silo host stopped");
            Console.WriteLine("Press enter to exit");
            Console.ReadLine();
        }
    }
}