using Orleans.Providers.Streams.Common;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using System;
using System.Collections.Generic;
using TestKinesisStreamProvider;

namespace TestStreamingHostUser
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = ClusterConfiguration.LocalhostPrimarySilo();
            config.Globals.DeploymentId = "mydeploymentid";
            config.AddMemoryStorageProvider("PubSubStore");
            //config.AddAzureQueueStreamProvider("azurequeue", @"DefaultEndpointsProtocol=https;AccountName=testmonne;AccountKey=P6ACvcp3lacr1mZFWPV3QmYANtpkJ17iPMbFJI6Ad+gMFgG9elSvlc3qS7q9puMZSwXN0PGq3njnbIRcxo291w==;EndpointSuffix=core.windows.net");
            //config.AddAzureQueueStreamProviderV2();
            AddKinesisStreamProvider(config);

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

        private static void AddKinesisStreamProvider(ClusterConfiguration config)
        {
            var properties = GetKinesisStreamProviderProperties("kinesis", "connectionstring", 1, "mydeploymentid", 4096, PersistentStreamProviderState.None);
            config.Globals.RegisterStreamProvider<KinesisQueueStreamProvider>("kinesis", properties);
        }

        private static Dictionary<string, string> GetKinesisStreamProviderProperties(string providerName, string connectionString, int numberOfQueues, string deploymentId, int cacheSize , PersistentStreamProviderState startupState/*, PersistentStreamProviderConfig persistentStreamProviderConfig*/)
        {
            if (string.IsNullOrWhiteSpace(providerName)) throw new ArgumentNullException(nameof(providerName));
            if (numberOfQueues < 1) throw new ArgumentOutOfRangeException(nameof(numberOfQueues));

            var properties = new Dictionary<string, string>
            {
                { "DataConnectionString", connectionString },
                { "NumQueues", numberOfQueues.ToString() },
                { "DeploymentId", deploymentId },
                { "CacheSize", cacheSize.ToString() },
                { "StartupState", startupState.ToString() },
            };

            //persistentStreamProviderConfig?.WriteProperties(properties);

            return properties;
        }
    }
}