using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using System;
using System.Collections.Generic;

namespace TestEventSourcingHost
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = ClusterConfiguration.LocalhostPrimarySilo();
            config.Globals.DeploymentId = "MySiloDeployment";
            //config.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose2;
            //config.AddMemoryStorageProvider("MemoryStorage");
            config.AddAzureBlobStorageProvider("Blob", @"DefaultEndpointsProtocol=https;AccountName=testmonne;AccountKey=P6ACvcp3lacr1mZFWPV3QmYANtpkJ17iPMbFJI6Ad+gMFgG9elSvlc3qS7q9puMZSwXN0PGq3njnbIRcxo291w==;EndpointSuffix=core.windows.net");

            //var properties = new Dictionary<string, string> { { "DataConnectionString", "Service=http://localhost:8000" }, { "UseJsonFormat", "True" } };
            //config.Globals.RegisterStorageProvider<Orleans.Storage.DynamoDBStorageProvider>("DynamoDB", properties);

            config.Globals.RegisterLogConsistencyProvider<Orleans.EventSourcing.StateStorage.LogConsistencyProvider>("StateStorage");

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