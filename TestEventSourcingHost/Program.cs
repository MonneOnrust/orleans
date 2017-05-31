using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using System;

namespace TestEventSourcingHost
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = ClusterConfiguration.LocalhostPrimarySilo();
            config.Globals.DeploymentId = "MySiloDeployment";
            //config.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose2;
            config.AddMemoryStorageProvider("MemoryStorage");
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