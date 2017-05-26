using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using Orleans.SqlUtils;
using Orleans.Storage;
using System;
using System.Collections.Generic;

namespace TestHostFramework
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = ClusterConfiguration.LocalhostPrimarySilo();
            config.Globals.DeploymentId = "MySiloDeployment";
            //config.Defaults.DefaultTraceLevel = Orleans.Runtime.Severity.Verbose2;
            //config.AddMemoryStorageProvider("Default");

            //config.AddAdoNetStorageProvider("Default", @"Server=tcp:testmonne.database.secure.windows.net,1433;Initial Catalog=testmonne;Persist Security Info=False;User ID=monne;Password=p@ssw0rd;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;", AdoNetSerializationFormat.Json);

            //config.Globals.AdoInvariant = AdoNetInvariants.InvariantNamePostgreSql;
            //config.AddAdoNetStorageProvider("Default", @"User ID=postgres;Password=mysecretpassword;Host=localhost;Port=5432;Database=eventstoretestdb;Pooling=true;Timeout=15", AdoNetSerializationFormat.Xml);

            var props = new Dictionary<string, string>
            {
                [AdoNetStorageProvider.DataConnectionInvariantPropertyName] = AdoNetInvariants.InvariantNamePostgreSql,
                [AdoNetStorageProvider.DataConnectionStringPropertyName] = @"User ID=postgres;Password=mysecretpassword;Host=localhost;Port=5432;Database=eventstoretestdb;Pooling=true;Timeout=15",
            };
            config.Globals.RegisterStorageProvider<AdoNetStorageProvider>("Default", props);

            //config.AddAzureBlobStorageProvider("Default", @"DefaultEndpointsProtocol=https;AccountName=testmonne;AccountKey=P6ACvcp3lacr1mZFWPV3QmYANtpkJ17iPMbFJI6Ad+gMFgG9elSvlc3qS7q9puMZSwXN0PGq3njnbIRcxo291w==;EndpointSuffix=core.windows.net");
            //var provider = new Orleans.Storage.AzureBlobStorage();

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
