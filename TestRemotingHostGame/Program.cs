using Orleans.Providers.Streams.AzureQueue;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Host;
using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace TestStreamingHostGame
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ClusterConfiguration();
            config.Globals.DataConnectionString = @"DefaultEndpointsProtocol=https;AccountName=testmonne;AccountKey=P6ACvcp3lacr1mZFWPV3QmYANtpkJ17iPMbFJI6Ad+gMFgG9elSvlc3qS7q9puMZSwXN0PGq3njnbIRcxo291w==;EndpointSuffix=core.windows.net";
            config.Globals.DeploymentId = "Orleans-Docker";
            //config.Globals.DeploymentId = "mydeploymentid";
            config.Globals.LivenessType = GlobalConfiguration.LivenessProviderType.AzureTable;
            config.Globals.ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.AzureTable;
            config.Defaults.PropagateActivityId = true;
            config.Defaults.ProxyGatewayEndpoint = new IPEndPoint(IPAddress.Any, 10400);
            config.Defaults.Port = 10301;
            var ips = Dns.GetHostAddressesAsync(Dns.GetHostName()).Result;
            config.Defaults.HostNameOrIPAddress = ips.Skip(1).FirstOrDefault()?.ToString();

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