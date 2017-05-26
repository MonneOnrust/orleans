using Orleans;
using System;
using System.Threading.Tasks;

namespace TestStreamingGrainInterfaces
{
    public interface IUserGrain : IGrainWithGuidKey
    {
        Task BecomeProducer(Guid streamId, string streamNamespace, string providerToUse);

        Task StartPeriodicProducing();

        Task StopPeriodicProducing();

        Task<int> GetNumberProduced();

        Task ClearNumberProduced();
        Task Produce();
    }
}
