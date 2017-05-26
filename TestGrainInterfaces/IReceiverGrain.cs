using Orleans;
using System;
using System.Threading.Tasks;

namespace TestGrainInterfaces
{
    public interface IReceiverGrain : IGrainWithGuidKey
    {
        Task BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse);

        Task StopConsuming();

        Task<int> GetNumberConsumed();
    }

    public interface IInlineReceiverGrain : IReceiverGrain
    {
    }
}
