using Orleans;
using System;
using System.Threading.Tasks;

namespace TestGrainInterfaces
{
    public interface IUserGrain : IGrainWithIntegerKey
    {
        Task<string> GetLastMessage();
        Task<string> Say(string message);

        #region Streaming
        Task BecomeProducer(Guid streamId, string streamNamespace, string providerToUse);

        Task StartPeriodicProducing();

        Task StopPeriodicProducing();

        Task<int> GetNumberProduced();

        Task ClearNumberProduced();
        Task Produce();
        #endregion
    }

    public interface IPersistedUserGrain : IUserGrain { }
    public interface IJournaledUserGrain : IUserGrain { }
}
