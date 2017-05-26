using Orleans;
using System;
using System.Threading.Tasks;

namespace TestRemotingGrainInterfaces
{
    public interface IUserGrain : IGrainWithIntegerKey
    {
        Task<string> GetLastMessage();
        Task<string> Say(string message);
    }
}
