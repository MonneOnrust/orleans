using Orleans;
using System.Threading.Tasks;

namespace TestPersistenceGrainInterfaces
{
    public interface IUserGrain : IGrainWithIntegerKey
    {
        Task<string> GetLastMessage();
        Task<string> Say(string message);
    }
}
