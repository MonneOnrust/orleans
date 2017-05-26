using Orleans;
using System.Threading.Tasks;
using TestPersistenceGrainInterfaces;

namespace TestPersistenceGrains
{
    public class UserGrain : Grain<UserGrainState>, IUserGrain
    {
        private UserGrainState state = new UserGrainState();

        public Task<string> GetLastMessage()
        {
            return Task.FromResult(state.LastMessage);
        }

        public async Task<string> Say(string message)
        {
            message = "User said " + message;

            state.LastMessage = message;

            await base.WriteStateAsync();

            return message;
        }
    }

    public class UserGrainState
    {
        public string LastMessage { get; set; }
    }
}
