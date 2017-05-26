using Orleans;
using System.Threading.Tasks;
using TestRemotingGrainInterfaces;

namespace TestRemotingHost
{
    public class UserGrain : Grain, IUserGrain
    {
        private UserGrainState state = new UserGrainState();

        public Task<string> GetLastMessage()
        {
            return Task.FromResult(state.LastMessage);
        }

        public async Task<string> Say(string message)
        {
            System.Console.WriteLine($"User {this.GetPrimaryKeyLong()} speaking");

            message = "User said " + message;

            state.LastMessage = message;

            return message;
        }
    }

    public class UserGrainState
    {
        public string LastMessage { get; set; }
    }
}
