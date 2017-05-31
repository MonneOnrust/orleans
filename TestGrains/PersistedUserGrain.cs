using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using TestGrainInterfaces;
using System.Threading.Tasks;
using Orleans.Providers;

namespace TestGrains
{
    [StorageProvider(ProviderName="DynamoDB")]
    public class PersistedUserGrain : Grain<UserGrainState>, IPersistedUserGrain
    {
        private UserGrainState state = new UserGrainState();

        public Task BecomeProducer(Guid streamId, string streamNamespace, string providerToUse)
        {
            throw new NotImplementedException();
        }

        public Task ClearNumberProduced()
        {
            throw new NotImplementedException();
        }

        public Task<string> GetLastMessage()
        {
            return Task.FromResult(state.LastMessage);
        }

        public Task<int> GetNumberProduced()
        {
            throw new NotImplementedException();
        }

        public Task Produce()
        {
            throw new NotImplementedException();
        }

        public async Task<string> Say(string message)
        {
            Console.WriteLine($"User {this.GetPrimaryKeyLong()} speaking");

            message = "User said " + message;

            state.LastMessage = message;

            await base.WriteStateAsync();

            return message;
        }

        public Task StartPeriodicProducing()
        {
            throw new NotImplementedException();
        }

        public Task StopPeriodicProducing()
        {
            throw new NotImplementedException();
        }
    }
}
