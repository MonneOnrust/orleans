using Orleans.EventSourcing;
using System;
using System.Collections.Generic;
using System.Text;
using TestGrainInterfaces;
using System.Threading.Tasks;
using Orleans.Providers;
using Newtonsoft.Json;

namespace TestGrains
{
    [LogConsistencyProvider(ProviderName = "StateStorage")]
    [StorageProvider(ProviderName = "Blob")]
    public class JournaledUserGrain : JournaledGrain<JournaledUserGrainState, UserSpokeEvent>, IJournaledUserGrain
    {
        public JournaledUserGrain()
        {
            Console.WriteLine($"{this.GetType().Name} created");
        }

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
            return Task.FromResult($"User {State.ID} (version {Version}) says: {State.LastMessageSpoken}");
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
            RaiseEvent(new UserSpokeEvent { Message = message });
            await ConfirmEvents();

            return $"User {State.ID} (version {Version}) says: {message}";
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

    [Serializable]
    public class JournaledUserGrainState
    {
        public int ID { get; set; }

        [JsonProperty]
        public int AmountOfMessagesSpoken { get; set; }
        [JsonProperty]
        public string LastMessageSpoken { get; set; }

        public void Apply(UserSpokeEvent @event)
        {
            LastMessageSpoken = @event.Message;
            AmountOfMessagesSpoken++;
        }
    }

    public class UserSpokeEvent
    {
        public string Message { get; set; }
    }
}
