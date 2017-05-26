using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using TestGrainInterfaces;

namespace TestGrains
{
    public class UserGrain : Grain, IUserGrain
    {
        private UserGrainState state = new UserGrainState();

        private IAsyncStream<int> producer;
        private int numProducedItems;
        private IDisposable producerTimer;
        internal readonly static string RequestContextKey = "RequestContextField";
        internal readonly static string RequestContextValue = "JustAString";

        public override Task OnActivateAsync()
        {
            numProducedItems = 0;
            return Task.CompletedTask;
        }

        public Task BecomeProducer(Guid streamId, string streamNamespace, string providerToUse)
        {
            IStreamProvider streamProvider = base.GetStreamProvider(providerToUse);
            producer = streamProvider.GetStream<int>(streamId, streamNamespace);
            return Task.CompletedTask;
        }

        public Task StartPeriodicProducing()
        {
            producerTimer = base.RegisterTimer(TimerCallback, null, TimeSpan.Zero, TimeSpan.FromMilliseconds(10));
            return Task.CompletedTask;
        }

        public Task StopPeriodicProducing()
        {
            producerTimer.Dispose();
            producerTimer = null;
            return Task.CompletedTask;
        }

        public Task<int> GetNumberProduced()
        {
            return Task.FromResult(numProducedItems);
        }

        public Task ClearNumberProduced()
        {
            numProducedItems = 0;
            return Task.CompletedTask;
        }

        public Task Produce()
        {
            return Fire();
        }

        private Task TimerCallback(object state)
        {
            return producerTimer != null ? Fire() : Task.CompletedTask;
        }

        private async Task Fire([CallerMemberName] string caller = null)
        {
            RequestContext.Set(RequestContextKey, RequestContextValue);
            await producer.OnNextAsync(numProducedItems);
            numProducedItems++;
        }

        public override Task OnDeactivateAsync()
        {
            return Task.CompletedTask;
        }


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