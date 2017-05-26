using Orleans;
using Orleans.Runtime;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using TestGrainInterfaces;

namespace TestGrains
{
    public class InlineReceiverGrain : Grain, IInlineReceiverGrain
    {
        private IAsyncObservable<int> consumer;
        internal int numConsumedItems;
        //internal Logger logger;
        private StreamSubscriptionHandle<int> consumerHandle;

        public override Task OnActivateAsync()
        {
            //logger = base.GetLogger("SampleStreaming_InlineConsumerGrain " + base.IdentityString);
            //logger.Info("OnActivateAsync");
            numConsumedItems = 0;
            consumerHandle = null;
            return Task.CompletedTask;
        }

        public async Task BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse)
        {
            //logger.Info("BecomeConsumer");
            IStreamProvider streamProvider = base.GetStreamProvider(providerToUse);
            consumer = streamProvider.GetStream<int>(streamId, streamNamespace);
            consumerHandle = await consumer.SubscribeAsync(OnNextAsync, OnErrorAsync, OnCompletedAsync);
        }

        public async Task StopConsuming()
        {
            //logger.Info("StopConsuming");
            if (consumerHandle != null)
            {
                await consumerHandle.UnsubscribeAsync();
                //consumerHandle.Dispose();
                consumerHandle = null;
            }
        }

        public Task<int> GetNumberConsumed()
        {
            return Task.FromResult(numConsumedItems);
        }

        public Task OnNextAsync(int item, StreamSequenceToken token = null)
        {
            //logger.Info("OnNextAsync({0}{1})", item, token != null ? token.ToString() : "null");
            numConsumedItems++;
            Console.WriteLine("ReceiverGrain: Event received: " + item);
            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            //logger.Info("OnCompletedAsync()");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            //logger.Info("OnErrorAsync({0})", ex);
            return Task.CompletedTask;
        }

        public override Task OnDeactivateAsync()
        {
            //logger.Info("OnDeactivateAsync");
            return Task.CompletedTask;
        }
    }


    public class ReceiverGrain : Grain, IReceiverGrain
    {
        private IAsyncObservable<int> consumer;
        internal int numConsumedItems;
        //internal Logger logger;
        private IAsyncObserver<int> consumerObserver;
        private StreamSubscriptionHandle<int> consumerHandle;

        public override Task OnActivateAsync()
        {
            //logger = base.GetLogger("SampleStreaming_ConsumerGrain " + base.IdentityString);
            //logger.Info("OnActivateAsync");
            numConsumedItems = 0;
            consumerHandle = null;
            return Task.CompletedTask;
        }

        public async Task BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse)
        {
            //logger.Info("BecomeConsumer");
            consumerObserver = new ReceiverObserver<int>(this);
            IStreamProvider streamProvider = base.GetStreamProvider(providerToUse);
            consumer = streamProvider.GetStream<int>(streamId, streamNamespace);
            consumerHandle = await consumer.SubscribeAsync(consumerObserver);
        }

        public async Task StopConsuming()
        {
            //logger.Info("StopConsuming");
            if (consumerHandle != null)
            {
                await consumerHandle.UnsubscribeAsync();
                consumerHandle = null;
            }
        }

        public Task<int> GetNumberConsumed()
        {
            return Task.FromResult(numConsumedItems);
        }

        public override Task OnDeactivateAsync()
        {
            //logger.Info("OnDeactivateAsync");
            return Task.CompletedTask;
        }
    }

    internal class ReceiverObserver<T> : IAsyncObserver<T>
    {
        private readonly ReceiverGrain hostingGrain;

        internal ReceiverObserver(ReceiverGrain hostingGrain)
        {
            this.hostingGrain = hostingGrain;
        }

        public Task OnNextAsync(T item, StreamSequenceToken token = null)
        {
            //hostingGrain.logger.Info("OnNextAsync(item={0}, token={1})", item, token != null ? token.ToString() : "null");
            hostingGrain.numConsumedItems++;
            return Task.CompletedTask;
        }

        public Task OnCompletedAsync()
        {
            //hostingGrain.logger.Info("OnCompletedAsync()");
            return Task.CompletedTask;
        }

        public Task OnErrorAsync(Exception ex)
        {
            //hostingGrain.logger.Info("OnErrorAsync({0})", ex);
            return Task.CompletedTask;
        }
    }
}
