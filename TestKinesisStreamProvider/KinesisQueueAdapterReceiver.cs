using Orleans;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TestKinesisStreamProvider
{
    public class KinesisQueueAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly SerializationManager serializationManager;
        private KinesisQueueDataManager queue;
        private long lastReadMessage;
        private Task outstandingTask;
        private readonly Logger logger;
        private readonly IKinesisQueueDataAdapter dataAdapter;
        private readonly List<PendingDelivery> pending;

        public QueueId Id { get; }

        public static IQueueAdapterReceiver Create(SerializationManager serializationManager, QueueId queueId, string dataConnectionString, string deploymentId, IKinesisQueueDataAdapter dataAdapter, Logger logger, TimeSpan? messageVisibilityTimeout = null)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (string.IsNullOrEmpty(dataConnectionString)) throw new ArgumentNullException(nameof(dataConnectionString));
            if (string.IsNullOrEmpty(deploymentId)) throw new ArgumentNullException(nameof(deploymentId));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));
            if (serializationManager == null) throw new ArgumentNullException(nameof(serializationManager));

            var queue = new KinesisQueueDataManager(queueId.ToString(), deploymentId, dataConnectionString, logger, messageVisibilityTimeout);
            return new KinesisQueueAdapterReceiver(serializationManager, queueId, queue, dataAdapter, logger);
        }

        private KinesisQueueAdapterReceiver(SerializationManager serializationManager, QueueId queueId, KinesisQueueDataManager queue, IKinesisQueueDataAdapter dataAdapter, Logger logger)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));

            Id = queueId;
            this.serializationManager = serializationManager;
            this.queue = queue;
            this.dataAdapter = dataAdapter;
            this.logger = logger; //LogManager.GetLogger(GetType().Name, LoggerType.Provider);
            this.pending = new List<PendingDelivery>();
        }

        public Task Initialize(TimeSpan timeout)
        {
            if (queue != null) // check in case we already shut it down.
            {
                return queue.InitQueueAsync();
            }
            return Task.CompletedTask;
        }

        public async Task Shutdown(TimeSpan timeout)
        {
            try
            {
                // await the last storage operation, so after we shutdown and stop this receiver we don't get async operation completions from pending storage operations.
                if (outstandingTask != null)
                    await outstandingTask;
            }
            finally
            {
                // remember that we shut down so we never try to read from the queue again.
                queue = null;
            }
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var queueRef = queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.    
                if (queueRef == null) return new List<IBatchContainer>();

                int count = maxCount < 0 || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG ?
                    KinesisQueueMessage.MaxNumberOfMessagesToPeek : Math.Min(maxCount, KinesisQueueMessage.MaxNumberOfMessagesToPeek);

                var task = queueRef.GetQueueMessages(count);
                outstandingTask = task;
                IEnumerable<KinesisQueueMessage> messages = await task;

                List<IBatchContainer> azureQueueMessages = new List<IBatchContainer>();
                foreach (var message in messages)
                {
                    IBatchContainer container = this.dataAdapter.FromKinesisStreamMessage(message, lastReadMessage++);
                    azureQueueMessages.Add(container);
                    this.pending.Add(new PendingDelivery(container.SequenceToken, message));
                }

                return azureQueueMessages;
            }
            finally
            {
                outstandingTask = null;
            }
        }

        public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
        {
            try
            {
                var queueRef = queue; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
                if (messages.Count == 0 || queueRef == null) return;
                // get sequence tokens of delivered messages
                List<StreamSequenceToken> deliveredTokens = messages.Select(message => message.SequenceToken).ToList();
                // find oldest delivered message
                StreamSequenceToken oldest = deliveredTokens.Max();
                // finalize all pending messages at or befor the oldest
                List<PendingDelivery> finalizedDeliveries = pending
                    .Where(pendingDelivery => !pendingDelivery.Token.Newer(oldest))
                    .ToList();
                if (finalizedDeliveries.Count == 0) return;
                // remove all finalized deliveries from pending, regardless of if it was delivered or not.
                pending.RemoveRange(0, finalizedDeliveries.Count);
                // get the queue messages for all finalized deliveries that were delivered.
                List<KinesisQueueMessage> deliveredCloudQueueMessages = finalizedDeliveries
                    .Where(finalized => deliveredTokens.Contains(finalized.Token))
                    .Select(finalized => finalized.Message)
                    .ToList();
                if (deliveredCloudQueueMessages.Count == 0) return;
                // delete all delivered queue messages from the queue.  Anything finalized but not delivered will show back up later
                outstandingTask = Task.WhenAll(deliveredCloudQueueMessages.Select(queueRef.DeleteQueueMessage));
                try
                {
                    await outstandingTask;
                }
                catch (Exception exc)
                {
                    logger.Warn((int)ErrorCode.AzureQueue_15,
                        $"Exception upon DeleteQueueMessage on queue {Id}. Ignoring.", exc);
                }
            }
            finally
            {
                outstandingTask = null;
            }
        }

        private class PendingDelivery
        {
            public PendingDelivery(StreamSequenceToken token, KinesisQueueMessage message)
            {
                this.Token = token;
                this.Message = message;
            }

            public KinesisQueueMessage Message { get; }

            public StreamSequenceToken Token { get; }
        }
    }
}
