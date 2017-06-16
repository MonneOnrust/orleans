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
    // TODO mon: implement checkpointing (sequence number save) instead of removing records from stream
    // TODO mon: implement sharding. This class used an Azure queue for sharding. With Kinesis you can use a stream shard, but you can also use multiple streams with one shard. This
    //           topic needs more design if every event gets its own stream.
    // TODO mon: prevent double processing of events by 1 app (done by KCL through shard leasing) https://github.com/awslabs/amazon-kinesis-client/blob/master/src/main/java/com/amazonaws/services/kinesis/leases/impl/LeaseManager.java

    // TODO: stop receiver if lease owner changed

    public class KinesisStreamShardAdapterReceiver : IQueueAdapterReceiver
    {
        private readonly SerializationManager serializationManager;
        private KinesisStreamShard streamManager;
        private long lastReadMessage;
        private Task outstandingTask;
        private readonly Logger logger;
        private readonly IKinesisStreamDataAdapter dataAdapter;
        private readonly List<PendingDelivery> pending;

        public QueueId Id { get; }

        public static IQueueAdapterReceiver Create(SerializationManager serializationManager, QueueId queueId, string dataConnectionString, string deploymentId, IKinesisStreamDataAdapter dataAdapter, Logger logger, TimeSpan? messageVisibilityTimeout = null)
        {
            if (queueId == null) throw new ArgumentNullException(nameof(queueId));
            if (string.IsNullOrEmpty(dataConnectionString)) throw new ArgumentNullException(nameof(dataConnectionString));
            if (string.IsNullOrEmpty(deploymentId)) throw new ArgumentNullException(nameof(deploymentId));
            if (dataAdapter == null) throw new ArgumentNullException(nameof(dataAdapter));
            if (serializationManager == null) throw new ArgumentNullException(nameof(serializationManager));
            if (logger == null) throw new ArgumentNullException(nameof(logger));

            var queue = new KinesisStreamShard(queueId.ToString(), deploymentId, dataConnectionString, logger, messageVisibilityTimeout);
            return new KinesisStreamShardAdapterReceiver(serializationManager, queueId, queue, dataAdapter, logger);
        }

        private KinesisStreamShardAdapterReceiver(SerializationManager serializationManager, QueueId queueId, KinesisStreamShard streamManager, IKinesisStreamDataAdapter dataAdapter, Logger logger)
        {
            this.Id = queueId;
            this.streamManager = streamManager;
            this.dataAdapter = dataAdapter;
            this.logger = logger; //LogManager.GetLogger(GetType().Name, LoggerType.Provider);
            this.serializationManager = serializationManager;
            this.pending = new List<PendingDelivery>();
        }

        public Task Initialize(TimeSpan timeout)
        {
            if (streamManager != null) // check in case we already shut it down.
            {
                return streamManager.InitShardAsync();
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
                // remember that we shut down so we never try to read from the stream again.
                streamManager = null;
            }
        }

        public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
        {
            try
            {
                var streamManagerRef = streamManager; // store direct ref, in case we are somehow asked to shutdown while we are receiving.    
                if (streamManagerRef == null) return new List<IBatchContainer>();

                int count = maxCount < 0 || maxCount == QueueAdapterConstants.UNLIMITED_GET_QUEUE_MSG ?
                    KinesisStreamMessage.MaxNumberOfMessagesToPeek : Math.Min(maxCount, KinesisStreamMessage.MaxNumberOfMessagesToPeek);

                var task = streamManagerRef.GetStreamMessagesAsync(count);
                outstandingTask = task;
                IEnumerable<KinesisStreamMessage> messages = await task;

                var kinesisStreamMessages = new List<IBatchContainer>();
                foreach (var message in messages)
                {
                    IBatchContainer container = this.dataAdapter.FromKinesisStreamMessage(message, lastReadMessage++);
                    kinesisStreamMessages.Add(container);
                    this.pending.Add(new PendingDelivery(container.SequenceToken, message));
                }

                return kinesisStreamMessages;
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
                var streamManagerRef = streamManager; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
                if (messages.Count == 0 || streamManagerRef == null) return;
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
                List<KinesisStreamMessage> deliveredStreamMessages = finalizedDeliveries
                    .Where(finalized => deliveredTokens.Contains(finalized.Token))
                    .Select(finalized => finalized.Message)
                    .ToList();
                if (deliveredStreamMessages.Count == 0) return;
                // delete all delivered queue messages from the queue.  Anything finalized but not delivered will show back up later
                outstandingTask = Task.WhenAll(deliveredStreamMessages.Select(streamManagerRef.DeleteQueueMessage));
                try
                {
                    await outstandingTask;
                }
                catch (Exception exc)
                {
                    logger.Warn((int)ErrorCode.AzureQueue_15, $"Exception upon DeleteQueueMessage on queue {Id}. Ignoring.", exc);
                }
            }
            finally
            {
                outstandingTask = null;
            }
        }

        private class PendingDelivery
        {
            public PendingDelivery(StreamSequenceToken token, KinesisStreamMessage message)
            {
                this.Token = token;
                this.Message = message;
            }

            public KinesisStreamMessage Message { get; }

            public StreamSequenceToken Token { get; }
        }
    }
}
