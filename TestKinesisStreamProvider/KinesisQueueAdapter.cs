using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace TestKinesisStreamProvider
{
    public class KinesisQueueAdapter<TDataAdapter> : IQueueAdapter
        where TDataAdapter : IKinesisQueueDataAdapter
    {
        protected readonly string DeploymentId;
        private readonly SerializationManager serializationManager;
        protected readonly string DataConnectionString;
        protected readonly TimeSpan? MessageVisibilityTimeout;
        private readonly HashRingBasedStreamQueueMapper streamQueueMapper;
        protected readonly ConcurrentDictionary<QueueId, KinesisQueueDataManager> Queues = new ConcurrentDictionary<QueueId, KinesisQueueDataManager>();
        protected readonly IKinesisQueueDataAdapter dataAdapter;

        protected readonly Logger logger;

        public string Name { get; }
        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public KinesisQueueAdapter(
            TDataAdapter dataAdapter,
            SerializationManager serializationManager,
            HashRingBasedStreamQueueMapper streamQueueMapper,
            string dataConnectionString,
            string deploymentId,
            string providerName,
            Logger logger,
            TimeSpan? messageVisibilityTimeout = null)
        {
            if (string.IsNullOrEmpty(dataConnectionString)) throw new ArgumentNullException(nameof(dataConnectionString));
            if (string.IsNullOrEmpty(deploymentId)) throw new ArgumentNullException(nameof(deploymentId));

            this.serializationManager = serializationManager;
            DataConnectionString = dataConnectionString;
            DeploymentId = deploymentId;
            Name = providerName;
            MessageVisibilityTimeout = messageVisibilityTimeout;
            this.streamQueueMapper = streamQueueMapper;
            this.dataAdapter = dataAdapter;
            this.logger = logger;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return KinesisQueueAdapterReceiver.Create(this.serializationManager, queueId, DataConnectionString, DeploymentId, this.dataAdapter, logger, MessageVisibilityTimeout);
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null) throw new ArgumentException("KinesisQueue stream provider currently does not support non-null StreamSequenceToken.", nameof(token));

            var queueId = streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            KinesisQueueDataManager queue;
            if (!Queues.TryGetValue(queueId, out queue))
            {
                var tmpQueue = new KinesisQueueDataManager(queueId.ToString(), DeploymentId, DataConnectionString, logger, MessageVisibilityTimeout);
                await tmpQueue.InitQueueAsync();
                queue = Queues.GetOrAdd(queueId, tmpQueue);
            }
            var cloudMsg = this.dataAdapter.ToKinesisStreamMessage(streamGuid, streamNamespace, events, requestContext);
            await queue.AddQueueMessage(cloudMsg);
        }
    }
}
