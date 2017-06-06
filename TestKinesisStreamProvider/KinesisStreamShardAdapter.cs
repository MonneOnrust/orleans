using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TestKinesisStreamProvider
{
    public class KinesisStreamShardAdapter<TDataAdapter> : IQueueAdapter
        where TDataAdapter : IKinesisStreamDataAdapter
    {
        protected readonly string DeploymentID;
        private readonly SerializationManager serializationManager;
        protected readonly string DataConnectionString;
        protected readonly TimeSpan? MessageVisibilityTimeout;
        private readonly HashRingBasedStreamQueueMapper streamShardMapper;
        protected readonly ConcurrentDictionary<QueueId, KinesisStreamShard> Shards = new ConcurrentDictionary<QueueId, KinesisStreamShard>();
        protected readonly IKinesisStreamDataAdapter dataAdapter;

        protected readonly Logger logger;

        public string Name { get; }
        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public KinesisStreamShardAdapter(
            TDataAdapter dataAdapter,
            SerializationManager serializationManager,
            HashRingBasedStreamQueueMapper streamShardMapper,
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
            DeploymentID = deploymentId;
            Name = providerName;
            MessageVisibilityTimeout = messageVisibilityTimeout;
            this.streamShardMapper = streamShardMapper;
            this.dataAdapter = dataAdapter;
            this.logger = logger;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null) throw new ArgumentException("Kinesis stream provider currently does not support non-null StreamSequenceToken.", nameof(token));

            var queueID = streamShardMapper.GetQueueForStream(streamGuid, streamNamespace);
            if (!Shards.TryGetValue(queueID, out KinesisStreamShard shardManager))
            {
                var tmpManager = new KinesisStreamShard(queueID.ToString(), DeploymentID, DataConnectionString, logger, MessageVisibilityTimeout);
                await tmpManager.InitShardAsync();
                shardManager = Shards.GetOrAdd(queueID, tmpManager);
            }
            var streamMessage = this.dataAdapter.ToKinesisStreamMessage(streamGuid, streamNamespace, events, requestContext);
            await shardManager.AddMessageAsync(streamMessage);
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return KinesisStreamShardAdapterReceiver.Create(this.serializationManager, queueId, DataConnectionString, DeploymentID, this.dataAdapter, logger, MessageVisibilityTimeout);
        }
    }
}
