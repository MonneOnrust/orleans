using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace TestKinesisStreamProvider
{
    public class KinesisStreamAdapter<TDataAdapter> : IQueueAdapter
        where TDataAdapter : IKinesisStreamDataAdapter
    {
        protected readonly string DeploymentID;
        private readonly SerializationManager serializationManager;
        protected readonly string DataConnectionString;
        protected readonly TimeSpan? MessageVisibilityTimeout;
        private readonly HashRingBasedStreamQueueMapper streamQueueMapper;
        protected readonly ConcurrentDictionary<QueueId, KinesisStream> Streams = new ConcurrentDictionary<QueueId, KinesisStream>();
        protected readonly IKinesisStreamDataAdapter dataAdapter;

        protected readonly Logger logger;

        public string Name { get; }
        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public KinesisStreamAdapter(
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
            DeploymentID = deploymentId;
            Name = providerName;
            MessageVisibilityTimeout = messageVisibilityTimeout;
            this.streamQueueMapper = streamQueueMapper;
            this.dataAdapter = dataAdapter;
            this.logger = logger;
        }

        public async Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            if (token != null) throw new ArgumentException("Kinesis stream provider currently does not support non-null StreamSequenceToken.", nameof(token));

            var queueID = streamQueueMapper.GetQueueForStream(streamGuid, streamNamespace);
            if (!Streams.TryGetValue(queueID, out KinesisStream streamManager))
            {
                var tmpManager = new KinesisStream(queueID.ToString(), DeploymentID, DataConnectionString, logger, MessageVisibilityTimeout);
                await tmpManager.InitStreamAsync();
                streamManager = Streams.GetOrAdd(queueID, tmpManager);
            }
            var streamMessage = this.dataAdapter.ToKinesisStreamMessage(streamGuid, streamNamespace, events, requestContext);
            await streamManager.AddStreamMessageAsync(streamMessage);
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return KinesisStreamAdapterReceiver.Create(this.serializationManager, queueId, DataConnectionString, DeploymentID, this.dataAdapter, logger, MessageVisibilityTimeout);
        }
    }
}
