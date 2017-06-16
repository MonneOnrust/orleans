using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Streams;
using Orleans.Providers;

namespace TestKinesisStreamProvider
{
    /// <summary> Factory class for Azure Queue based stream provider.</summary>
    public class KinesisStreamShardAdapterFactory<TDataAdapter> : IQueueAdapterFactory
        where TDataAdapter : IKinesisStreamDataAdapter
    {
        private string deploymentId;
        private string dataConnectionString;
        private string providerName;
        private int cacheSize;
        private int numShards;
        private TimeSpan? messageVisibilityTimeout;
        private HashRingBasedStreamQueueMapper streamShardMapper;
        private IQueueAdapterCache adapterCache;
        private Func<TDataAdapter> adaptorFactory;

        private Logger logger;

        /// <summary>
        /// Gets the serialization manager.
        /// </summary>
        public SerializationManager SerializationManager { get; private set; }

        /// <summary>
        /// Application level failure handler override.
        /// </summary>
        protected Func<QueueId, Task<IStreamFailureHandler>> StreamFailureHandlerFactory { private get; set; }

        /// <summary> Init the factory.</summary>
        public virtual void Init(IProviderConfiguration config, string providerName, Logger logger, IServiceProvider serviceProvider)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (!config.Properties.TryGetValue("DataConnectionString", out dataConnectionString)) throw new ArgumentException($"DataConnectionString property not set");
            if (!config.Properties.TryGetValue("DeploymentId", out deploymentId)) throw new ArgumentException($"DeploymentId property not set");
            if (config.Properties.TryGetValue("VisibilityTimeout", out string messageVisibilityTimeoutRaw))
            {
                if (!TimeSpan.TryParse(messageVisibilityTimeoutRaw, out TimeSpan messageVisibilityTimeoutTemp))
                {
                    throw new ArgumentException($"Failed to parse VisibilityTimeout value '{messageVisibilityTimeoutRaw}' as a TimeSpan");
                }

                messageVisibilityTimeout = messageVisibilityTimeoutTemp;
            }
            else
            {
                messageVisibilityTimeout = null;
            }

            cacheSize = SimpleQueueAdapterCache.ParseSize(config, 4096/*AzureQueueAdapterConstants.CacheSizeDefaultValue*/);

            numShards = 8;
            if (config.Properties.TryGetValue("NumQueues", out string numShardsString))
            {
                if (!int.TryParse(numShardsString, out numShards)) throw new ArgumentException($"NumQueues invalid. Must be int");
            }

            this.providerName = providerName;
            streamShardMapper = new HashRingBasedStreamQueueMapper(numShards, providerName);
            adapterCache = new SimpleQueueAdapterCache(cacheSize, logger);
            if (StreamFailureHandlerFactory == null)
            {
                StreamFailureHandlerFactory = qid => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
            }

            this.logger = logger;

            this.SerializationManager = serviceProvider.GetRequiredService<SerializationManager>();
            this.adaptorFactory = () => ActivatorUtilities.GetServiceOrCreateInstance<TDataAdapter>(serviceProvider);
        }

        /// <summary>Creates the Kinesis Stream based adapter.</summary>
        public virtual Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new KinesisStreamShardAdapter<TDataAdapter>(this.adaptorFactory(), this.SerializationManager, streamShardMapper, dataConnectionString, deploymentId, providerName, logger, messageVisibilityTimeout);
            return Task.FromResult<IQueueAdapter>(adapter);
        }

        /// <summary>Creates the adapter cache.</summary>
        public virtual IQueueAdapterCache GetQueueAdapterCache()
        {
            return adapterCache;
        }

        /// <summary>Creates the factory stream queue mapper.</summary>
        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return streamShardMapper;
        }

        /// <summary>
        /// Creates a delivery failure handler for the specified queue.
        /// </summary>
        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return StreamFailureHandlerFactory(queueId);
        }
    }
}