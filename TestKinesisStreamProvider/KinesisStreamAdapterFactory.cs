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
    public class KinesisStreamAdapterFactory<TDataAdapter> : IQueueAdapterFactory
        where TDataAdapter : IKinesisStreamDataAdapter
    {
        private string deploymentId;
        private string dataConnectionString;
        private string providerName;
        private int cacheSize;
        private int numQueues;
        private TimeSpan? messageVisibilityTimeout;
        private HashRingBasedStreamQueueMapper streamQueueMapper;
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
            if (!config.Properties.TryGetValue("DataConnectionString", out dataConnectionString))
                throw new ArgumentException($"DataConnectionString property not set");
            if (!config.Properties.TryGetValue("DeploymentId", out deploymentId))
                throw new ArgumentException($"DeploymentId property not set");
            if (config.Properties.TryGetValue("VisibilityTimeout", out string messageVisibilityTimeoutRaw))
            {
                TimeSpan messageVisibilityTimeoutTemp;
                if (!TimeSpan.TryParse(messageVisibilityTimeoutRaw, out messageVisibilityTimeoutTemp))
                {
                    throw new ArgumentException(
                        $"Failed to parse VisibilityTimeout value '{messageVisibilityTimeoutRaw}' as a TimeSpan");
                }

                messageVisibilityTimeout = messageVisibilityTimeoutTemp;
            }
            else
            {
                messageVisibilityTimeout = null;
            }

            cacheSize = SimpleQueueAdapterCache.ParseSize(config, 4096/*AzureQueueAdapterConstants.CacheSizeDefaultValue*/);

            numQueues = 8;
            if (config.Properties.TryGetValue("NumQueues", out string numQueuesString))
            {
                if (!int.TryParse(numQueuesString, out numQueues))
                    throw new ArgumentException($"NumQueues invalid. Must be int");
            }

            this.providerName = providerName;
            streamQueueMapper = new HashRingBasedStreamQueueMapper(numQueues, providerName);
            adapterCache = new SimpleQueueAdapterCache(cacheSize, logger);
            if (StreamFailureHandlerFactory == null)
            {
                StreamFailureHandlerFactory =
                    qid => Task.FromResult<IStreamFailureHandler>(new NoOpStreamDeliveryFailureHandler());
            }

            this.logger = logger;

            this.SerializationManager = serviceProvider.GetRequiredService<SerializationManager>();
            this.adaptorFactory = () => ActivatorUtilities.GetServiceOrCreateInstance<TDataAdapter>(serviceProvider);
        }

        /// <summary>Creates the Azure Queue based adapter.</summary>
        public virtual Task<IQueueAdapter> CreateAdapter()
        {
            var adapter = new KinesisStreamAdapter<TDataAdapter>(this.adaptorFactory(), this.SerializationManager, streamQueueMapper, dataConnectionString, deploymentId, providerName, logger, messageVisibilityTimeout);
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
            return streamQueueMapper;
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