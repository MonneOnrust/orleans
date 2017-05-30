using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;



// http://dotnet.github.io/orleans/Documentation/Orleans-Streams/Streams-Extensibility.html

namespace TestKinesisStreamProvider
{
    public interface IKinesisQueueDataAdapter
    {
        /// <summary>
        /// Creates a cloud queue message from stream event data.
        /// </summary>
        KinesisQueueMessage ToKinesisStreamMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext);

        /// <summary>
        /// Creates a batch container from a cloud queue message
        /// </summary>
        IBatchContainer FromKinesisStreamMessage(KinesisQueueMessage cloudMsg, long sequenceId);
    }

    /// <summary>
    /// Data adapter that uses types that support custom serializers (like json).
    /// </summary>
    public class KinesisQueueDataAdapter : IKinesisQueueDataAdapter, IOnDeserialized
    {
        private SerializationManager serializationManager;

        /// <summary>
        /// Initializes a new instance of the <see cref="AzureQueueDataAdapterV2"/> class.
        /// </summary>
        /// <param name="serializationManager"></param>
        public KinesisQueueDataAdapter(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager;
        }

        /// <summary>
        /// Creates a cloud queue message from stream event data.
        /// </summary>
        public KinesisQueueMessage ToKinesisStreamMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var kinesisStreamBatchMessage = new KinesisQueueBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var rawBytes = this.serializationManager.SerializeToByteArray(kinesisStreamBatchMessage);

            var kinesisStreamMessage = new KinesisQueueMessage(rawBytes);
            return kinesisStreamMessage;
        }

        /// <summary>
        /// Creates a batch container from a cloud queue message
        /// </summary>
        public IBatchContainer FromKinesisStreamMessage(KinesisQueueMessage cloudMsg, long sequenceId)
        {
            var azureQueueBatch = this.serializationManager.DeserializeFromByteArray<KinesisQueueBatchContainer>(cloudMsg.Content);
            azureQueueBatch.RealSequenceToken = new EventSequenceTokenV2(sequenceId);
            return azureQueueBatch;
        }

        void IOnDeserialized.OnDeserialized(ISerializerContext context)
        {
            this.serializationManager = context.SerializationManager;
        }
    }
}
