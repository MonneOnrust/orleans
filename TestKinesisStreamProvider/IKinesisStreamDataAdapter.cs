using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;



// http://dotnet.github.io/orleans/Documentation/Orleans-Streams/Streams-Extensibility.html

namespace TestKinesisStreamProvider
{
    public interface IKinesisStreamDataAdapter
    {
        /// <summary>
        /// Creates a cloud queue message from stream event data.
        /// </summary>
        KinesisStreamMessage ToKinesisStreamMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext);

        /// <summary>
        /// Creates a batch container from a stream message
        /// </summary>
        IBatchContainer FromKinesisStreamMessage(KinesisStreamMessage streamMessage, long sequenceId);
    }

    /// <summary>
    /// Data adapter that uses types that support custom serializers (like json).
    /// </summary>
    public class KinesisStreamDataAdapter : IKinesisStreamDataAdapter, IOnDeserialized
    {
        private SerializationManager serializationManager;

        /// <summary>
        /// Initializes a new instance of the <see cref="KinesisStreamDataAdapter"/> class.
        /// </summary>
        /// <param name="serializationManager"></param>
        public KinesisStreamDataAdapter(SerializationManager serializationManager)
        {
            this.serializationManager = serializationManager;
        }

        /// <summary>
        /// Creates a cloud queue message from stream event data.
        /// </summary>
        public KinesisStreamMessage ToKinesisStreamMessage<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, Dictionary<string, object> requestContext)
        {
            var kinesisStreamBatchMessage = new KinesisStreamBatchContainer(streamGuid, streamNamespace, events.Cast<object>().ToList(), requestContext);
            var rawBytes = this.serializationManager.SerializeToByteArray(kinesisStreamBatchMessage);

            return new KinesisStreamMessage(rawBytes);
        }

        /// <summary>
        /// Creates a batch container from a cloud queue message
        /// </summary>
        public IBatchContainer FromKinesisStreamMessage(KinesisStreamMessage streamMessage, long sequenceId)
        {
            var kinesisStreamBatch = this.serializationManager.DeserializeFromByteArray<KinesisStreamBatchContainer>(streamMessage.Content);
            kinesisStreamBatch.RealSequenceToken = new EventSequenceTokenV2(sequenceId);
            return kinesisStreamBatch;
        }

        void IOnDeserialized.OnDeserialized(ISerializerContext context)
        {
            this.serializationManager = context.SerializationManager;
        }
    }
}
