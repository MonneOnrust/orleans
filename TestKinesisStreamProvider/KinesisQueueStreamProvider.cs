using Orleans.Providers.Streams.Common;
using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider
{
    public class KinesisQueueStreamProvider : PersistentStreamProvider<KinesisQueueAdapterFactory<KinesisQueueDataAdapter>>
    {
    }

    //public class KinesisQueueStreamProvider<TDataAdapter> : PersistentStreamProvider<KinesisQueueAdapterFactory<TDataAdapter>>
    //    where TDataAdapter : IKinesisQueueDataAdapter
    //{
    //}
}
