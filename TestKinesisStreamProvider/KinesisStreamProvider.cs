using Orleans.Providers.Streams.Common;

namespace TestKinesisStreamProvider
{
    public class KinesisStreamProvider : PersistentStreamProvider<KinesisStreamShardAdapterFactory<KinesisStreamDataAdapter>>
    {
    }
}
