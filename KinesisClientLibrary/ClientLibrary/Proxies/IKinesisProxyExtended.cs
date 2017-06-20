using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Proxies
{
    /**
 * Kinesis proxy interface extended with addition method(s). Operates on a
 * single stream (set up at initialization).
 * 
 */
    public interface IKinesisProxyExtended : IKinesisProxy
    {

        /**
         * Get the Shard corresponding to shardId associated with this
         * IKinesisProxy.
         * 
         * @param shardId
         *            Fetch the Shard with this given shardId
         * @return the Shard with the given shardId
         */
        Shard GetShard(String shardId);
    }
}
