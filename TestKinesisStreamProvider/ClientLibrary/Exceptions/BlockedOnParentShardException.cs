using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Exceptions
{
    /**
 * Used internally in the Amazon Kinesis Client Library. Indicates that we cannot start processing data for a shard
 * because the data from the parent shard has not been completely processed (yet).
 */
    public class BlockedOnParentShardException : KinesisClientLibRetryableException
    {

        //private static readonly long serialVersionUID = 1L;

        /**
         * Constructor.
         * 
         * @param message Error message.
         */
        public BlockedOnParentShardException(String message) : base(message)
        {
        }

        /**
         * Constructor.
         * 
         * @param message Error message.
         * @param e Cause of the exception.
         */
        public BlockedOnParentShardException(String message, Exception e) : base(message, e)
        {
        }
    }
}
