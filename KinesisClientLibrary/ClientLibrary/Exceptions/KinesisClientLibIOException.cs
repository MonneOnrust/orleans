using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Exceptions
{
    /**
 * Thrown when we encounter issues when reading/writing information (e.g. shard information from Kinesis may not be
 * current/complete).
 */
    public class KinesisClientLibIOException : KinesisClientLibRetryableException
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * Constructor.
         * 
         * @param message Error message.
         */
        public KinesisClientLibIOException(String message) : base(message)
        {
        }

        /**
         * Constructor.
         * 
         * @param message Error message.
         * @param e Cause.
         */
        public KinesisClientLibIOException(String message, Exception e) : base(message, e)
        {
        }
    }
}
