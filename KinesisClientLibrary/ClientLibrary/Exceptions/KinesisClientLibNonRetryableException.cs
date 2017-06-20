using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Exceptions
{
    /**
 * Non-retryable exceptions. Simply retrying the same request/operation is not expected to succeed.
 * 
 */
    public abstract class KinesisClientLibNonRetryableException : KinesisClientLibException
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * Constructor.
         * 
         * @param message Message.
         */
        public KinesisClientLibNonRetryableException(String message) : base(message)
        {

        }

        /**
         * Constructor.
         * 
         * @param message Message.
         * @param e Cause.
         */
        public KinesisClientLibNonRetryableException(String message, Exception e) : base(message, e)
        {
        }
    }
}
