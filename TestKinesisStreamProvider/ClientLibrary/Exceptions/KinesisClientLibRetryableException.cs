using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Exceptions
{
    /**
 * Retryable exceptions (e.g. transient errors). The request/operation is expected to succeed upon (back off and) retry.
 */
    public abstract class KinesisClientLibRetryableException : RuntimeException
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * Constructor.
         * 
         * @param message Message with details about the exception.
         */
        public KinesisClientLibRetryableException(String message) : base(message)
        {
        }

        /**
         * Constructor.
         * 
         * @param message Message with details about the exception.
         * @param e Cause.
         */
        public KinesisClientLibRetryableException(String message, Exception e) : base(message, e)
        {
        }
    }
}
