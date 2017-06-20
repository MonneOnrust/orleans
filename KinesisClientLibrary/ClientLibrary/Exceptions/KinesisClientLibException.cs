using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Exceptions
{
    /**
 * Abstract class for exceptions of the Amazon Kinesis Client Library.
 * This exception has two subclasses:
 * 1. KinesisClientLibNonRetryableException
 * 2. KinesisClientLibRetryableException.
 */
    public abstract class KinesisClientLibException : Exception
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * Constructor.
         * 
         * @param message Message of with details of the exception.
         */
        public KinesisClientLibException(String message) : base(message)
        {
        }

        /**
         * Constructor.
         * 
         * @param message Message with details of the exception.
         * @param cause Cause.
         */
        public KinesisClientLibException(String message, Exception cause) : base(message, cause)
        {
        }
    }
}
