using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Exceptions
{
    /**
 * This is thrown when the Amazon Kinesis Client Library encounters issues with its internal state (e.g. DynamoDB table
 * is not found).
 */
    public class InvalidStateException : KinesisClientLibNonRetryableException
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * @param message provides more details about the cause and potential ways to debug/address.
         */
        public InvalidStateException(String message) : base(message)
        {
        }

        /**
         * @param message provides more details about the cause and potential ways to debug/address.
         * @param e Cause of the exception
         */
        public InvalidStateException(String message, Exception e) : base(message, e)
        {
        }
    }
}