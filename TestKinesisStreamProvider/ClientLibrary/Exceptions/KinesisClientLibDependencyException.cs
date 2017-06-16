using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Exceptions
{
    /**
 *  This is thrown when the Amazon Kinesis Client Library encounters issues talking to its dependencies 
 *  (e.g. fetching data from Kinesis, DynamoDB table reads/writes, emitting metrics to CloudWatch).
 *  
 */
    public class KinesisClientLibDependencyException : KinesisClientLibRetryableException
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * @param message provides more details about the cause and potential ways to debug/address.
         */
        public KinesisClientLibDependencyException(String message) : base(message)
        {
        }

        /**
         * @param message provides more details about the cause and potential ways to debug/address.
         * @param e Cause of the exception
         */
        public KinesisClientLibDependencyException(String message, Exception e) : base(message, e)
        {
        }

    }
}
