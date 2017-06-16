using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Exceptions
{
    /**
 * Thrown when requests are throttled by a service (e.g. DynamoDB when storing a checkpoint).
 */
    public class ThrottlingException : KinesisClientLibRetryableException
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * @param message Message about what was throttled and any guidance we can provide.
         */
        public ThrottlingException(String message) : base(message)
        {
        }

        /**
         * @param message provides more details about the cause and potential ways to debug/address.
         * @param e Underlying cause of the exception.
         */
        public ThrottlingException(String message, Exception e) : base(message, e)
        {
        }

    }
}
