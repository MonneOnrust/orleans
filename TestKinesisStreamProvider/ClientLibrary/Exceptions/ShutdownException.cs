using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Exceptions
{
    /**
 * The RecordProcessor instance has been shutdown (e.g. and attempts a checkpoint).
 */
    public class ShutdownException : KinesisClientLibNonRetryableException
    {
        //private static readonly long serialVersionUID = 1L;

        /**
         * @param message provides more details about the cause and potential ways to debug/address.
         */
        public ShutdownException(String message) : base(message)
        {
        }

        /**
         * @param message provides more details about the cause and potential ways to debug/address.
         * @param e Cause of the exception
         */
        public ShutdownException(String message, Exception e) : base(message, e)
        {
        }
    }
}
