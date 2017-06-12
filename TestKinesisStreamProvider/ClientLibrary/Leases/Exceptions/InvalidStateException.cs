using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Exceptions
{
    /**
  * Indicates that a lease operation has failed because DynamoDB is an invalid state. The most common example is failing
  * to create the DynamoDB table before doing any lease operations.
  */
    public class InvalidStateException : LeasingException
    {

        private static readonly long serialVersionUID = 1L;

        public InvalidStateException(String message, Exception e) : base(message, e)
        {
        }

        public InvalidStateException(String message) : base(message)
        {
        }
    }
}
