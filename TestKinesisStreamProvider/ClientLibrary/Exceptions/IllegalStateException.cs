using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Exceptions
{
    public class IllegalStateException : Exception
    {
        public IllegalStateException()
        {
        }

        public IllegalStateException(string message) : base(message)
        {
        }

        public IllegalStateException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
