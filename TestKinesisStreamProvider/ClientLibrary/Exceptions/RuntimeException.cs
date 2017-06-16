using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Exceptions
{
    /// <summary>
    /// This is a standard Java class, but is explicitly emitted here, because exceptions in the KCL derive from it and (generic) RuntimeExceptions are caught. 
    /// This could go wrong if a RuntimeException was thrown outside of the KCL, which in the .NET variant will not be caught.
    /// </summary>
    public class RuntimeException : Exception
    {
        public RuntimeException()
        {
        }

        public RuntimeException(string message) : base(message)
        {
        }

        public RuntimeException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
