using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Exceptions
{
    public class LeasingException : Exception
    {
        public LeasingException(String message, Exception e) : base(message, e)
        { }

        public LeasingException(String message) : base(message)
        {
        }

        //private static readonly long serialVersionUID = 1L;
    }
}
