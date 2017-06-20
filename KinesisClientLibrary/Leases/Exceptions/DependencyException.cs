using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Leases.Exceptions
{
    public class DependencyException : LeasingException
    {
        //private static readonly long serialVersionUID = 1L;

        public DependencyException(String message, Exception e) : base(message, e)
        {
        }
    }
}
