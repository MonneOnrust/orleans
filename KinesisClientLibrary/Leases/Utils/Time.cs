using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Leases.Utils
{
    public static class Time
    {
        public static long NanoTime { get { return DateTime.Now.Ticks * 100; } }
        public static long MilliTime { get { return (int)(DateTime.Now.Ticks / 10000); } }
    }
}
