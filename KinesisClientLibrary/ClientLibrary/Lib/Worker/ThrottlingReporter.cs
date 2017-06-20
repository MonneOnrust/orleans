using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    //    @RequiredArgsConstructor
    //@CommonsLog
    public class ThrottlingReporter
    {
        private readonly int maxConsecutiveWarnThrottles;
        private readonly String shardId;

        private int consecutiveThrottles = 0;

        public ThrottlingReporter(int maxConsecutiveWarnThrottles, string shardId)
        {
            this.maxConsecutiveWarnThrottles = maxConsecutiveWarnThrottles;
            this.shardId = shardId;
        }

        public void Throttled()
        {
            consecutiveThrottles++;
            String message = "Shard '" + shardId + "' has been throttled " + consecutiveThrottles + " consecutively";

            if (consecutiveThrottles > maxConsecutiveWarnThrottles)
            {
                Trace.TraceError(message);
                //GetLog().error(message);
            }
            else
            {
                Trace.TraceWarning(message);
                //GetLog().warn(message);
            }
        }

        public void Success()
        {
            consecutiveThrottles = 0;
        }

        //protected Log GetLog()
        //{
        //    return log;
        //}
    }
}