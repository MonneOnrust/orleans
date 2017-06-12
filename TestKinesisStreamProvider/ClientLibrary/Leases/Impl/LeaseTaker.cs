using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestKinesisStreamProvider.ClientLibrary.Leases.Exceptions;
using TestKinesisStreamProvider.ClientLibrary.Leases.Interfaces;
using TestKinesisStreamProvider.ClientLibrary.Leases.Utils;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Impl
{
    public class LeaseTaker<T> : ILeaseTaker<T> where T : Lease
    {
        private static readonly int TAKE_RETRIES = 3;
        private static readonly int SCAN_RETRIES = 1;

        private readonly Dictionary<string, T> allLeases = new Dictionary<string, T>();
        private readonly long leaseDurationNanos;
        private int maxLeasesForWorker = int.MaxValue;
        private int maxLeasesToStealAtOneTime = 1;
        private long lastScanTimeNanos = 0L;
        private readonly ILeaseManager<T> leaseManager;

        public string WorkerIdentifier { get; private set; }


        public LeaseTaker(ILeaseManager<T> leaseManager, String workerIdentifier, long leaseDurationMillis)
        {
            this.leaseManager = leaseManager;
            this.WorkerIdentifier = workerIdentifier;
            this.leaseDurationNanos = leaseDurationMillis * 1000000;
        }

        /// <summary>
        /// Worker will not acquire more than the specified max number of leases even if there are more
        /// shards that need to be processed.This can be used in scenarios where a worker is resource constrained or
        /// to prevent lease thrashing when small number of workers pick up all leases for small amount of time during
        /// deployment.
        /// Note that setting a low value may cause data loss (e.g. if there aren't enough Workers to make progress on all
        /// shards). When setting the value for this property, one must ensure enough workers are present to process
        /// shards and should consider future resharding, child shards that may be blocked on parent shards, some workers
        /// becoming unhealthy, etc.
        /// </summary>
        /// <param name="maxLeasesForWorker">Max leases this Worker can handle at a time</param>
        /// <returns></returns>
        public LeaseTaker<T> WithMaxLeasesForWorker(int maxLeasesForWorker)
        {
            if (maxLeasesForWorker <= 0)
            {
                throw new ArgumentException("maxLeasesForWorker should be >= 1");
            }
            this.maxLeasesForWorker = maxLeasesForWorker;
            return this;
        }

        /// <summary>
        /// Max leases to steal from a more loaded Worker at one time(for load balancing).
        ///* Setting this to a higher number can allow for faster load convergence (e.g.during deployments, cold starts),
        ///* but can cause higher churn in the system.
        /// </summary>
        /// <param name="maxLeasesToStealAtOneTime">Steal up to this many leases at one time (for load balancing)</param>
        /// <returns></returns>
        public LeaseTaker<T> WithMaxLeasesToStealAtOneTime(int maxLeasesToStealAtOneTime)
        {
            if (maxLeasesToStealAtOneTime <= 0)
            {
                throw new ArgumentException("maxLeasesToStealAtOneTime should be >= 1");
            }
            this.maxLeasesToStealAtOneTime = maxLeasesToStealAtOneTime;
            return this;
        }


        public Dictionary<String, T> TakeLeases()
        {
            return TakeLeases(() => DateTime.Now.Ticks * 100);
        }

        public Dictionary<string, T> TakeLeases(Func<long> timeProvider)
        {
            lock (this)
            {
                var takenLeases = new Dictionary<string, T>();

                //bool success = false;

                ProvisionedThroughputException lastException = null;

                try
                {
                    for (int i = 1; i <= SCAN_RETRIES; i++)
                    {
                        try
                        {
                            UpdateAllLeases(timeProvider);
                            //success = true;
                        }
                        catch (ProvisionedThroughputException e)
                        {
                            Trace.TraceInformation(String.Format("Worker {0} could not find expired leases on try {1} out of {2}", WorkerIdentifier, i, TAKE_RETRIES));
                            lastException = e;
                        }
                    }
                }
                finally
                {
                    //MetricsHelper.addSuccessAndLatency("ListLeases", startTime, success, MetricsLevel.DETAILED);
                }

                if (lastException != null)
                {
                    Trace.TraceError("Worker " + WorkerIdentifier + " could not scan leases table, aborting takeLeases. Exception caught by last retry: {0}", lastException);
                    return takenLeases;
                }

                List<T> expiredLeases = GetExpiredLeases();

                var leasesToTake = ComputeLeasesToTake(expiredLeases);
                var untakenLeaseKeys = new List<String>();

                foreach (var lease in leasesToTake)
                {
                    String leaseKey = lease.LeaseKey;

                    //success = false;
                    try
                    {
                        for (int i = 1; i <= TAKE_RETRIES; i++)
                        {
                            try
                            {
                                if (leaseManager.TakeLease(lease, WorkerIdentifier))
                                {
                                    lease.LastCounterIncrementNanos = DateTime.Now.Ticks * 100;
                                    takenLeases[leaseKey] = lease;
                                }
                                else
                                {
                                    untakenLeaseKeys.Add(leaseKey);
                                }

                                //success = true;
                                break;
                            }
                            catch (ProvisionedThroughputException)
                            {
                                Trace.TraceInformation(String.Format("Could not take lease with key {0} for worker {1} on try {2} out of {3} due to capacity", leaseKey, WorkerIdentifier, i, TAKE_RETRIES));
                            }
                        }
                    }
                    finally
                    {
                        //MetricsHelper.addSuccessAndLatency("TakeLease", startTime, success, MetricsLevel.DETAILED);
                    }
                }

                if (takenLeases.Count > 0)
                {
                    Trace.TraceInformation(String.Format("Worker {0} successfully took {1} leases: {2}",
                            WorkerIdentifier,
                            takenLeases.Count,
                            takenLeases.Keys.Aggregate((k1, k2) => k1 + ", " + k2)));
                }

                if (untakenLeaseKeys.Count > 0)
                {
                    Trace.TraceInformation(String.Format("Worker {0} failed to take {1} leases: {2}",
                            WorkerIdentifier,
                            untakenLeaseKeys.Count,
                            untakenLeaseKeys.Aggregate((k1, k2) => k1 + ", " + k2)));
                }

                //MetricsHelper.getMetricsScope().addData("TakenLeases", takenLeases.size(), StandardUnit.Count, MetricsLevel.SUMMARY);

                return takenLeases;
            }
        }

        private void UpdateAllLeases(Func<long> timeProvider)
        {
            List<T> freshList = leaseManager.ListLeases();
            try
            {
                lastScanTimeNanos = timeProvider();
            }
            catch (Exception e)
            {
                throw new DependencyException("Exception caught from timeProvider", e);
            }

            // This set will hold the lease keys not updated by the previous listLeases call.
            var notUpdated = new HashSet<String>(allLeases.Keys);

            // Iterate over all leases, finding ones to try to acquire that haven't changed since the last iteration
            foreach (T lease in freshList)
            {
                String leaseKey = lease.LeaseKey;

                allLeases.TryGetValue(leaseKey, out T oldLease);
                allLeases[leaseKey] = lease;
                notUpdated.Remove(leaseKey);

                if (oldLease != null)
                {
                    // If we've seen this lease before...
                    if (oldLease.LeaseCounter == lease.LeaseCounter)
                    {
                        // ...and the counter hasn't changed, propagate the lastRenewalNanos time from the old lease
                        lease.LastCounterIncrementNanos = oldLease.LastCounterIncrementNanos;
                    }
                    else
                    {
                        // ...and the counter has changed, set lastRenewalNanos to the time of the scan.
                        lease.LastCounterIncrementNanos = lastScanTimeNanos;
                    }
                }
                else
                {
                    if (lease.LeaseOwner == null)
                    {
                        // if this new lease is unowned, it's never been renewed.
                        lease.LastCounterIncrementNanos = 0L;

                        Trace.WriteLine("Treating new lease with key " + leaseKey + " as never renewed because it is new and unowned.");
                    }
                    else
                    {
                        // if this new lease is owned, treat it as renewed as of the scan
                        lease.LastCounterIncrementNanos = lastScanTimeNanos;

                        Trace.WriteLine("Treating new lease with key " + leaseKey + " as recently renewed because it is new and owned.");
                    }
                }
            }

            // Remove dead leases from allLeases
            foreach (String key in notUpdated)
            {
                allLeases.Remove(key);
            }
        }

        /// <summary>
        /// returns list of leases that were expired as of our last scan.
        /// </summary>
        /// <returns></returns>
        private List<T> GetExpiredLeases()
        {
            var expiredLeases = new List<T>();

            foreach (T lease in allLeases.Values)
            {
                if (lease.IsExpired(leaseDurationNanos, lastScanTimeNanos))
                {
                    expiredLeases.Add(lease);
                }
            }

            return expiredLeases;
        }

        /**
    * Compute the number of leases I should try to take based on the state of the system.
    * 
    * @param allLeases map of shardId to lease containing all leases
    * @param expiredLeases list of leases we determined to be expired
    * @return set of leases to take.
    */
        private List<T> ComputeLeasesToTake(List<T> expiredLeases)
        {
            Dictionary<String, int> leaseCounts = ComputeLeaseCounts(expiredLeases);
            var leasesToTake = new List<T>();
            //IMetricsScope metrics = MetricsHelper.getMetricsScope();

            int numLeases = allLeases.Count;
            int numWorkers = leaseCounts.Count;

            if (numLeases == 0)
            {
                // If there are no leases, I shouldn't try to take any.
                return leasesToTake;
            }

            int target;
            if (numWorkers >= numLeases)
            {
                // If we have n leases and n or more workers, each worker can have up to 1 lease, including myself.
                target = 1;
            }
            else
            {
                /*
                 * numWorkers must be < numLeases.
                 * 
                 * Our target for each worker is numLeases / numWorkers (+1 if numWorkers doesn't evenly divide numLeases)
                 */
                target = numLeases / numWorkers + (numLeases % numWorkers == 0 ? 0 : 1);

                // Spill over is the number of leases this worker should have claimed, but did not because it would
                // exceed the max allowed for this worker.
                int leaseSpillover = Math.Max(0, target - maxLeasesForWorker);
                if (target > maxLeasesForWorker)
                {
                    Trace.TraceWarning(String.Format("Worker {0} target is {1} leases and maxLeasesForWorker is {2}. Resetting target to {3}, lease spillover is {4}. Note that some shards may not be processed if no other workers are able to pick them up.",
                            WorkerIdentifier,
                            target,
                            maxLeasesForWorker,
                            maxLeasesForWorker,
                            leaseSpillover));
                    target = maxLeasesForWorker;
                }
                //metrics.addData("LeaseSpillover", leaseSpillover, StandardUnit.Count, MetricsLevel.SUMMARY);
            }

            int myCount = leaseCounts[WorkerIdentifier];
            int numLeasesToReachTarget = target - myCount;

            if (numLeasesToReachTarget <= 0)
            {
                // If we don't need anything, return the empty set.
                return leasesToTake;
            }

            // Shuffle expiredLeases so workers don't all try to contend for the same leases.
            expiredLeases.Shuffle();

            int originalExpiredLeasesSize = expiredLeases.Count;
            if (expiredLeases.Count > 0)
            {
                // If we have expired leases, get up to <needed> leases from expiredLeases
                for (; numLeasesToReachTarget > 0 && expiredLeases.Count > 0; numLeasesToReachTarget--)
                {
                    var expiredLease = expiredLeases.First();
                    leasesToTake.Add(expiredLease);
                    expiredLeases.RemoveAt(0);
                }
            }
            else
            {
                // If there are no expired leases and we need a lease, consider stealing.
                List<T> leasesToSteal = ChooseLeasesToSteal(leaseCounts, numLeasesToReachTarget, target);
                foreach (T leaseToSteal in leasesToSteal)
                {
                    Trace.TraceInformation(String.Format("Worker {0} needed {1} leases but none were expired, so it will steal lease {2} from {3}",
                            WorkerIdentifier,
                            numLeasesToReachTarget,
                            leaseToSteal.LeaseKey,
                            leaseToSteal.LeaseOwner));
                    leasesToTake.Add(leaseToSteal);
                }
            }

            if (leasesToTake.Count != 0)
            {
                Trace.TraceInformation(String.Format("Worker {0} saw {1} total leases, {2} available leases, {3} workers. Target is {4} leases, I have {5} leases, I will take {6} leases",
                        WorkerIdentifier,
                        numLeases,
                        originalExpiredLeasesSize,
                        numWorkers,
                        target,
                        myCount,
                        leasesToTake.Count));
            }

            //metrics.addData("TotalLeases", numLeases, StandardUnit.Count, MetricsLevel.DETAILED);
            //metrics.addData("ExpiredLeases", originalExpiredLeasesSize, StandardUnit.Count, MetricsLevel.SUMMARY);
            //metrics.addData("NumWorkers", numWorkers, StandardUnit.Count, MetricsLevel.SUMMARY);
            //metrics.addData("NeededLeases", numLeasesToReachTarget, StandardUnit.Count, MetricsLevel.DETAILED);
            //metrics.addData("LeasesToTake", leasesToTake.size(), StandardUnit.Count, MetricsLevel.DETAILED);

            return leasesToTake;
        }

        /// <summary>
        /// Count leases by host. Always includes myself, but otherwise only includes hosts that are currently holding leases.
        /// </summary>
        /// <param name="expiredLeases">list of leases that are currently expired</param>
        /// <returns>map of workerIdentifier to lease count</returns>
        private Dictionary<String, int> ComputeLeaseCounts(List<T> expiredLeases)
        {
            var leaseCounts = new Dictionary<String, int>();

            // Compute the number of leases per worker by looking through allLeases and ignoring leases that have expired.
            foreach (T lease in allLeases.Values)
            {
                if (!expiredLeases.Contains(lease))
                {
                    String leaseOwner = lease.LeaseOwner;
                    var present = leaseCounts.TryGetValue(leaseOwner, out int oldCount);
                    if (!present)
                    {
                        leaseCounts[leaseOwner] = 1;
                    }
                    else
                    {
                        leaseCounts[leaseOwner] = oldCount + 1;
                    }
                }
            }

            // If I have no leases, I wasn't represented in leaseCounts. Let's fix that.
            var myWorkerPresent = leaseCounts.TryGetValue(WorkerIdentifier, out int myCount);
            if (!myWorkerPresent)
            {
                myCount = 0;
                leaseCounts[WorkerIdentifier] = myCount;
            }

            return leaseCounts;
        }

        /// <summary>
        /// Choose leases to steal by randomly selecting one or more (up to max) from the most loaded worker.
        /// Stealing rules:
        /// 
        /// Steal up to maxLeasesToStealAtOneTime leases from the most loaded worker if
        /// a) he has > target leases and I need >= 1 leases : steal min(leases needed, maxLeasesToStealAtOneTime)
        /// b) he has == target leases and I need > 1 leases : steal 1
        /// 
        /// </summary>
        /// <param name="leaseCounts">map of workerIdentifier to lease count</param>
        /// <param name="needed"># of leases needed to reach the target leases for the worker</param>
        /// <param name="target">target # of leases per worker</param>
        /// <returns>Leases to steal, or empty list if we should not steal</returns>
        private List<T> ChooseLeasesToSteal(Dictionary<String, int> leaseCounts, int needed, int target)
        {
            var leasesToSteal = new List<T>();

            KeyValuePair<String, int>? mostLoadedWorker = null;
            // Find the most loaded worker
            foreach (KeyValuePair<String, int> worker in leaseCounts)
            {
                if (mostLoadedWorker == null || mostLoadedWorker.Value.Value < worker.Value)
                {
                    mostLoadedWorker = worker;
                }
            }

            int numLeasesToSteal = 0;
            if ((mostLoadedWorker.Value.Value >= target) && (needed > 0))
            {
                int leasesOverTarget = mostLoadedWorker.Value.Value - target;
                numLeasesToSteal = Math.Min(needed, leasesOverTarget);
                // steal 1 if we need > 1 and max loaded worker has target leases.
                if ((needed > 1) && (numLeasesToSteal == 0))
                {
                    numLeasesToSteal = 1;
                }
                numLeasesToSteal = Math.Min(numLeasesToSteal, maxLeasesToStealAtOneTime);
            }

            if (numLeasesToSteal <= 0)
            {
                Trace.WriteLine(String.Format("Worker {0} not stealing from most loaded worker {1}.  He has {2}, target is {3}, and I need {4}",
                            WorkerIdentifier,
                            mostLoadedWorker.Value.Key,
                            mostLoadedWorker.Value.Value,
                            target,
                            needed));

                return leasesToSteal;
            }
            else
            {
                Trace.WriteLine(String.Format("Worker {0} will attempt to steal {1} leases from most loaded worker {2}. He has {3} leases, target is {4}, I need {5}, maxLeasesToSteatAtOneTime is {6}.",
                               WorkerIdentifier,
                               numLeasesToSteal,
                               mostLoadedWorker.Value.Key,
                               mostLoadedWorker.Value.Value,
                               target,
                               needed,
                               maxLeasesToStealAtOneTime));
            }

            String mostLoadedWorkerIdentifier = mostLoadedWorker.Value.Key;
            List<T> candidates = new List<T>();
            // Collect leases belonging to that worker
            foreach (T lease in allLeases.Values)
            {
                if (mostLoadedWorkerIdentifier.Equals(lease.LeaseOwner))
                {
                    candidates.Add(lease);
                }
            }

            // Return random ones
            candidates.Shuffle();
            int toIndex = Math.Min(candidates.Count, numLeasesToSteal);
            leasesToSteal.AddRange(candidates.Take(toIndex));

            return leasesToSteal;
        }
    }
}
