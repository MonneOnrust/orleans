using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using KinesisClientLibrary.ClientLibrary.Leases.Exceptions;
using KinesisClientLibrary.ClientLibrary.Leases.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Utils;

namespace KinesisClientLibrary.ClientLibrary.Leases.Impl
{
    public class LeaseRenewer<T> : ILeaseRenewer<T> where T : Lease
    {
        private static readonly int RENEWAL_RETRIES = 2;

        private readonly ILeaseManager<T> leaseManager;
        private readonly ConcurrentDictionary<String, T> ownedLeases = new ConcurrentDictionary<String, T>();
        private readonly long leaseDurationNanos;

        public string WorkerIdentifier { get; private set; }


        /// <summary>
        ///  Constructor.
        /// </summary>
        /// <param name="leaseManager">LeaseManager to use</param>
        /// <param name="workerIdentifier">identifier of this worker</param>
        /// <param name="leaseDurationMillis">duration of a lease in milliseconds</param>
        /// <param name="executorService">ExecutorService to use for renewing leases in parallel</param>
        public LeaseRenewer(ILeaseManager<T> leaseManager, String workerIdentifier, long leaseDurationMillis)
        {
            this.leaseManager = leaseManager;
            this.WorkerIdentifier = workerIdentifier;
            this.leaseDurationNanos = leaseDurationMillis * 1000000;
        }

        public void Initialize()
        {
            var leases = leaseManager.ListLeases();
            var myLeases = new List<T>();
            bool renewEvenIfExpired = true;

            foreach (T lease in leases)
            {
                if (WorkerIdentifier.Equals(lease.LeaseOwner))
                {
                    Trace.TraceInformation(String.Format(" Worker {0} found lease {1}", WorkerIdentifier, lease));
                    // Okay to renew even if lease is expired, because we start with an empty list and we add the lease to
                    // our list only after a successful renew. So we don't need to worry about the edge case where we could
                    // continue renewing a lease after signaling a lease loss to the application.
                    if (RenewLease(lease, renewEvenIfExpired))
                    {
                        myLeases.Add(lease);
                    }
                }
                else
                {
                    Trace.WriteLine(String.Format("Worker {0} ignoring lease {1} ", WorkerIdentifier, lease));
                }
            }

            AddLeasesToRenew(myLeases);
        }

        public void RenewLeases()
        {
            // Due to the eventually consistent nature of ConcurrentNavigableMap iterators, this log entry may become
            // inaccurate during iteration.
            Trace.WriteLine(String.Format("Worker {0} holding {1} leases: {2}", WorkerIdentifier, ownedLeases.Count, ownedLeases));

            /*
             * Lease renewals are done in parallel so many leases can be renewed for short lease fail over time
             * configuration. In this case, metrics scope is also shared across different threads, so scope must be thread
             * safe.
             */
            //IMetricsScope renewLeaseTaskMetricsScope = new ThreadSafeMetricsDelegatingScope(MetricsHelper.getMetricsScope());

            /*
             * We iterate in descending order here so that the synchronized(lease) inside renewLease doesn't "lead" calls
             * to getCurrentlyHeldLeases. They'll still cross paths, but they won't interleave their executions.
             */
            int lostLeases = 0;
            var renewLeaseTasks = new List<Task<bool>>();
            foreach (T lease in ownedLeases.OrderByDescending(l => l.Key).Select(l => l.Value).ToList())
            {
                var task = Task.Run(() => RenewLease(lease));

                //var task = new Task<bool>(l => RenewLease((T)l), lease);

                renewLeaseTasks.Add(task);

                //ThreadPool.QueueUserWorkItem(new WaitCallback(o => task.RunSynchronously()));
            }
            int leasesInUnknownState = 0;
            Exception lastException = null;
            foreach (Task<Boolean> renewLeaseTask in renewLeaseTasks)
            {
                try
                {
                    renewLeaseTask.Wait(); // TODO test exceptions

                    if (!renewLeaseTask.Result)
                    {
                        lostLeases++;
                    }
                }
                //catch (ThreadInterruptedException)
                //{
                //    Trace.TraceInformation("Interrupted while waiting for a lease to renew.");
                //    leasesInUnknownState += 1;
                //    Thread.CurrentThread.interrupt();
                //}
                //catch (ExecutionException e)
                //{
                //    Trace.TraceError("Encountered an exception while renewing a lease.", e.getCause());
                //    leasesInUnknownState += 1;
                //    lastException = e;
                //}
                catch (Exception ex)
                {
                    Trace.TraceError("Encountered an exception while renewing a lease: " + ex.ToString());
                    leasesInUnknownState += 1;
                    lastException = ex;
                }
            }

            //renewLeaseTaskMetricsScope.addData("LostLeases", lostLeases, StandardUnit.Count, MetricsLevel.SUMMARY);
            //renewLeaseTaskMetricsScope.addData("CurrentLeases", ownedLeases.size(), StandardUnit.Count, MetricsLevel.SUMMARY);
            if (leasesInUnknownState > 0)
            {
                throw new DependencyException(String.Format("Encountered an exception while renewing leases. The number of leases which might not have been renewed is {0}", leasesInUnknownState), lastException);
            }
        }

        private bool RenewLease(T lease)
        {
            return RenewLease(lease, false);
        }

        private bool RenewLease(T lease, bool renewEvenIfExpired)
        {
            String leaseKey = lease.LeaseKey;

            //bool success = false;
            bool renewedLease = false;
            //long startTime = System.currentTimeMillis();
            try
            {
                for (int i = 1; i <= RENEWAL_RETRIES; i++)
                {
                    try
                    {
                        lock (lease)
                        {
                            // Don't renew expired lease during regular renewals. getCopyOfHeldLease may have returned null
                            // triggering the application processing to treat this as a lost lease (fail checkpoint with
                            // ShutdownException).
                            if (renewEvenIfExpired || !lease.IsExpired(leaseDurationNanos, Time.NanoTime))
                            {
                                renewedLease = leaseManager.RenewLease(lease);
                            }
                            if (renewedLease)
                            {
                                lease.LastCounterIncrementNanos = Time.NanoTime;
                            }
                        }

                        if (renewedLease)
                        {
                            Trace.WriteLine(String.Format("Worker {0} successfully renewed lease with key {1}", WorkerIdentifier, leaseKey));
                        }
                        else
                        {
                            Trace.TraceInformation(String.Format("Worker {0} lost lease with key {1}", WorkerIdentifier, leaseKey));
                            ownedLeases.TryRemove(leaseKey, out T tempLease);
                        }

                        //success = true;
                        break;
                    }
                    catch (ProvisionedThroughputException)
                    {
                        Trace.TraceInformation(String.Format("Worker {0} could not renew lease with key {1} on try {2} out of {3} due to capacity", WorkerIdentifier, leaseKey, i, RENEWAL_RETRIES));
                    }
                }
            }
            finally
            {
                //MetricsHelper.addSuccessAndLatency("RenewLease", startTime, success, MetricsLevel.DETAILED);
            }

            return renewedLease;
        }

        public void AddLeasesToRenew(List<T> newLeases)
        {
            VerifyNotNull(newLeases, "newLeases cannot be null");

            foreach (T lease in newLeases)
            {
                //if (!lease.LastCounterIncrementNanos.HasValue)
                //{
                //    Trace.TraceInformation(String.Format("AddLeasesToRenew ignoring lease with key {0} because it does not have LastCounterIncrementNanos set", lease.LeaseKey));
                //    continue;
                //}

                T authoritativeLease = lease.Copy<T>();

                /*
                 * Assign a concurrency token when we add this to the set of currently owned leases. This ensures that
                 * every time we acquire a lease, it gets a new concurrency token.
                 */
                authoritativeLease.ConcurrencyToken = Guid.NewGuid();
                ownedLeases[authoritativeLease.LeaseKey] = authoritativeLease;
            }
        }

        public void ClearCurrentlyHeldLeases()
        {
            ownedLeases.Clear();
        }

        public Dictionary<String, T> GetCurrentlyHeldLeases()
        {
            var result = new Dictionary<String, T>();
            long now = Time.NanoTime;

            foreach (String leaseKey in ownedLeases.Keys)
            {
                T copy = GetCopyOfHeldLease(leaseKey, now);
                if (copy != null)
                {
                    result[copy.LeaseKey] = copy;
                }
            }

            return result;
        }

        public T GetCurrentlyHeldLease(String leaseKey)
        {
            return GetCopyOfHeldLease(leaseKey, Time.NanoTime);
        }

        /// <summary>
        /// Internal method to return a lease with a specific lease key only if we currently hold it.
        /// </summary>
        /// <param name="leaseKey">key of lease to return</param>
        /// <param name="now">current timestamp for old-ness checking</param>
        /// <returns>non-authoritative copy of the held lease, or null if we don't currently hold it</returns>
        private T GetCopyOfHeldLease(String leaseKey, long now)
        {
            ownedLeases.TryGetValue(leaseKey, out T authoritativeLease);
            if (authoritativeLease == null)
            {
                return null;
            }
            else
            {
                T copy = null;
                lock (authoritativeLease)
                {
                    copy = authoritativeLease.Copy<T>();
                }

                if (copy.IsExpired(leaseDurationNanos, now))
                {
                    Trace.TraceInformation(String.Format("GetCurrentlyHeldLease not returning lease with key {0} because it is expired", copy.LeaseKey));
                    return null;
                }
                else
                {
                    return copy;
                }
            }
        }

        public bool UpdateLease(T lease, Guid concurrencyToken)
        {
            VerifyNotNull(lease, "lease cannot be null");
            VerifyNotNull(lease.LeaseKey, "leaseKey cannot be null");
            VerifyNotNull(concurrencyToken, "concurrencyToken cannot be null");

            String leaseKey = lease.LeaseKey;
            ownedLeases.TryGetValue(leaseKey, out T authoritativeLease);

            if (authoritativeLease == null)
            {
                Trace.TraceInformation(String.Format("Worker {0} could not update lease with key {1} because it does not hold it", WorkerIdentifier, leaseKey));
                return false;
            }

            /*
             * If the passed-in concurrency token doesn't match the concurrency token of the authoritative lease, it means
             * the lease was lost and regained between when the caller acquired his concurrency token and when the caller
             * called update.
             */
            if (!authoritativeLease.ConcurrencyToken.Equals(concurrencyToken))
            {
                Trace.TraceInformation(String.Format("Worker {0} refusing to update lease with key {1} because concurrency tokens don't match", WorkerIdentifier, leaseKey));
                return false;
            }

            //long startTime = System.currentTimeMillis();
            //boolean success = false;
            try
            {
                lock (authoritativeLease)
                {
                    authoritativeLease.Update(lease);
                    bool updatedLease = leaseManager.UpdateLease(authoritativeLease);
                    if (updatedLease)
                    {
                        // Updates increment the counter
                        authoritativeLease.LastCounterIncrementNanos = Time.NanoTime;
                    }
                    else
                    {
                        /*
                         * If updateLease returns false, it means someone took the lease from us. Remove the lease
                         * from our set of owned leases pro-actively rather than waiting for a run of renewLeases().
                         */
                        Trace.TraceInformation(String.Format("Worker {0} lost lease with key {1} - discovered during update", WorkerIdentifier, leaseKey));

                        /*
                         * Remove only if the value currently in the map is the same as the authoritative lease. We're
                         * guarding against a pause after the concurrency token check above. It plays out like so:
                         * 
                         * 1) Concurrency token check passes
                         * 2) Pause. Lose lease, re-acquire lease. This requires at least one lease counter update.
                         * 3) Unpause. leaseManager.updateLease fails conditional write due to counter updates, returns
                         * false.
                         * 4) ownedLeases.remove(key, value) doesn't do anything because authoritativeLease does not
                         * .equals() the re-acquired version in the map on the basis of lease counter. This is what we want.
                         * If we just used ownedLease.remove(key), we would have pro-actively removed a lease incorrectly.
                         * 
                         * Note that there is a subtlety here - Lease.equals() deliberately does not check the concurrency
                         * token, but it does check the lease counter, so this scheme works.
                         */
                        ownedLeases.TryGetValue(leaseKey, out T l);
                        if (l == authoritativeLease)
                        {
                            ownedLeases.TryRemove(leaseKey, out T al);
                        }
                    }

                    //success = true;
                    return updatedLease;
                }
            }
            finally
            {
                //MetricsHelper.addSuccessAndLatency("UpdateLease", startTime, success, MetricsLevel.DETAILED);
            }
        }

        public void DropLease(T lease)
        {
            ownedLeases.TryRemove(lease.LeaseKey, out T value);
        }

        private void VerifyNotNull(Object @object, String message)
        {
            if (@object == null)
            {
                throw new ArgumentException(message);
            }
        }
    }
}
