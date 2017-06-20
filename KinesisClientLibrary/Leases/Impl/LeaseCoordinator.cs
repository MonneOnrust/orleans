using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using KinesisClientLibrary.ClientLibrary.Leases.Interfaces;

namespace KinesisClientLibrary.ClientLibrary.Leases.Impl
{
    /// <summary>
    /// Manages shard leases like in the Kinesis Client Library. This ensures that only one instance of a microservice reads the shard. 
    /// Supported: multiple shards per stream.
    /// Not supported: multiple streams, resharding
    /// Tbd: rebalancing shards across workers 
    /// </summary>
    public class LeaseCoordinator<T> where T : Lease
    {
        private static readonly int DEFAULT_MAX_LEASES_FOR_WORKER = int.MaxValue;
        private static readonly int DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME = 1;

        // Time to wait for in-flight Runnables to finish when calling .stop();
        private static readonly int STOP_WAIT_TIME_MILLIS = 2000;

        private LeaseTaker<T> leaseTaker;
        private LeaseRenewer<T> leaseRenewer;

        private readonly long renewerIntervalMillis;
        private readonly long takerIntervalMillis;

        private readonly Object shutdownLock = new Object();
        private volatile bool running = false;

        private Task renewerTask;
        private Task takerTask;
        private CancellationTokenSource cancellationTokenSource;

        /// <summary>
        ///  Constructor.
        /// </summary>
        /// <param name="leaseManager">LeaseManager instance to use</param>
        /// <param name="workerIdentifier">Identifies the worker(e.g.useful to track lease ownership)</param>
        /// <param name="leaseDurationMillis">Duration of a lease</param>
        /// <param name="epsilonMillis">Allow for some variance when calculating lease expirations</param>
        /// <param name="metricsFactory">Used to publish metrics about lease operations</param>
        public LeaseCoordinator(ILeaseManager<T> leaseManager,
                String workerIdentifier,
                long leaseDurationMillis,
                long epsilonMillis/*,
                IMetricsFactory metricsFactory*/)
            : this(leaseManager, workerIdentifier, leaseDurationMillis, epsilonMillis, DEFAULT_MAX_LEASES_FOR_WORKER, DEFAULT_MAX_LEASES_TO_STEAL_AT_ONE_TIME/*, metricsFactory*/)
        { }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="leaseManager">LeaseManager instance to use</param>
        /// <param name="workerIdentifier">Identifies the worker(e.g.useful to track lease ownership)</param>
        /// <param name="leaseDurationMillis">Duration of a lease</param>
        /// <param name="epsilonMillis">Allow for some variance when calculating lease expirations</param>
        /// <param name="maxLeasesForWorker">Max leases this Worker can handle at a time</param>
        /// <param name="maxLeasesToStealAtOneTime">Steal up to these many leases at a time(for load balancing)</param>
        public LeaseCoordinator(ILeaseManager<T> leaseManager,
                String workerIdentifier,
                long leaseDurationMillis,
                long epsilonMillis,
                int maxLeasesForWorker,
                int maxLeasesToStealAtOneTime/*,
                IMetricsFactory metricsFactory*/)
        {
            //this.leaseRenewalThreadpool = getLeaseRenewalExecutorService(MAX_LEASE_RENEWAL_THREAD_COUNT);

            this.leaseTaker = new LeaseTaker<T>(leaseManager, workerIdentifier, leaseDurationMillis)
                                                .WithMaxLeasesForWorker(maxLeasesForWorker)
                                                .WithMaxLeasesToStealAtOneTime(maxLeasesToStealAtOneTime);

            this.leaseRenewer = new LeaseRenewer<T>(leaseManager, workerIdentifier, leaseDurationMillis/*, leaseRenewalThreadpool*/);

            this.renewerIntervalMillis = leaseDurationMillis / 3 - epsilonMillis;
            this.takerIntervalMillis = (leaseDurationMillis + epsilonMillis) * 2;
            //this.metricsFactory = metricsFactory;

            Trace.TraceInformation(String.Format("With failover time {0} ms and epsilon {1} ms, LeaseCoordinator will renew leases every {2} ms, take leases every {3} ms, process maximum of {4} leases and steal {5} lease(s) at a time.",
                    leaseDurationMillis,
                    epsilonMillis,
                    renewerIntervalMillis,
                    takerIntervalMillis,
                    maxLeasesForWorker,
                    maxLeasesToStealAtOneTime));
        }

        public void Start()
        {
            leaseRenewer.Initialize();

            cancellationTokenSource = new CancellationTokenSource();

            takerTask = Task.Factory.StartNew(() => TakeLeases(), cancellationTokenSource.Token);
            renewerTask = Task.Factory.StartNew(() => RenewLeases(), cancellationTokenSource.Token);

            running = true;
        }

        /// <summary>
        /// Stops background threads and waits for {@link #STOP_WAIT_TIME_MILLIS} for all background tasks to complete.
        /// If tasks are not completed after this time, method will shutdown thread pool forcefully and return.
        /// </summary>
        public void Stop()
        {
            if (!running) return;

            cancellationTokenSource.Cancel();

            if (takerTask != null)
            {
                if (takerTask.Wait(STOP_WAIT_TIME_MILLIS))
                {
                    Trace.TraceInformation(String.Format("Worker {0} has successfully stopped lease-tracking threads", leaseTaker.WorkerIdentifier));
                }
                else
                {
                    //leaseCoordinatorThreadPool.shutdownNow();
                    //Trace.TraceInformation(String.Format("Worker %s stopped lease-tracking threads %dms after stop", leaseTaker.WorkerIdentifier, STOP_WAIT_TIME_MILLIS));
                    Trace.TraceInformation(String.Format("Worker {0} couldn't stop lease-tracking threads in {1} ms after stop", leaseTaker.WorkerIdentifier, STOP_WAIT_TIME_MILLIS));
                }
            }
            else
            {
                Trace.WriteLine("takerTask was null, no need to shutdown/terminate.");
            }

            if (renewerTask != null)
            {
                if (renewerTask.Wait(STOP_WAIT_TIME_MILLIS))
                {
                    Trace.TraceInformation(String.Format("Worker {0} has successfully stopped lease-tracking threads", leaseRenewer.WorkerIdentifier));
                }
                else
                {
                    //leaseCoordinatorThreadPool.shutdownNow();
                    //Trace.TraceInformation(String.Format("Worker %s stopped lease-tracking threads %dms after stop", leaseTaker.WorkerIdentifier, STOP_WAIT_TIME_MILLIS));
                    Trace.TraceInformation(String.Format("Worker {0} couldn't stop lease-renewing threads in {1} ms after stop", leaseRenewer.WorkerIdentifier, STOP_WAIT_TIME_MILLIS));
                }
            }
            else
            {
                Trace.WriteLine("leaseRenewer was null, no need to shutdown/terminate.");
            }

            lock (shutdownLock)
            {
                leaseRenewer.ClearCurrentlyHeldLeases();
                running = false;
            }
        }

        private void TakeLeases()
        {
            while (true)
            {
                var leases = leaseTaker.TakeLeases();

                lock (shutdownLock)
                {
                    if (running)
                    {
                        leaseRenewer.AddLeasesToRenew(leases.Values.ToList());
                    }
                }

                // TODO: start receivers

                if (cancellationTokenSource.IsCancellationRequested) break;

                Thread.Sleep((int)takerIntervalMillis);
            }
        }

        private void RenewLeases()
        {
            while (true)
            {
                var sw = Stopwatch.StartNew();

                leaseRenewer.RenewLeases();

                if (cancellationTokenSource.IsCancellationRequested) break;

                Thread.Sleep((int)(renewerIntervalMillis - sw.ElapsedMilliseconds));
            }
        }
    }
}
