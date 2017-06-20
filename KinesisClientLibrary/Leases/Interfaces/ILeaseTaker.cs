using System;
using System.Collections.Generic;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Leases.Impl;

namespace KinesisClientLibrary.ClientLibrary.Leases.Interfaces
{
    /**
 * ILeaseTaker is used by LeaseCoordinator to take new leases, or leases that other workers fail to renew. Each
 * LeaseCoordinator instance corresponds to one worker and uses exactly one ILeaseTaker to take leases for that worker.
 */
    public interface ILeaseTaker<T> where T : Lease
    {

        /**
         * Compute the set of leases available to be taken and attempt to take them. Lease taking rules are:
         * 
         * 1) If a lease's counter hasn't changed in long enough, try to take it.
         * 2) If we see a lease we've never seen before, take it only if owner == null. If it's owned, odds are the owner is
         * holding it. We can't tell until we see it more than once.
         * 3) For load balancing purposes, you may violate rules 1 and 2 for EXACTLY ONE lease per call of takeLeases().
         * 
         * @return map of shardId to Lease object for leases we just successfully took.
         * 
         * @throws DependencyException on unexpected DynamoDB failures
         * @throws InvalidStateException if lease table does not exist
         */
        Dictionary<String, T> TakeLeases();

        /**
         * @return workerIdentifier for this LeaseTaker
         */
        String WorkerIdentifier { get; }

    }
}
