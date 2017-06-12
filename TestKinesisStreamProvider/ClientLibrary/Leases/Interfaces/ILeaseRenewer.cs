using System;
using System.Collections.Generic;
using System.Text;
using TestKinesisStreamProvider.ClientLibrary.Leases.Impl;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Interfaces
{
    /**
 * ILeaseRenewer objects are used by LeaseCoordinator to renew leases held by the LeaseCoordinator. Each
 * LeaseCoordinator instance corresponds to one worker, and uses exactly one ILeaseRenewer to manage lease renewal for
 * that worker.
 */
    public interface ILeaseRenewer<T> where T : Lease
    {

        /**
         * Bootstrap initial set of leases from the LeaseManager (e.g. upon process restart, pick up leases we own)
         * @throws DependencyException on unexpected DynamoDB failures
         * @throws InvalidStateException if lease table doesn't exist
         * @throws ProvisionedThroughputException if DynamoDB reads fail due to insufficient capacity
         */
        void Initialize();

        /**
         * Attempt to renew all currently held leases.
         * 
         * @throws DependencyException on unexpected DynamoDB failures
         * @throws InvalidStateException if lease table does not exist
         */
        void RenewLeases();

        /**
         * @return currently held leases. Key is shardId, value is corresponding Lease object. A lease is currently held if
         *         we successfully renewed it on the last run of renewLeases(). Lease objects returned are deep copies -
         *         their lease counters will not tick.
         */
        Dictionary<String, T> GetCurrentlyHeldLeases();

        /**
         * @param leaseKey key of the lease to retrieve
         * 
         * @return a deep copy of a currently held lease, or null if we don't hold the lease
         */
        T GetCurrentlyHeldLease(String leaseKey);

        /**
         * Adds leases to this LeaseRenewer's set of currently held leases. Leases must have lastRenewalNanos set to the
         * last time the lease counter was incremented before being passed to this method.
         * 
         * @param newLeases new leases.
         */
        void AddLeasesToRenew(List<T> newLeases);

        /**
         * Clears this LeaseRenewer's set of currently held leases.
         */
        void ClearCurrentlyHeldLeases();

        /**
         * Stops the lease renewer from continunig to maintain the given lease.
         * 
         * @param lease the lease to drop.
         */
        void DropLease(T lease);

        /**
         * Update application-specific fields in a currently held lease. Cannot be used to update internal fields such as
         * leaseCounter, leaseOwner, etc. Fails if we do not hold the lease, or if the concurrency token does not match
         * the concurrency token on the internal authoritative copy of the lease (ie, if we lost and re-acquired the lease).
         * 
         * @param lease lease object containing updated data
         * @param concurrencyToken obtained by calling Lease.getConcurrencyToken for a currently held lease
         * 
         * @return true if update succeeds, false otherwise
         * 
         * @throws InvalidStateException if lease table does not exist
         * @throws ProvisionedThroughputException if DynamoDB update fails due to lack of capacity
         * @throws DependencyException if DynamoDB update fails in an unexpected way
         */
        bool UpdateLease(T lease, Guid concurrencyToken);
    }
}