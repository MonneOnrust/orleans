using Amazon.Kinesis.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using KinesisClientLibrary.ClientLibrary.Exceptions;
using KinesisClientLibrary.ClientLibrary.Leases.Impl;
using KinesisClientLibrary.ClientLibrary.Leases.Interfaces;
using KinesisClientLibrary.ClientLibrary.Leases.Utils;
using KinesisClientLibrary.ClientLibrary.Proxies;
using KinesisClientLibrary.ClientLibrary.Types;

namespace KinesisClientLibrary.ClientLibrary.Lib.Worker
{
    /**
 * Helper class to sync leases with shards of the Kinesis stream.
 * It will create new leases/activities when it discovers new Kinesis shards (bootstrap/resharding).
 * It deletes leases for shards that have been trimmed from Kinesis, or if we've completed processing it
 * and begun processing it's child shards.
 */
    public class ShardSyncer
    {
        //private static final Log LOG = LogFactory.getLog(ShardSyncer.class);

        private static object @lock = new object();

        /**
         * Note constructor is private: We use static synchronized methods - this is a utility class.
         */
        private ShardSyncer()
        {
        }

        static /*synchronized*/ void BootstrapShardLeases(IKinesisProxy kinesisProxy, ILeaseManager<KinesisClientLease> leaseManager, InitialPositionInStreamExtended initialPositionInStream, bool cleanupLeasesOfCompletedShards)
        {
            lock (@lock)
            {
                SyncShardLeases(kinesisProxy, leaseManager, initialPositionInStream, cleanupLeasesOfCompletedShards);
            }
        }

        /**
         * Check and create leases for any new shards (e.g. following a reshard operation).
         * 
         * @param kinesisProxy
         * @param leaseManager
         * @param initialPositionInStream
         * @param expectedClosedShardId If this is not null, we will assert that the shard list we get from Kinesis
         *        shows this shard to be closed (e.g. parent shard must be closed after a reshard operation).
         *        If it is open, we assume this is an race condition around a reshard event and throw
         *        a KinesisClientLibIOException so client can backoff and retry later.
         * @throws DependencyException
         * @throws InvalidStateException
         * @throws ProvisionedThroughputException
         * @throws KinesisClientLibIOException
         */
        public static /*synchronized*/ void CheckAndCreateLeasesForNewShards(IKinesisProxy kinesisProxy, ILeaseManager<KinesisClientLease> leaseManager, InitialPositionInStreamExtended initialPositionInStream, bool cleanupLeasesOfCompletedShards)
        {
            lock (@lock)
            {
                SyncShardLeases(kinesisProxy, leaseManager, initialPositionInStream, cleanupLeasesOfCompletedShards);
            }
        }

        /**
         * Sync leases with Kinesis shards (e.g. at startup, or when we reach end of a shard).
         * 
         * @param kinesisProxy
         * @param leaseManager
         * @param expectedClosedShardId If this is not null, we will assert that the shard list we get from Kinesis
         *        does not show this shard to be open (e.g. parent shard must be closed after a reshard operation).
         *        If it is still open, we assume this is a race condition around a reshard event and
         *        throw a KinesisClientLibIOException so client can backoff and retry later. If the shard doesn't exist in
         *        Kinesis at all, we assume this is an old/expired shard and continue with the sync operation.
         * @throws DependencyException
         * @throws InvalidStateException
         * @throws ProvisionedThroughputException
         * @throws KinesisClientLibIOException
         */
        // CHECKSTYLE:OFF CyclomaticComplexity
        private static /*synchronized*/ void SyncShardLeases(IKinesisProxy kinesisProxy, ILeaseManager<KinesisClientLease> leaseManager, InitialPositionInStreamExtended initialPosition, bool cleanupLeasesOfCompletedShards)
        {
            lock (@lock)
            {
                List<Shard> shards = GetShardList(kinesisProxy);
                Trace.WriteLine("Num shards: " + shards.Count);

                Dictionary<String, Shard> shardIdToShardMap = ConstructShardIdToShardMap(shards);
                Dictionary<String, HashSet<String>> shardIdToChildShardIdsMap = ConstructShardIdToChildShardIdsMap(shardIdToShardMap);
                AssertAllParentShardsAreClosed(shardIdToChildShardIdsMap, shardIdToShardMap);

                List<KinesisClientLease> currentLeases = leaseManager.ListLeases();

                List<KinesisClientLease> newLeasesToCreate = DetermineNewLeasesToCreate(shards, currentLeases, initialPosition);
                Trace.WriteLine("Num new leases to create: " + newLeasesToCreate.Count);
                foreach (KinesisClientLease lease in newLeasesToCreate)
                {
                    long startTimeMillis = Time.MilliTime;
                    //bool success = false;
                    try
                    {
                        leaseManager.CreateLeaseIfNotExists(lease);
                        //success = true;
                    }
                    finally
                    {
                        //MetricsHelper.addSuccessAndLatency("CreateLease", startTimeMillis, success, MetricsLevel.DETAILED);
                    }
                }

                List<KinesisClientLease> trackedLeases = new List<KinesisClientLease>();
                if (currentLeases != null)
                {
                    trackedLeases.AddRange(currentLeases);
                }
                trackedLeases.AddRange(newLeasesToCreate);
                CleanupGarbageLeases(shards, trackedLeases, kinesisProxy, leaseManager);
                if (cleanupLeasesOfCompletedShards)
                {
                    CleanupLeasesOfFinishedShards(currentLeases, shardIdToShardMap, shardIdToChildShardIdsMap, trackedLeases, leaseManager);
                }
            }
        }

        // CHECKSTYLE:ON CyclomaticComplexity

        /** Helper method to detect a race condition between fetching the shards via paginated DescribeStream calls
         * and a reshard operation.
         * @param shardIdToChildShardIdsMap
         * @param shardIdToShardMap
         * @throws KinesisClientLibIOException
         */
        private static void AssertAllParentShardsAreClosed(Dictionary<String, HashSet<String>> shardIdToChildShardIdsMap, Dictionary<String, Shard> shardIdToShardMap)
        {
            foreach (String parentShardId in shardIdToChildShardIdsMap.Keys)
            {
                Shard parentShard = shardIdToShardMap[parentShardId];
                if (parentShardId == null || parentShard.SequenceNumberRange.EndingSequenceNumber == null)
                {
                    throw new KinesisClientLibIOException("Parent shardId " + parentShardId + " is not closed. This can happen due to a race condition between describeStream and a reshard operation.");
                }
            }
        }

        /**
         * Helper method to create a shardId->KinesisClientLease map.
         * Note: This has package level access for testing purposes only.
         * @param trackedLeaseList
         * @return
         */
        static Dictionary<String, KinesisClientLease> ConstructShardIdToKCLLeaseMap(List<KinesisClientLease> trackedLeaseList)
        {
            Dictionary<String, KinesisClientLease> trackedLeasesMap = new Dictionary<string, KinesisClientLease>();
            foreach (KinesisClientLease lease in trackedLeaseList)
            {
                trackedLeasesMap[lease.LeaseKey] = lease;
            }
            return trackedLeasesMap;
        }

        /**
         * Note: this has package level access for testing purposes. 
         * Useful for asserting that we don't have an incomplete shard list following a reshard operation.
         * We verify that if the shard is present in the shard list, it is closed and its hash key range
         *     is covered by its child shards.
         * @param shards List of all Kinesis shards
         * @param shardIdsOfClosedShards Id of the shard which is expected to be closed
         * @return ShardIds of child shards (children of the expectedClosedShard)
         * @throws KinesisClientLibIOException
         */
        static /*synchronized*/ void AssertClosedShardsAreCoveredOrAbsent(Dictionary<String, Shard> shardIdToShardMap, Dictionary<String, HashSet<String>> shardIdToChildShardIdsMap, HashSet<String> shardIdsOfClosedShards)
        {
            lock (@lock)
            {
                String exceptionMessageSuffix = "This can happen if we constructed the list of shards while a reshard operation was in progress.";

                foreach (String shardId in shardIdsOfClosedShards)
                {
                    shardIdToShardMap.TryGetValue(shardId, out Shard shard);
                    if (shard == null)
                    {
                        Trace.TraceInformation("Shard " + shardId + " is not present in Kinesis anymore.");
                        continue;
                    }

                    String endingSequenceNumber = shard.SequenceNumberRange.EndingSequenceNumber;
                    if (endingSequenceNumber == null)
                    {
                        throw new KinesisClientLibIOException("Shard " + shardIdsOfClosedShards + " is not closed. " + exceptionMessageSuffix);
                    }

                    shardIdToChildShardIdsMap.TryGetValue(shardId, out HashSet<String> childShardIds);
                    if (childShardIds == null)
                    {
                        throw new KinesisClientLibIOException("Incomplete shard list: Closed shard " + shardId + " has no children." + exceptionMessageSuffix);
                    }

                    AssertHashRangeOfClosedShardIsCovered(shard, shardIdToShardMap, childShardIds);
                }
            }
        }

        private static /*synchronized*/ void AssertHashRangeOfClosedShardIsCovered(Shard closedShard, Dictionary<String, Shard> shardIdToShardMap, HashSet<String> childShardIds)
        {
            lock (@lock)
            {
                long startingHashKeyOfClosedShard = long.Parse(closedShard.HashKeyRange.StartingHashKey);
                long endingHashKeyOfClosedShard = long.Parse(closedShard.HashKeyRange.EndingHashKey);
                long? minStartingHashKeyOfChildren = null;
                long? maxEndingHashKeyOfChildren = null;

                foreach (String childShardId in childShardIds)
                {
                    Shard childShard = shardIdToShardMap[childShardId];
                    long startingHashKey = long.Parse(childShard.HashKeyRange.StartingHashKey);
                    if (minStartingHashKeyOfChildren == null || startingHashKey.CompareTo(minStartingHashKeyOfChildren.Value) < 0)
                    {
                        minStartingHashKeyOfChildren = startingHashKey;
                    }
                    long endingHashKey = long.Parse(childShard.HashKeyRange.EndingHashKey);
                    if (maxEndingHashKeyOfChildren == null || endingHashKey.CompareTo(maxEndingHashKeyOfChildren.Value) > 0)
                    {
                        maxEndingHashKeyOfChildren = endingHashKey;
                    }
                }

                if (minStartingHashKeyOfChildren == null ||
                    maxEndingHashKeyOfChildren == null ||
                    minStartingHashKeyOfChildren.Value.CompareTo(startingHashKeyOfClosedShard) > 0 ||
                    maxEndingHashKeyOfChildren.Value.CompareTo(endingHashKeyOfClosedShard) < 0)
                {
                    throw new KinesisClientLibIOException("Incomplete shard list: hash key range of shard " + closedShard.ShardId + " is not covered by its child shards.");
                }
            }
        }

        /**
         * Helper method to construct shardId->setOfChildShardIds map.
         * Note: This has package access for testing purposes only.
         * @param shardIdToShardMap
         * @return
         */
        static Dictionary<String, HashSet<String>> ConstructShardIdToChildShardIdsMap(Dictionary<String, Shard> shardIdToShardMap)
        {
            Dictionary<String, HashSet<String>> shardIdToChildShardIdsMap = new Dictionary<string, HashSet<string>>();
            foreach (var entry in shardIdToShardMap)
            {
                String shardId = entry.Key;
                Shard shard = entry.Value;
                String parentShardId = shard.ParentShardId;
                if (parentShardId != null && shardIdToShardMap.ContainsKey(parentShardId))
                {
                    shardIdToChildShardIdsMap.TryGetValue(parentShardId, out HashSet<String> childShardIds);
                    if (childShardIds == null)
                    {
                        childShardIds = new HashSet<String>();
                        shardIdToChildShardIdsMap[parentShardId] = childShardIds;
                    }
                    childShardIds.Add(shardId);
                }

                String adjacentParentShardId = shard.AdjacentParentShardId;
                if (adjacentParentShardId != null && shardIdToShardMap.ContainsKey(adjacentParentShardId))
                {
                    shardIdToChildShardIdsMap.TryGetValue(adjacentParentShardId, out HashSet<String> childShardIds);
                    if (childShardIds == null)
                    {
                        childShardIds = new HashSet<String>();
                        shardIdToChildShardIdsMap[adjacentParentShardId] = childShardIds;
                    }
                    childShardIds.Add(shardId);
                }
            }
            return shardIdToChildShardIdsMap;
        }

        private static List<Shard> GetShardList(IKinesisProxy kinesisProxy)
        {
            List<Shard> shards = kinesisProxy.GetShardList();
            if (shards == null)
            {
                throw new KinesisClientLibIOException("Stream is not in ACTIVE OR UPDATING state - will retry getting the shard list.");
            }
            return shards;
        }

        /**
         * Determine new leases to create and their initial checkpoint.
         * Note: Package level access only for testing purposes.
         * 
         * For each open (no ending sequence number) shard that doesn't already have a lease,
         * determine if it is a descendent of any shard which is or will be processed (e.g. for which a lease exists):
         * If so, set checkpoint of the shard to TrimHorizon and also create leases for ancestors if needed.
         * If not, set checkpoint of the shard to the initial position specified by the client.
         * To check if we need to create leases for ancestors, we use the following rules:
         *   * If we began (or will begin) processing data for a shard, then we must reach end of that shard before we begin processing data from any of its descendants.
         *   * A shard does not start processing data until data from all its parents has been processed.
         * Note, if the initial position is LATEST and a shard has two parents and only one is a descendant - we'll create
         * leases corresponding to both the parents - the parent shard which is not a descendant will have  
         * its checkpoint set to Latest.
         * 
         * We assume that if there is an existing lease for a shard, then either:
         *   * we have previously created a lease for its parent (if it was needed), or
         *   * the parent shard has expired.
         * 
         * For example:
         * Shard structure (each level depicts a stream segment):
         * 0 1 2 3 4 5    - shards till epoch 102
         * \ / \ / | |
         *  6   7  4 5    - shards from epoch 103 - 205
         *   \ /   | /\
         *    8    4 9 10 - shards from epoch 206 (open - no ending sequenceNumber)
         * Current leases: (3, 4, 5)
         * New leases to create: (2, 6, 7, 8, 9, 10)
         * 
         * The leases returned are sorted by the starting sequence number - following the same order
         * when persisting the leases in DynamoDB will ensure that we recover gracefully if we fail
         * before creating all the leases.
         * 
         * @param shards List of all shards in Kinesis (we'll create new leases based on this set)
         * @param currentLeases List of current leases
         * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
         *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
         * @return List of new leases to create sorted by starting sequenceNumber of the corresponding shard
         */
        static List<KinesisClientLease> DetermineNewLeasesToCreate(List<Shard> shards, List<KinesisClientLease> currentLeases, InitialPositionInStreamExtended initialPosition)
        {
            Dictionary<String, KinesisClientLease> shardIdToNewLeaseMap = new Dictionary<string, KinesisClientLease>();
            Dictionary<String, Shard> shardIdToShardMapOfAllKinesisShards = ConstructShardIdToShardMap(shards);

            HashSet<String> shardIdsOfCurrentLeases = new HashSet<String>();
            foreach (KinesisClientLease lease in currentLeases)
            {
                shardIdsOfCurrentLeases.Add(lease.LeaseKey);
                Trace.WriteLine("Existing lease: " + lease);
            }

            List<Shard> openShards = GetOpenShards(shards);
            Dictionary<String, Boolean> memoizationContext = new Dictionary<string, bool>();

            // Iterate over the open shards and find those that don't have any lease entries.
            foreach (Shard shard in openShards)
            {
                String shardId = shard.ShardId;
                Trace.WriteLine("Evaluating leases for open shard " + shardId + " and its ancestors.");
                if (shardIdsOfCurrentLeases.Contains(shardId))
                {
                    Trace.WriteLine("Lease for shardId " + shardId + " already exists. Not creating a lease");
                }
                else
                {
                    Trace.WriteLine("Need to create a lease for shardId " + shardId);
                    KinesisClientLease newLease = NewKCLLease(shard);
                    bool isDescendant = CheckIfDescendantAndAddNewLeasesForAncestors(shardId,
                                                                                    initialPosition,
                                                                                    shardIdsOfCurrentLeases,
                                                                                    shardIdToShardMapOfAllKinesisShards,
                                                                                    shardIdToNewLeaseMap,
                                                                                    memoizationContext);

                    /**
                     * If the shard is a descendant and the specified initial position is AT_TIMESTAMP, then the
                     * checkpoint should be set to AT_TIMESTAMP, else to TRIM_HORIZON. For AT_TIMESTAMP, we will add a
                     * lease just like we do for TRIM_HORIZON. However we will only return back records with server-side
                     * timestamp at or after the specified initial position timestamp.
                     *
                     * Shard structure (each level depicts a stream segment):
                     * 0 1 2 3 4   5   - shards till epoch 102
                     * \ / \ / |   |
                     *  6   7  4   5   - shards from epoch 103 - 205
                     *   \ /   |  /\
                     *    8    4 9  10 - shards from epoch 206 (open - no ending sequenceNumber)
                     *
                     * Current leases: empty set
                     *
                     * For the above example, suppose the initial position in stream is set to AT_TIMESTAMP with
                     * timestamp value 206. We will then create new leases for all the shards (with checkpoint set to
                     * AT_TIMESTAMP), including the ancestor shards with epoch less than 206. However as we begin
                     * processing the ancestor shards, their checkpoints would be updated to SHARD_END and their leases
                     * would then be deleted since they won't have records with server-side timestamp at/after 206. And
                     * after that we will begin processing the descendant shards with epoch at/after 206 and we will
                     * return the records that meet the timestamp requirement for these shards.
                     */
                    if (isDescendant && !initialPosition.GetInitialPositionInStream().Equals(InitialPositionInStream.AT_TIMESTAMP))
                    {
                        newLease.SetCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                    }
                    else
                    {
                        newLease.SetCheckpoint(ConvertToCheckpoint(initialPosition));
                    }
                    Trace.WriteLine("Set checkpoint of " + newLease.LeaseKey + " to " + newLease.Checkpoint);
                    shardIdToNewLeaseMap[shardId] = newLease;
                }
            }

            List<KinesisClientLease> newLeasesToCreate = new List<KinesisClientLease>();
            newLeasesToCreate.AddRange(shardIdToNewLeaseMap.Values);
            IComparer<KinesisClientLease> startingSequenceNumberComparator = new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMapOfAllKinesisShards);
            newLeasesToCreate.Sort(startingSequenceNumberComparator);
            return newLeasesToCreate;
        }

        /**
         * Note: Package level access for testing purposes only.
         * Check if this shard is a descendant of a shard that is (or will be) processed.
         * Create leases for the ancestors of this shard as required.
         * See javadoc of determineNewLeasesToCreate() for rules and example.
         * 
         * @param shardId The shardId to check.
         * @param initialPosition One of LATEST, TRIM_HORIZON, or AT_TIMESTAMP. We'll start fetching records from that
         *        location in the shard (when an application starts up for the first time - and there are no checkpoints).
         * @param shardIdsOfCurrentLeases The shardIds for the current leases.
         * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
         * @param shardIdToLeaseMapOfNewShards Add lease POJOs corresponding to ancestors to this map.
         * @param memoizationContext Memoization of shards that have been evaluated as part of the evaluation
         * @return true if the shard is a descendant of any current shard (lease already exists)
         */
        // CHECKSTYLE:OFF CyclomaticComplexity
        static bool CheckIfDescendantAndAddNewLeasesForAncestors(String shardId,
                                                                InitialPositionInStreamExtended initialPosition,
                                                                HashSet<String> shardIdsOfCurrentLeases,
                                                                Dictionary<String, Shard> shardIdToShardMapOfAllKinesisShards,
                                                                Dictionary<String, KinesisClientLease> shardIdToLeaseMapOfNewShards,
                                                                Dictionary<String, Boolean> memoizationContext)
        {
            if (memoizationContext.TryGetValue(shardId, out bool previousValue))
            {
                return previousValue;
            }

            bool isDescendant = false;
            Shard shard;
            HashSet<String> parentShardIds;
            HashSet<String> descendantParentShardIds = new HashSet<String>();

            if (shardId != null && shardIdToShardMapOfAllKinesisShards.ContainsKey(shardId))
            {
                if (shardIdsOfCurrentLeases.Contains(shardId))
                {
                    // This shard is a descendant of a current shard.
                    isDescendant = true;
                    // We don't need to add leases of its ancestors,
                    // because we'd have done it when creating a lease for this shard.
                }
                else
                {
                    shardIdToShardMapOfAllKinesisShards.TryGetValue(shardId, out shard);
                    parentShardIds = GetParentShardIds(shard, shardIdToShardMapOfAllKinesisShards);
                    foreach (String parentShardId in parentShardIds)
                    {
                        // Check if the parent is a descendant, and include its ancestors.
                        if (CheckIfDescendantAndAddNewLeasesForAncestors(parentShardId, initialPosition, shardIdsOfCurrentLeases,
                                                                         shardIdToShardMapOfAllKinesisShards, shardIdToLeaseMapOfNewShards, memoizationContext))
                        {
                            isDescendant = true;
                            descendantParentShardIds.Add(parentShardId);
                            Trace.WriteLine("Parent shard " + parentShardId + " is a descendant.");
                        }
                        else
                        {
                            Trace.WriteLine("Parent shard " + parentShardId + " is NOT a descendant.");
                        }
                    }

                    // If this is a descendant, create leases for its parent shards (if they don't exist)
                    if (isDescendant)
                    {
                        foreach (String parentShardId in parentShardIds)
                        {
                            if (!shardIdsOfCurrentLeases.Contains(parentShardId))
                            {
                                Trace.WriteLine("Need to create a lease for shardId " + parentShardId);
                                shardIdToLeaseMapOfNewShards.TryGetValue(parentShardId, out KinesisClientLease lease);
                                if (lease == null)
                                {
                                    lease = NewKCLLease(shardIdToShardMapOfAllKinesisShards[parentShardId]);
                                    shardIdToLeaseMapOfNewShards[parentShardId] = lease;
                                }

                                if (descendantParentShardIds.Contains(parentShardId) && !initialPosition.GetInitialPositionInStream().Equals(InitialPositionInStream.AT_TIMESTAMP))
                                {
                                    lease.SetCheckpoint(ExtendedSequenceNumber.TRIM_HORIZON);
                                }
                                else
                                {
                                    lease.SetCheckpoint(ConvertToCheckpoint(initialPosition));
                                }
                            }
                        }
                    }
                    else
                    {
                        // This shard should be included, if the customer wants to process all records in the stream or
                        // if the initial position is AT_TIMESTAMP. For AT_TIMESTAMP, we will add a lease just like we do
                        // for TRIM_HORIZON. However we will only return back records with server-side timestamp at or
                        // after the specified initial position timestamp.
                        if (initialPosition.GetInitialPositionInStream().Equals(InitialPositionInStream.TRIM_HORIZON) || initialPosition.GetInitialPositionInStream().Equals(InitialPositionInStream.AT_TIMESTAMP))
                        {
                            isDescendant = true;
                        }
                    }

                }
            }

            memoizationContext[shardId] = isDescendant;
            return isDescendant;
        }
        // CHECKSTYLE:ON CyclomaticComplexity

        /**
         * Helper method to get parent shardIds of the current shard - includes the parent shardIds if:
         * a/ they are not null
         * b/ if they exist in the current shard map (i.e. haven't expired)
         * 
         * @param shard Will return parents of this shard
         * @param shardIdToShardMapOfAllKinesisShards ShardId->Shard map containing all shards obtained via DescribeStream.
         * @return Set of parentShardIds
         */
        static HashSet<String> GetParentShardIds(Shard shard, Dictionary<String, Shard> shardIdToShardMapOfAllKinesisShards)
        {
            HashSet<String> parentShardIds = new HashSet<String>();
            String parentShardId = shard.ParentShardId;
            if (parentShardId != null && shardIdToShardMapOfAllKinesisShards.ContainsKey(parentShardId))
            {
                parentShardIds.Add(parentShardId);
            }
            String adjacentParentShardId = shard.AdjacentParentShardId;
            if ((adjacentParentShardId != null) && shardIdToShardMapOfAllKinesisShards.ContainsKey(adjacentParentShardId))
            {
                parentShardIds.Add(adjacentParentShardId);
            }
            return parentShardIds;
        }

        /**
         * Delete leases corresponding to shards that no longer exist in the stream. 
         * Current scheme: Delete a lease if:
         *   * the corresponding shard is not present in the list of Kinesis shards, AND
         *   * the parentShardIds listed in the lease are also not present in the list of Kinesis shards.
         * @param shards List of all Kinesis shards (assumed to be a consistent snapshot - when stream is in Active state).
         * @param trackedLeases List of 
         * @param kinesisProxy Kinesis proxy (used to get shard list)
         * @param leaseManager 
         * @throws KinesisClientLibIOException Thrown if we couldn't get a fresh shard list from Kinesis.
         * @throws ProvisionedThroughputException 
         * @throws InvalidStateException 
         * @throws DependencyException 
         */
        private static void CleanupGarbageLeases(List<Shard> shards, List<KinesisClientLease> trackedLeases, IKinesisProxy kinesisProxy, ILeaseManager<KinesisClientLease> leaseManager)
        {
            HashSet<String> kinesisShards = new HashSet<string>();
            foreach (Shard shard in shards)
            {
                kinesisShards.Add(shard.ShardId);
            }

            // Check if there are leases for non-existent shards
            List<KinesisClientLease> garbageLeases = new List<KinesisClientLease>();
            foreach (KinesisClientLease lease in trackedLeases)
            {
                if (IsCandidateForCleanup(lease, kinesisShards))
                {
                    garbageLeases.Add(lease);
                }
            }

            if (garbageLeases.Count != 0)
            {
                Trace.TraceInformation("Found " + garbageLeases.Count + " candidate leases for cleanup. Refreshing list of" + " Kinesis shards to pick up recent/latest shards");
                List<Shard> currentShardList = GetShardList(kinesisProxy);
                HashSet<String> currentKinesisShardIds = new HashSet<string>();
                foreach (Shard shard in currentShardList)
                {
                    currentKinesisShardIds.Add(shard.ShardId);
                }

                foreach (KinesisClientLease lease in garbageLeases)
                {
                    if (IsCandidateForCleanup(lease, currentKinesisShardIds))
                    {
                        Trace.TraceInformation("Deleting lease for shard " + lease.LeaseKey + " as it is not present in Kinesis stream.");
                        leaseManager.DeleteLease(lease);
                    }
                }
            }

        }

        /**
         * Note: This method has package level access, solely for testing purposes.
         * 
         * @param lease Candidate shard we are considering for deletion.
         * @param currentKinesisShardIds
         * @return true if neither the shard (corresponding to the lease), nor its parents are present in
         *         currentKinesisShardIds
         * @throws KinesisClientLibIOException Thrown if currentKinesisShardIds contains a parent shard but not the child
         *         shard (we are evaluating for deletion).
         */
        static bool IsCandidateForCleanup(KinesisClientLease lease, HashSet<String> currentKinesisShardIds)
        {
            bool isCandidateForCleanup = true;

            if (currentKinesisShardIds.Contains(lease.LeaseKey))
            {
                isCandidateForCleanup = false;
            }
            else
            {
                Trace.TraceInformation("Found lease for non-existent shard: " + lease.LeaseKey + ". Checking its parent shards");
                HashSet<String> parentShardIds = lease.GetParentShardIds();
                foreach (String parentShardId in parentShardIds)
                {

                    // Throw an exception if the parent shard exists (but the child does not).
                    // This may be a (rare) race condition between fetching the shard list and Kinesis expiring shards. 
                    if (currentKinesisShardIds.Contains(parentShardId))
                    {
                        String message = "Parent shard " + parentShardId + " exists but not the child shard " + lease.LeaseKey;
                        Trace.TraceInformation(message);
                        throw new KinesisClientLibIOException(message);
                    }
                }
            }

            return isCandidateForCleanup;
        }

        /**
         * Private helper method.
         * Clean up leases for shards that meet the following criteria:
         * a/ the shard has been fully processed (checkpoint is set to SHARD_END)
         * b/ we've begun processing all the child shards: we have leases for all child shards and their checkpoint is not
         *      TRIM_HORIZON.
         * 
         * @param currentLeases List of leases we evaluate for clean up
         * @param shardIdToShardMap Map of shardId->Shard (assumed to include all Kinesis shards)
         * @param shardIdToChildShardIdsMap Map of shardId->childShardIds (assumed to include all Kinesis shards)
         * @param trackedLeases List of all leases we are tracking.
         * @param leaseManager Lease manager (will be used to delete leases)
         * @throws DependencyException
         * @throws InvalidStateException
         * @throws ProvisionedThroughputException
         * @throws KinesisClientLibIOException
         */
        private static /*synchronized*/ void CleanupLeasesOfFinishedShards(List<KinesisClientLease> currentLeases,
                Dictionary<String, Shard> shardIdToShardMap,
                Dictionary<String, HashSet<String>> shardIdToChildShardIdsMap,
                List<KinesisClientLease> trackedLeases,
                ILeaseManager<KinesisClientLease> leaseManager)
        {
            lock (@lock)
            {
                HashSet<String> shardIdsOfClosedShards = new HashSet<String>();
                List<KinesisClientLease> leasesOfClosedShards = new List<KinesisClientLease>();
                foreach (KinesisClientLease lease in currentLeases)
                {
                    if (lease.Checkpoint.Equals(ExtendedSequenceNumber.SHARD_END))
                    {
                        shardIdsOfClosedShards.Add(lease.LeaseKey);
                        leasesOfClosedShards.Add(lease);
                    }
                }

                if (leasesOfClosedShards.Count != 0)
                {
                    AssertClosedShardsAreCoveredOrAbsent(shardIdToShardMap, shardIdToChildShardIdsMap, shardIdsOfClosedShards);
                    IComparer<KinesisClientLease> startingSequenceNumberComparator = new StartingSequenceNumberAndShardIdBasedComparator(shardIdToShardMap);
                    leasesOfClosedShards.Sort(startingSequenceNumberComparator);
                    Dictionary<String, KinesisClientLease> trackedLeaseMap = ConstructShardIdToKCLLeaseMap(trackedLeases);

                    foreach (KinesisClientLease leaseOfClosedShard in leasesOfClosedShards)
                    {
                        String closedShardId = leaseOfClosedShard.LeaseKey;
                        shardIdToChildShardIdsMap.TryGetValue(closedShardId, out HashSet<String> childShardIds);
                        if (closedShardId != null && childShardIds != null && childShardIds.Count != 0)
                        {
                            CleanupLeaseForClosedShard(closedShardId, childShardIds, trackedLeaseMap, leaseManager);
                        }
                    }
                }
            }
        }

        /** 
         * Delete lease for the closed shard. Rules for deletion are:
         * a/ the checkpoint for the closed shard is SHARD_END,
         * b/ there are leases for all the childShardIds and their checkpoint is NOT TRIM_HORIZON
         * Note: This method has package level access solely for testing purposes.
         * 
         * @param closedShardId Identifies the closed shard
         * @param childShardIds ShardIds of children of the closed shard
         * @param trackedLeases shardId->KinesisClientLease map with all leases we are tracking (should not be null)
         * @param leaseManager 
         * @throws ProvisionedThroughputException 
         * @throws InvalidStateException 
         * @throws DependencyException 
         */
        static /*synchronized*/ void CleanupLeaseForClosedShard(String closedShardId, HashSet<String> childShardIds, Dictionary<String, KinesisClientLease> trackedLeases, ILeaseManager<KinesisClientLease> leaseManager)
        {
            lock (@lock)
            {
                trackedLeases.TryGetValue(closedShardId, out KinesisClientLease leaseForClosedShard);
                List<KinesisClientLease> childShardLeases = new List<KinesisClientLease>();

                foreach (String childShardId in childShardIds)
                {
                    trackedLeases.TryGetValue(childShardId, out KinesisClientLease childLease);
                    if (childLease != null)
                    {
                        childShardLeases.Add(childLease);
                    }
                }

                if ((leaseForClosedShard != null) && (leaseForClosedShard.Checkpoint.Equals(ExtendedSequenceNumber.SHARD_END)) && (childShardLeases.Count == childShardIds.Count))
                {
                    bool okayToDelete = true;
                    foreach (KinesisClientLease lease in childShardLeases)
                    {
                        if (lease.Checkpoint.Equals(ExtendedSequenceNumber.TRIM_HORIZON))
                        {
                            okayToDelete = false;
                            break;
                        }
                    }

                    if (okayToDelete)
                    {
                        Trace.TraceInformation("Deleting lease for shard " + leaseForClosedShard.LeaseKey + " as it has been completely processed and processing of child shards has begun.");
                        leaseManager.DeleteLease(leaseForClosedShard);
                    }
                }
            }
        }

        /**
         * Helper method to create a new KinesisClientLease POJO for a shard.
         * Note: Package level access only for testing purposes
         * 
         * @param shard
         * @return
         */
        static KinesisClientLease NewKCLLease(Shard shard)
        {
            KinesisClientLease newLease = new KinesisClientLease();
            newLease.LeaseKey = shard.ShardId;
            List<String> parentShardIds = new List<string>();
            if (shard.ParentShardId != null)
            {
                parentShardIds.Add(shard.ParentShardId);
            }
            if (shard.AdjacentParentShardId != null)
            {
                parentShardIds.Add(shard.AdjacentParentShardId);
            }
            newLease.SetParentShardIds(parentShardIds);
            newLease.SetOwnerSwitchesSinceCheckpoint(0L);

            return newLease;
        }

        /**
         * Helper method to construct a shardId->Shard map for the specified list of shards.
         * 
         * @param shards List of shards
         * @return ShardId->Shard map
         */
        static Dictionary<String, Shard> ConstructShardIdToShardMap(List<Shard> shards)
        {
            Dictionary<String, Shard> shardIdToShardMap = new Dictionary<String, Shard>();
            foreach (Shard shard in shards)
            {
                shardIdToShardMap[shard.ShardId] = shard;
            }
            return shardIdToShardMap;
        }

        /**
         * Helper method to return all the open shards for a stream.
         * Note: Package level access only for testing purposes.
         * 
         * @param allShards All shards returved via DescribeStream. We assume this to represent a consistent shard list.
         * @return List of open shards (shards at the tip of the stream) - may include shards that are not yet active.
         */
        static List<Shard> GetOpenShards(List<Shard> allShards)
        {
            List<Shard> openShards = new List<Shard>();
            foreach (Shard shard in allShards)
            {
                String endingSequenceNumber = shard.SequenceNumberRange.EndingSequenceNumber;
                if (endingSequenceNumber == null)
                {
                    openShards.Add(shard);
                    Trace.WriteLine("Found open shard: " + shard.ShardId);
                }
            }
            return openShards;
        }

        private static ExtendedSequenceNumber ConvertToCheckpoint(InitialPositionInStreamExtended position)
        {
            ExtendedSequenceNumber checkpoint = null;

            if (position.GetInitialPositionInStream().Equals(InitialPositionInStream.TRIM_HORIZON))
            {
                checkpoint = ExtendedSequenceNumber.TRIM_HORIZON;
            }
            else if (position.GetInitialPositionInStream().Equals(InitialPositionInStream.LATEST))
            {
                checkpoint = ExtendedSequenceNumber.LATEST;
            }
            else if (position.GetInitialPositionInStream().Equals(InitialPositionInStream.AT_TIMESTAMP))
            {
                checkpoint = ExtendedSequenceNumber.AT_TIMESTAMP;
            }

            return checkpoint;
        }

        /** Helper class to compare leases based on starting sequence number of the corresponding shards.
         *
         */
        //[Serializable]
        private class StartingSequenceNumberAndShardIdBasedComparator : IComparer<KinesisClientLease>
        {
            //private static readonly long serialVersionUID = 1L;

            private readonly Dictionary<String, Shard> shardIdToShardMap;

            /**
             * @param shardIdToShardMapOfAllKinesisShards
             */
            public StartingSequenceNumberAndShardIdBasedComparator(Dictionary<String, Shard> shardIdToShardMapOfAllKinesisShards)
            {
                shardIdToShardMap = shardIdToShardMapOfAllKinesisShards;
            }

            /**
             * Compares two leases based on the starting sequence number of corresponding shards.
             * If shards are not found in the shardId->shard map supplied, we do a string comparison on the shardIds.
             * We assume that lease1 and lease2 are:
             *     a/ not null,
             *     b/ shards (if found) have non-null starting sequence numbers
             * 
             * {@inheritDoc}
             */
            public int Compare(KinesisClientLease lease1, KinesisClientLease lease2)
            {
                int result = 0;
                String shardId1 = lease1.LeaseKey;
                String shardId2 = lease2.LeaseKey;
                shardIdToShardMap.TryGetValue(shardId1, out Shard shard1);
                shardIdToShardMap.TryGetValue(shardId2, out Shard shard2);

                // If we found shards for the two leases, use comparison of the starting sequence numbers
                if (shard1 != null && shard2 != null)
                {
                    long sequenceNumber1 = long.Parse(shard1.SequenceNumberRange.StartingSequenceNumber);
                    long sequenceNumber2 = long.Parse(shard2.SequenceNumberRange.StartingSequenceNumber);
                    result = sequenceNumber1.CompareTo(sequenceNumber2);
                }

                if (result == 0)
                {
                    result = shardId1.CompareTo(shardId2);
                }

                return result;
            }
        }
    }
}