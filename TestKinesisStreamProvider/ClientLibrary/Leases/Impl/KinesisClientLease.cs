using System;
using System.Collections.Generic;
using System.Linq;
using TestKinesisStreamProvider.ClientLibrary.Types;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Impl
{
    /**
    * A Lease subclass containing KinesisClientLibrary related fields for checkpoints.
    */
    public class KinesisClientLease : Lease
    {
        private HashSet<String> parentShardIds = new HashSet<String>();

        public ExtendedSequenceNumber Checkpoint { get; set; }

        public long OwnerSwitchesSinceCheckpoint { get; set; } = 0L;

        public KinesisClientLease()
        {

        }

        public KinesisClientLease(KinesisClientLease other) : base(other)
        {
            this.Checkpoint = other.Checkpoint;
            this.OwnerSwitchesSinceCheckpoint = other.OwnerSwitchesSinceCheckpoint;
            other.GetParentShardIds().ToList().ForEach(id => this.parentShardIds.Add(id));
        }

        KinesisClientLease(String leaseKey, String leaseOwner, long leaseCounter, Guid concurrencyToken,
                long lastCounterIncrementNanos, ExtendedSequenceNumber checkpoint, long ownerSwitchesSinceCheckpoint,
                HashSet<String> parentShardIds)
            : base(leaseKey, leaseOwner, leaseCounter, concurrencyToken, lastCounterIncrementNanos)
        {
            this.Checkpoint = checkpoint;
            this.OwnerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
            parentShardIds.ToList().ForEach(id => this.parentShardIds.Add(id));
        }

        public override void Update<T>(T other)
        {
            base.Update(other);
            if (!(other is KinesisClientLease))
            {
                throw new ArgumentException("Must pass KinesisClientLease object to KinesisClientLease.update(Lease)");
            }
            KinesisClientLease casted = other as KinesisClientLease;

            SetOwnerSwitchesSinceCheckpoint(casted.OwnerSwitchesSinceCheckpoint);
            SetCheckpoint(casted.Checkpoint);
            SetParentShardIds(casted.GetParentShardIds());
        }

        /**
         * @return shardIds that parent this lease. Used for resharding.
         */
        public HashSet<String> GetParentShardIds()
        {
            return new HashSet<String>(parentShardIds);
        }

        /**
         * Sets checkpoint.
         * 
         * @param checkpoint may not be null
         */
        public void SetCheckpoint(ExtendedSequenceNumber checkpoint)
        {
            VerifyNotNull(checkpoint, "Checkpoint should not be null");

            this.Checkpoint = checkpoint;
        }

        /**
         * Sets ownerSwitchesSinceCheckpoint.
         * 
         * @param ownerSwitchesSinceCheckpoint may not be null
         */
        public void SetOwnerSwitchesSinceCheckpoint(long ownerSwitchesSinceCheckpoint)
        {
            VerifyNotNull(ownerSwitchesSinceCheckpoint, "ownerSwitchesSinceCheckpoint should not be null");

            this.OwnerSwitchesSinceCheckpoint = ownerSwitchesSinceCheckpoint;
        }

        /**
         * Sets parentShardIds.
         * 
         * @param parentShardIds may not be null
         */
        public void SetParentShardIds(ICollection<String> parentShardIds)
        {
            VerifyNotNull(parentShardIds, "parentShardIds should not be null");

            this.parentShardIds.Clear();
            parentShardIds.ToList().ForEach(id => this.parentShardIds.Add(id));
        }

        private void VerifyNotNull(Object @object, String message)
        {
            if (@object == null)
            {
                throw new ArgumentException(message);
            }
        }


        public override int GetHashCode()
        {
            const int prime = 31;
            int result = base.GetHashCode();
            result = prime * result + ((Checkpoint == null) ? 0 : Checkpoint.GetHashCode());
            result = prime * result + ((OwnerSwitchesSinceCheckpoint == 0) ? 0 : OwnerSwitchesSinceCheckpoint.GetHashCode());
            result = prime * result + ((parentShardIds == null) ? 0 : parentShardIds.GetHashCode());
            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if (!base.Equals(obj))
                return false;
            if (this.GetType() != obj.GetType())
                return false;
            KinesisClientLease other = (KinesisClientLease)obj;
            if (Checkpoint == null)
            {
                if (other.Checkpoint != null)
                    return false;
            }
            else if (!Checkpoint.Equals(other.Checkpoint))
                return false;
            /*if (OwnerSwitchesSinceCheckpoint == null)
            {
                if (other.OwnerSwitchesSinceCheckpoint != null)
                    return false;
            }
            else*/ if (!OwnerSwitchesSinceCheckpoint.Equals(other.OwnerSwitchesSinceCheckpoint))
                return false;
            if (parentShardIds == null)
            {
                if (other.parentShardIds != null)
                    return false;
            }
            else if (!parentShardIds.Equals(other.parentShardIds))
                return false;
            return true;
        }

        /**
         * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
         * 
         * @return A deep copy of this object.
         */
        //@SuppressWarnings("unchecked")
        public override T Copy<T>()
        {
            return new KinesisClientLease(this) as T;
        }
    }
}
