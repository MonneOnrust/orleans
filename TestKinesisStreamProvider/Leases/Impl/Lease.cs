using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Impl
{
    /**
 * This class contains data pertaining to a Lease. Distributed systems may use leases to partition work across a
 * fleet of workers. Each unit of work (identified by a leaseKey) has a corresponding Lease. Every worker will contend
 * for all leases - only one worker will successfully take each one. The worker should hold the lease until it is ready to stop
 * processing the corresponding unit of work, or until it fails. When the worker stops holding the lease, another worker will
 * take and hold the lease.
 */
    public class Lease
    {
        /*
         * See javadoc for System.nanoTime - summary:
         * 
         * Sometimes System.nanoTime's return values will wrap due to overflow. When they do, the difference between two
         * values will be very large. We will consider leases to be expired if they are more than a year old.
         */
        private static readonly long MAX_ABS_AGE_NANOS = (long)TimeSpan.FromDays(365).TotalMilliseconds * 1000000;

        public String LeaseKey
        {
            get { return leaseKey; }
            set
            {
                if (this.leaseKey != null)
                {
                    throw new ArgumentException("LeaseKey is immutable once set");
                }
                VerifyNotNull(leaseKey, "LeaseKey cannot be set to null");
                leaseKey = value;
            }
        }
        private string leaseKey;

        public String LeaseOwner { get; set; }
        public long LeaseCounter { get; set; } = 0L;

        /*
         * This field is used to prevent updates to leases that we have lost and re-acquired. It is deliberately not
         * persisted in DynamoDB and excluded from hashCode and equals.
         */
        public Guid ConcurrencyToken { get { return concurrencyToken; } set { VerifyNotNull(value, "concurencyToken cannot be null"); concurrencyToken = value; } }
        private Guid concurrencyToken;

        /*
         * This field is used by LeaseRenewer and LeaseTaker to track the last time a lease counter was incremented. It is
         * deliberately not persisted in DynamoDB and excluded from hashCode and equals.
         */
        public long LastCounterIncrementNanos { get; set; }

        /**
         * Constructor.
         */
        public Lease()
        {
        }

        /**
         * Copy constructor, used by clone().
         * 
         * @param lease lease to copy
         */
        protected Lease(Lease lease) : this(lease.LeaseKey, lease.LeaseOwner, lease.LeaseCounter, lease.ConcurrencyToken, lease.LastCounterIncrementNanos)
        {
        }

        protected Lease(String leaseKey, String leaseOwner, long leaseCounter, Guid concurrencyToken, long lastCounterIncrementNanos)
        {
            this.LeaseKey = leaseKey;
            this.LeaseOwner = leaseOwner;
            this.LeaseCounter = leaseCounter;
            this.ConcurrencyToken = concurrencyToken;
            this.LastCounterIncrementNanos = lastCounterIncrementNanos;
        }

        /**
         * Updates this Lease's mutable, application-specific fields based on the passed-in lease object. Does not update
         * fields that are internal to the leasing library (leaseKey, leaseOwner, leaseCounter).
         * 
         * @param other
         */
        public virtual void Update<T>(T other) where T : Lease
        {
            // The default implementation (no application-specific fields) has nothing to do.
        }

        /**
         * @param leaseDurationNanos duration of lease in nanoseconds
         * @param asOfNanos time in nanoseconds to check expiration as-of
         * @return true if lease is expired as-of given time, false otherwise
         */
        public bool IsExpired(long leaseDurationNanos, long asOfNanos)
        {
            //if (LastCounterIncrementNanos == null)
            //{
            //    return true;
            //}

            long age = asOfNanos - LastCounterIncrementNanos;
            // see comment on MAX_ABS_AGE_NANOS
            if (Math.Abs(age) > MAX_ABS_AGE_NANOS)
            {
                return true;
            }
            else
            {
                return age > leaseDurationNanos;
            }
        }


        public override int GetHashCode()
        {
            const int prime = 31;
            int result = 1;
            result = prime * result + LeaseCounter.GetHashCode();
            result = prime * result + ((LeaseOwner == null) ? 0 : LeaseOwner.GetHashCode());
            result = prime * result + ((LeaseKey == null) ? 0 : LeaseKey.GetHashCode());
            return result;
        }

        public override bool Equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (GetType() != obj.GetType())
                return false;
            Lease other = (Lease)obj;
            if (!LeaseCounter.Equals(other.LeaseCounter))
                return false;
            if (LeaseOwner == null)
            {
                if (other.LeaseOwner != null)
                    return false;
            }
            else if (!LeaseOwner.Equals(other.LeaseOwner))
                return false;
            if (leaseKey == null)
            {
                if (other.LeaseKey != null)
                    return false;
            }
            else if (!leaseKey.Equals(other.LeaseKey))
                return false;
            return true;
        }

        //public override string ToString()
        //{
        //    return Jackson.toJsonPrettyString(this);
        //}

        /**
         * Returns a deep copy of this object. Type-unsafe - there aren't good mechanisms for copy-constructing generics.
         * 
         * @return A deep copy of this object.
         */
        //@SuppressWarnings("unchecked")
        public virtual T Copy<T>() where T : Lease
        {
            return (T)new Lease(this);
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
