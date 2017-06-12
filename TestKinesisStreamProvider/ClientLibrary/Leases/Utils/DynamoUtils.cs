using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Utils
{
    /**
 * Static utility functions used by our LeaseSerializers.
 */
    public class DynamoUtils
    {

        public static AttributeValue createAttributeValue(ICollection<String> collectionValue)
        {
            if (collectionValue == null || collectionValue.Count == 0)
            {
                throw new ArgumentException("Collection attributeValues cannot be null or empty.");
            }

            return new AttributeValue(collectionValue.ToList());
        }

        public static AttributeValue createAttributeValue(String stringValue)
        {
            if (String.IsNullOrWhiteSpace(stringValue))
            {
                throw new ArgumentException("String attributeValues cannot be null or empty.");
            }

            return new AttributeValue(stringValue);
        }

        public static AttributeValue createAttributeValue(long longValue)
        {
            return new AttributeValue(longValue.ToString());
        }

        public static long SafeGetLong(Dictionary<String, AttributeValue> dynamoRecord, String key)
        {
            AttributeValue av = dynamoRecord[key];
            if (av == null)
            {
                return 0; // monne: this was return null....
            }
            else
            {
                return long.Parse(av.N);
            }
        }

        public static String SafeGetString(Dictionary<String, AttributeValue> dynamoRecord, String key)
        {
            AttributeValue av = dynamoRecord[key];
            if (av == null)
            {
                return null;
            }
            else
            {
                return av.S;
            }
        }

        public static List<String> safeGetSS(Dictionary<String, AttributeValue> dynamoRecord, String key)
        {
            AttributeValue av = dynamoRecord[key];

            if (av == null)
            {
                return new List<String>();
            }
            else
            {
                return av.SS;
            }
        }

    }

}
