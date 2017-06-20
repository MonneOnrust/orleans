using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Utils
{
    public class HashCodeBuilder
    {
        public static int ComputeHashFrom(params object[] obj)
        {
            ulong res = 0;
            for (uint i = 0; i < obj.Length; i++)
            {
                object val = obj[i];
                res += val == null ? i : (ulong)val.GetHashCode() * (1 + 2 * i);
            }
            return (int)(uint)(res ^ (res >> 32));
        }
    }
}
