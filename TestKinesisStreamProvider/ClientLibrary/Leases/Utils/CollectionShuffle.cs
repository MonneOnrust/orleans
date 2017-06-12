﻿using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider.ClientLibrary.Leases.Utils
{
    public static class CollectionShuffle
    {
        private static Random rng = new Random();

        public static void Shuffle<T>(this IList<T> list)
        {
            int n = list.Count;
            while (n > 1)
            {
                n--;
                int k = rng.Next(n + 1);
                T value = list[k];
                list[k] = list[n];
                list[n] = value;
            }
        }
    }
}
