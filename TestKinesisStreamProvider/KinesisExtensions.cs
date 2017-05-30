using Amazon.Runtime;
using System;
using System.Collections.Generic;
using System.Text;

namespace TestKinesisStreamProvider
{
    public static class Extensions
    {
        public static void EnsureSuccessResponse(this AmazonWebServiceResponse response)
        {
            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception($"Incorrect HTTP response code: {response.HttpStatusCode}");
            }
        }
    }
}
