using System;
using System.Collections.Generic;
using System.Text;

namespace KinesisClientLibrary.ClientLibrary.Proxies
{
    /** 
 * Interface for a KinesisProxyFactory.
 *
 */
    public interface IKinesisProxyFactory
    {

        /**
         * Return an IKinesisProxy object for the specified stream.
         * @param streamName Stream from which data is consumed.
         * @return IKinesisProxy object.
         */
        IKinesisProxy GetProxy(String streamName);

    }
}