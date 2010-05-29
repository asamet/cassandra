package org.apache.cassandra.service;

import org.apache.cassandra.db.CounterClock;
import org.apache.cassandra.db.context.IncrementCounterContext;
import org.junit.Before;

public class AntiEntropyServiceIncrementCounterTest extends AntiEntropyServiceTestAbstract
{

    @Before
    public void init()
    {
        tablename = "Keyspace4";
        cfname = "IncrementCounter1";
        clock = new CounterClock(new byte[] {}, IncrementCounterContext.instance());
    }
    
}
