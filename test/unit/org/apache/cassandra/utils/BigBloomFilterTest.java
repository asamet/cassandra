package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.cassandra.io.util.DataOutputBuffer;

import org.junit.Before;
import org.junit.Test;

public class BigBloomFilterTest
{
    public BloomFilter bf;

    public BigBloomFilterTest()
    {
        bf = BloomFilter.getFilter(179042688L, 15);
    }

    @Before
    public void clear()
    {
        bf.clear();
    }

    @Test
    public void testClass()
    {
        assert bf instanceof BigBloomFilter : "Wrong Class! Not BigBloomFilter";
    }

    @Test
    public void testSerialize() throws IOException
    {
        bf.add("a");
        assert bf.isPresent("a");
        assert !bf.isPresent("b");

        DataOutputBuffer out = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        Filter f2 = BloomFilter.serializer().deserialize(new DataInputStream(in));

        BigBloomFilter origBF = (BigBloomFilter)bf;
        BigBloomFilter newBF  = (BigBloomFilter)f2;
        assert origBF.hashCount == newBF.hashCount;
        assert origBF.filter.size() == newBF.filter.size();

        BigBitSet origBBS = origBF.filter;
        BigBitSet newBBS  = newBF.filter;
        for (int i=0; i < origBBS.buckets.length; i++)
        {
            assert origBBS.buckets[i].equals(newBBS.buckets[i]) : "BBS index wrong: " + i;
        }

        assert f2.isPresent("a");
        assert !f2.isPresent("b");
    }
}
