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
System.out.println("testSerialize: 0: " + bf.toString());
System.out.println("testSerialize: 1: " + bf.isPresent("a"));
        assert bf.isPresent("a");
System.out.println("testSerialize: 2: " + bf.isPresent("b"));
        assert !bf.isPresent("b");

        DataOutputBuffer out = new DataOutputBuffer();
        BloomFilter.serializer().serialize(bf, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        Filter f2 = BloomFilter.serializer().deserialize(new DataInputStream(in));

        BigBloomFilter origBF = (BigBloomFilter)bf;
        BigBitSet origBBS = origBF.bigFilter();
        BigBloomFilter newBF  = (BigBloomFilter)f2;
        BigBitSet newBBS  = newBF.bigFilter();
        for (int i=0; i < origBBS.buckets.length; i++)
        {
System.out.println("testSerialize: X1: orig: " + origBBS.buckets[i]);
System.out.println("testSerialize: X2: new:  " + newBBS.buckets[i]);
            assert origBBS.buckets[i].equals(newBBS.buckets[i]) : "BBS index wrong: " + i;
        }
System.out.println("testSerialize: Y1: orig: " + origBF.numBits);
System.out.println("testSerialize: Y2: new:  " + newBF.numBits);

System.out.println("testSerialize: Z1: orig: " + origBF.numHashes);
System.out.println("testSerialize: Z2: new:  " + newBF.numHashes);

System.out.println("testSerialize: A: " + f2.toString());
System.out.println("testSerialize: B: " + f2.isPresent("a"));
        assert f2.isPresent("a");
System.out.println("testSerialize: C: " + f2.isPresent("b"));
        assert !f2.isPresent("b");
    }
}
