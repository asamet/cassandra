package org.apache.cassandra.utils;

import java.nio.charset.Charset;

import java.util.BitSet;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.io.ICompactSerializer;
import org.apache.cassandra.utils.BloomFilter;

import org.apache.log4j.Logger;

/**
 * Configurable bloom filter implementation with up to 2^30 * Integer.MAX_VALUE bits, 
 * using MurmurHash for hashing.
 * 
 * Not threadsafe without external synchronization
 * 
 * @author sjiang; adapted by kelvin
 *
 */
public class BigBloomFilter extends BloomFilter
{
    private static final Logger logger = Logger.getLogger(BigBloomFilter.class);

    private static final int log2PerBucket = 30;

    protected BigBitSet filter;
    private final Charset CHARSET_FOR_STRINGHASH = Charset.forName("UTF-16");

    public static ICompactSerializer<BloomFilter> serializer()
    {
        return BloomFilter.serializer();
    }

    protected static int maxBucketsPerElementForBigBloomFilter(long numElements)
    {
        numElements = Math.max(1L, numElements);
        double v = (Long.MAX_VALUE - EXCESS) / (double)numElements;
        if (v < 1.0)
        {
            throw new UnsupportedOperationException("Cannot compute probabilities for " + numElements + " elements.");
        }
        return Math.min(BloomCalculations.probs.length - 1, (int)v);
    }

    public BigBloomFilter(long nbits, int hashCount)
    {
//TODO: REFACTOR
        super(hashCount, null);

        assert nbits > 0 : "nbits must be positive: " + nbits;
        assert nbits <= ((0x1L << log2PerBucket)*(long)Integer.MAX_VALUE) : "nbits must be less than " + ((0x1L << log2PerBucket)*(long)Integer.MAX_VALUE);

        assert hashCount > 0 : "hashCount must be positive: " + hashCount;
        this.hashCount = hashCount;

        filter = new BigBitSet(nbits);
    }

    public BigBloomFilter(BigBitSet bbs, int hashCount)
    {
//TODO: REFACTOR
        super(hashCount, null);

        filter = bbs;
        this.hashCount = hashCount;
    }

    @Override
    public void clear()
    {
        filter.clear();
    }

    long bigBuckets()
    {
        return filter.size();
    }

    BigBitSet bigFilter()
    {
        return filter;
    }

    private long[] getHashBits(byte[] key)
    {
        //TODO (sjiang) optimize with anonymous functions and/or allow custom hash
        long[] bitIndices = new long[hashCount];
        long hash1 = LongMurmurHash.longHash(key, 0);
        long hash2 = LongMurmurHash.longHash(key, (int)(hash1 & 0xffffffffL));

        for (int i = 0; i < hashCount; ++i)
        {
            bitIndices[i] = Math.abs((hash1 + (long)i * hash2) % filter.size());
        }
        return bitIndices;
    }

    /**
     * @param bits the set of bit offsets to check
     * @returns true if all of the bits are set to true
     */
    private boolean checkBits(long[] bits)
    {
        for (long bitIndex : bits) {
            if (!filter.get(bitIndex))
                return false;
        }
        return true;
    }

    @Override
    public boolean isPresent(String key)
    {
        return isPresent(key.getBytes(CHARSET_FOR_STRINGHASH));
    }

    @Override
    public boolean isPresent(byte[] key)
    {
        long[] hashBits = getHashBits(key);
        return checkBits(hashBits);
    }

    private void setBits(long[] bits)
    {
        for (long bitIndex : bits)
        {
            filter.set(bitIndex);
        }
    }

    @Override
    public void add(String key)
    {
        add(key.getBytes(CHARSET_FOR_STRINGHASH));
    }

    @Override
    public void add(byte[] key)
    {
        long[] hashBits = getHashBits(key);
        setBits(hashBits);
    }

    @Override
    public String toString()
    {
        return filter.toString();
    }

    @Override
    int buckets()
    {
        //XXX: backwards-compatible
        return (int)filter.size();
    }

    @Override
    int emptyBuckets()
    {
        //XXX: backwards-compatible
        return (int)(filter.size() - filter.cardinality());
    }

    public boolean equals(Object o)
    {
        if (!(o instanceof BigBloomFilter))
            return false;

        BigBloomFilter otherBBF = (BigBloomFilter)o;
        if (this.hashCount != otherBBF.hashCount)
            return false;

        BigBitSet otherFilter = otherBBF.filter;
        if (this.filter.size() != otherFilter.size())
            return false;
        for (int i=0; i < filter.buckets.length; i++)
        {
            if (!filter.buckets[i].equals(otherFilter.buckets[i]))
                return false;
        }

        return true;
    }
}

