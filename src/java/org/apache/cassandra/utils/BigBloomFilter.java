package org.apache.cassandra.utils;

import java.nio.charset.Charset;

import java.util.BitSet;

import org.apache.commons.lang.ArrayUtils;

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
    protected final int numHashes;
    protected final long numBits;
    private BigBitSet filter;
    private final Charset CHARSET_FOR_STRINGHASH = Charset.forName("UTF-16");

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

    public BigBloomFilter(long numBits, int numHashes)
    {
        super(numHashes, new BitSet(1));
System.out.println("BigBloomFilter: 0: " + numBits + "; " + numHashes);

        assert numBits > 0 : "numBits must be positive: " + numBits;
        assert numBits <= ((0x1L << log2PerBucket)*(long)Integer.MAX_VALUE) : "numBits must be less than " + ((0x1L << log2PerBucket)*(long)Integer.MAX_VALUE);

        assert numHashes > 0 : "numHashes must be positive: " + numHashes;
        this.numHashes = numHashes;

        filter = new BigBitSet(numBits);
        this.numBits = filter.size();
    }

    public BigBloomFilter(BigBitSet bbs, int numHashes)
    {
        super(numHashes, new BitSet(1));
System.out.println("BigBloomFilter: 1: " + bbs.size() + "; " + numHashes);

        numBits = bbs.size();
        filter = bbs;
        this.numHashes = numHashes;
    }

    @Override
    int getHashCount()
    {
        return numHashes;
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

    private long[] getHashBits(byte[] key) {
        //TODO (sjiang) optimize with anonymous functions and/or allow custom hash
        long[] bitIndices = new long[numHashes];
        long hash1 = LongMurmurHash.longHash(key, 0);
        long hash2 = LongMurmurHash.longHash(key, (int)(hash1 & 0xffffffffL));

        for (int i = 0; i < numHashes; ++i) {
            bitIndices[i] = Math.abs((hash1 + (long)i * hash2) % numBits);
        }
        return bitIndices;
    }

    /**
     * @param bits the set of bit offsets to check
     * @returns true if all of the bits are set to true
     */
    private boolean checkBits(long[] bits) {
System.out.println("checkBits: 0:");
        for (long bitIndex : bits) {
            if (!filter.get(bitIndex))
                return false;
        }
        return true;
    }

    @Override
    public boolean isPresent(String key)
    {
System.out.println("isPresent: 0: " + key);
        return isPresent(key.getBytes(CHARSET_FOR_STRINGHASH));
    }

    @Override
    public boolean isPresent(byte[] key)
    {
System.out.println("isPresent: A: " + ArrayUtils.toString(key));
        long[] hashBits = getHashBits(key);
System.out.println("isPresent: B:");
        return checkBits(hashBits);
    }

    private void setBits(long[] bits) {
System.out.println("setBits: 0:");
        for (long bitIndex : bits) {
            filter.set(bitIndex);
        }
    }

    @Override
    public void add(String key)
    {
System.out.println("add: 0:");
        add(key.getBytes(CHARSET_FOR_STRINGHASH));
    }

    @Override
    public void add(byte[] key)
    {
System.out.println("add: A:");
        long[] hashBits = getHashBits(key);
System.out.println("add: B:");
        setBits(hashBits);
System.out.println("add: C:");
    }

    @Override
    public String toString()
    {
        return filter.toString();
    }

    @Override
    int buckets()
    {
        return (int)filter.size();
    }

    @Override
    int emptyBuckets()
    {
        //XXX: backwards-compatible
        return (int)(numBits - filter.cardinality());
    }

//TODO: MAYBE?
    /** @return a BloomFilter that always returns a positive match, for testing */
//    @Override
//    public static BloomFilter alwaysMatchingBloomFilter()
}

