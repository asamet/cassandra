/**
 * Configurable bloom filter implementation with up to 2^30 * Integer.MAX_VALUE bits, 
 * using MurmurHash for hashing.
 * 
 * Not threadsafe without external synchronization
 * 
 * @author sjiang
 *
 */
package org.apache.cassandra.utils;

import java.nio.charset.Charset;
import java.util.BitSet;

public class BigBitSet
{
    // BitSet only allows max 2^31 - 1 bits which makes calculations awkward,
    // so limit size per BitSet to 2^30
    private static final int log2PerBucket = 30;

    protected final BitSet[] buckets;
    protected final long maxBits;

    public BigBitSet(long nbits)
    {
        assert nbits <= ((0x1L << log2PerBucket)*(long)Integer.MAX_VALUE) : "nbits must be less than " + ((0x1L << log2PerBucket)*(long)Integer.MAX_VALUE);
        assert nbits > 0 : "nbits must be positive: " + nbits;

        int numBuckets = bucketsRequiredFor(nbits);

        buckets = new BitSet[numBuckets];
        long size = 0L;
        for (int i = 0; i < numBuckets-1; ++i)
        {
            buckets[i] = new BitSet(bitsPerBitSet());
            size += (long)buckets[i].size();
        }
        buckets[numBuckets-1] = new BitSet(indexInBucket(nbits)+1);
        size += buckets[numBuckets-1].size();

        maxBits = size;
    }

    public BigBitSet(BitSet[] buckets)
    {
        this.buckets = buckets;

        long size = 0L;
        for (BitSet bs : this.buckets)
        {
            size += (long)bs.size();
        }

        maxBits = size;
    }

    public long size()
    {
        return maxBits;
    }

    public void clear()
    {
        for (BitSet bs : buckets)
        {
            bs.clear();
        }
    }

    public void clear(long bitIndex)
    {
        buckets[bucketForBit(bitIndex)].clear(indexInBucket(bitIndex));
    }

    public boolean get(long bitIndex)
    {
        return buckets[bucketForBit(bitIndex)].get(indexInBucket(bitIndex));
    }

    public void set(long bitIndex)
    {
        buckets[bucketForBit(bitIndex)].set(indexInBucket(bitIndex));
    }

    public long cardinality()
    {
        long result = 0;
        for (BitSet bs : buckets)
        {
            result += bs.cardinality();
        }
        return result;
    }

    private int bucketsRequiredFor(long nbits)
    {
        return (int)((nbits-1) >> log2PerBucket) + 1;
    }

    private int bucketForBit(long bitIndex)
    {
        return (int)(bitIndex >> log2PerBucket);
    }

    private int bitsPerBitSet()
    {
        return 0x1 << log2PerBucket;
    }

    private int indexInBucket(long bitIndex)
    {
        return (int)(bitIndex & (long)((0x1 << log2PerBucket)-1));
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (BitSet bs : buckets)
        {
            sb.append(bs.toString());
        }
        sb.append("}");
        return sb.toString();
    }
}

