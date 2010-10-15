/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.BitSet;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.io.ICompactSerializer;

import org.apache.log4j.Logger;

public class BloomFilter extends Filter
{
    private static final Logger logger = Logger.getLogger(BloomFilter.class);
    static ICompactSerializer<BloomFilter> serializer_ = new BloomFilterSerializer();

    protected static final int EXCESS = 20;

    public static ICompactSerializer<BloomFilter> serializer()
    {
        return serializer_;
    }

    protected BitSet filter;

    BloomFilter(int hashes, BitSet filter)
    {
        hashCount = hashes;
        this.filter = filter;
    }

    private static BitSet bucketsFor(long numElements, int bucketsPer)
    {
        long numBits = numElements * bucketsPer + EXCESS;
        return new BitSet((int)Math.min(Integer.MAX_VALUE, numBits));
    }

    /**
     * Calculates the maximum number of buckets per element that this implementation
     * can support.  Crucially, it will lower the bucket count if necessary to meet
     * BitSet's size restrictions.
     */
    private static int maxBucketsPerElement(long numElements)
    {
        numElements = Math.max(1, numElements);
        double v = (Integer.MAX_VALUE - EXCESS) / (double)numElements;
        if (v < 1.0)
        {
            throw new UnsupportedOperationException("Cannot compute probabilities for " + numElements + " elements.");
        }
        return Math.min(BloomCalculations.probs.length - 1, (int)v);
    }

    /**
     * @return A BloomFilter with the lowest practical false positive probability
     * for the given number of elements.
     */
    public static BloomFilter getFilter(long numElements, int targetBucketsPerElem)
    {
        int maxBucketsPerElement = Math.max(1, maxBucketsPerElement(numElements));
        int bucketsPerElement = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement >= targetBucketsPerElem)
        {
            BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
            return new BloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
        }
        logger.warn(String.format("BigBloomFilter being used!!! Cannot provide optimal BloomFilter for %d elements (%d/%d buckets per element).",
                                  numElements, bucketsPerElement, targetBucketsPerElem));

        maxBucketsPerElement = Math.max(1, BigBloomFilter.maxBucketsPerElementForBigBloomFilter(numElements));
        bucketsPerElement    = Math.min(targetBucketsPerElem, maxBucketsPerElement);
        if (bucketsPerElement < targetBucketsPerElem)
        {
            logger.warn(String.format("Cannot provide optimal BigBloomFilter for %d elements (%d/%d buckets per element).",
                                      numElements, bucketsPerElement, targetBucketsPerElem));
        }

        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement);
        return new BigBloomFilter(
            Math.min((long)Long.MAX_VALUE, numElements * (long)spec.bucketsPerElement + (long)EXCESS),
            spec.K);
    }

    /**
     * @return The smallest BloomFilter that can provide the given false positive
     * probability rate for the given number of elements.
     *
     * Asserts that the given probability can be satisfied using this filter.
     */
    public static BloomFilter getFilter(long numElements, double maxFalsePosProbability)
    {
        assert maxFalsePosProbability <= 1.0 : "Invalid probability";
        int bucketsPerElement = maxBucketsPerElement(numElements);
        BloomCalculations.BloomSpecification spec = BloomCalculations.computeBloomSpec(bucketsPerElement, maxFalsePosProbability);
        return new BloomFilter(spec.K, bucketsFor(numElements, spec.bucketsPerElement));
    }

    public void clear()
    {
        filter.clear();
    }

    int buckets()
    {
        return filter.size();
    }

    public boolean isPresent(String key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            if (!filter.get(bucketIndex))
            {
                return false;
            }
        }
        return true;
    }

    public boolean isPresent(byte[] key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            if (!filter.get(bucketIndex))
            {
                return false;
            }
        }
        return true;
    }

    /*
     @param key -- value whose hash is used to fill
     the filter.
     This is a general purpose API.
     */
    public void add(String key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            filter.set(bucketIndex);
        }
    }

    public void add(byte[] key)
    {
        for (int bucketIndex : getHashBuckets(key))
        {
            filter.set(bucketIndex);
        }
    }

    public String toString()
    {
        return filter.toString();
    }

    ICompactSerializer tserializer()
    {
        return serializer_;
    }

    int emptyBuckets()
    {
        int n = 0;
        for (int i = 0; i < buckets(); i++)
        {
            if (!filter.get(i))
            {
                n++;
            }
        }
        return n;
    }

    /** @return a BloomFilter that always returns a positive match, for testing */
    public static BloomFilter alwaysMatchingBloomFilter()
    {
        BitSet set = new BitSet(64);
        set.set(0, 64);
        return new BloomFilter(1, set);
    }
}

class BloomFilterSerializer implements ICompactSerializer<BloomFilter>
{
    public void serialize(BloomFilter bf, DataOutputStream dos)
            throws IOException
    {
        dos.writeInt(bf.getHashCount());

        ObjectOutputStream oos = new ObjectOutputStream(dos);
        if (bf instanceof BigBloomFilter)
        {
            for (BitSet bs : ((BigBloomFilter)bf).filter.buckets)
            {
                oos.writeObject(bs);
                oos.flush();
            }
            return;
        }

        // BloomFilter
        oos.writeObject(bf.filter);
        oos.flush();
    }

    public BloomFilter deserialize(DataInputStream dis) throws IOException
    {
        int hashes = dis.readInt();
        
        ObjectInputStream ois = new ObjectInputStream(dis);
        try
        {
            Object bs = ois.readObject();
            if (dis.available() == 0)
                return new BloomFilter(hashes, (BitSet)bs);

            List<BitSet> bsList = new LinkedList<BitSet>();
            bsList.add((BitSet)bs);
            while (dis.available() > 0)
            {
                bsList.add((BitSet)ois.readObject());
            }
            return new BigBloomFilter(
                new BigBitSet(bsList.toArray(new BitSet[0])),
                hashes);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }
}
