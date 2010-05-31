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
package org.apache.cassandra.db.context;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import org.apache.log4j.Logger;

/**
 * An implementation of a distributed increment-only counter context.
 *
 * The data structure is a list of (node id, count) pairs.  As a value is updated,
 * the node updating the value will increment its associated count.  The
 * aggregated count can then be determined by rolling up all the counts from each
 * (node id, count) pair.  NOTE: only a given node id may increment its associated
 * count and care must be taken that (node id, count) pairs are correctly made
 * consistent.
 */
public class MaxCounterContext extends AbstractCounterContext
{
    private static class LazyHolder
    {
        private static final MaxCounterContext maxCounterContext = new MaxCounterContext();
    }

    public static MaxCounterContext instance()
    {
        return LazyHolder.maxCounterContext;
    }

    /**
     * Updates a counter context for this node's id.
     *
     * @param context
     *            counter context
     * @param node
     *            InetAddress of node to update.
     * @return the updated version vector.
     */
    @Override
    public byte[] update(byte[] context, InetAddress node)
    {
        return update(context, node, 1);
    }

    @Override
    public byte[] update(byte[] context, InetAddress node, long delta)
    {
         // update timestamp
        FBUtilities.copyIntoBytes(context, 0, System.currentTimeMillis());

        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = timestampLength; offset < context.length; offset += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: increment count, shift to front
            long count = FBUtilities.byteArrayToLong(context, offset + idLength);

            // Take the max
            if (delta > count)
            {
                count = delta;
            }

            System.arraycopy(
                context,
                timestampLength,
                context,
                timestampLength + stepLength,
                offset - timestampLength);
            writeElement(context, nodeId, count);

            return context;
        }

        // node id not found: widen context
        byte[] previous = context;
        context = new byte[previous.length + stepLength];


        System.arraycopy(previous, 0, context, 0, timestampLength);
        writeElement(context, nodeId, delta);
        System.arraycopy(
            previous,
            timestampLength,
            context,
            timestampLength + stepLength,
            previous.length - timestampLength);

        return context;
    }

    /**
     * Return a context w/ an aggregated count for each node id.
     *
     * @param contexts
     *            a list of contexts to be merged
     */
    @Override
    public byte[] merge(List<byte[]> contexts)
    {
        //   1) take highest timestamp
        //   2) map id -> count
        //      a) local id:  sum counts; keep highest timestamp
        //      b) remote id: keep highest count (reconcile)
        //   3) create a context from sorted array
        long highestTimestamp = Long.MIN_VALUE;
        Map<FBUtilities.ByteArrayWrapper, Long> contextsMap =
            new HashMap<FBUtilities.ByteArrayWrapper, Long>();
        for (byte[] context : contexts)
        {
            // take highest timestamp
            highestTimestamp = Math.max(FBUtilities.byteArrayToLong(context, 0), highestTimestamp);

            // map id -> count
            for (int offset = timestampLength; offset < context.length; offset += stepLength)
            {
                FBUtilities.ByteArrayWrapper id = new FBUtilities.ByteArrayWrapper(
                        ArrayUtils.subarray(context, offset, offset + idLength));
                long count = FBUtilities.byteArrayToLong(context, offset + idLength);

                if (!contextsMap.containsKey(id))
                {
                    contextsMap.put(id, count);
                    continue;
                }

                // local id: take the max
                if (this.idWrapper.equals(id))
                {
                    long newMax = Math.max(count, (Long)contextsMap.get(id));
                    contextsMap.put(id, newMax);
                    continue;
                }

                // remote id: take the max
                if (((Long)contextsMap.get(id) < count))
                {
                    contextsMap.put(id, count);
                }
            }
        }

        List<Map.Entry<FBUtilities.ByteArrayWrapper, Long>> contextsList =
            new ArrayList<Map.Entry<FBUtilities.ByteArrayWrapper, Long>>(
                    contextsMap.entrySet());
        Collections.sort(
            contextsList,
            new Comparator<Map.Entry<FBUtilities.ByteArrayWrapper, Long>>()
            {
                public int compare(
                    Map.Entry<FBUtilities.ByteArrayWrapper, Long> e1,
                    Map.Entry<FBUtilities.ByteArrayWrapper, Long> e2)
                {
                    // reversed
                    return e2.getValue().compareTo(e1.getValue());
                }
            });

        int length = contextsList.size();
        byte[] merged = new byte[timestampLength + (length * stepLength)];
        FBUtilities.copyIntoBytes(merged, 0, highestTimestamp);
        for (int i = 0; i < length; i++)
        {
            Map.Entry<FBUtilities.ByteArrayWrapper, Long> entry = contextsList.get(i);
            writeElementAtStepOffset(
                merged,
                i,
                entry.getKey().data,
                entry.getValue().longValue());
        }
        return merged;
    }

    // return an aggregated count across all node ids
    @Override
    public byte[] aggregateNodes(byte[] context)
    {
        long max = Long.MIN_VALUE;

        for (int offset = timestampLength; offset < context.length; offset += stepLength)
        {
            long count = FBUtilities.byteArrayToLong(context, offset + idLength);
            if (count > max)
            {
                max = count;
            }
        }

        return FBUtilities.toByteArray(max);
    }
}
