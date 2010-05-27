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
public class MinCounterContext extends AbstractCounterContext
{
    private static class LazyHolder
    {
        private static final MinCounterContext minCounterContext = new MinCounterContext();
    }

    public static MinCounterContext instance()
    {
        return LazyHolder.minCounterContext;
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
        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: increment count, shift to front
            long count = FBUtilities.byteArrayToLong(context, offset + idLength);
            if (delta < count)
            {
                count = delta;
            }
            System.arraycopy(context, 0, context, stepLength, offset);
            writeElement(context, nodeId, count);

            return context;
        }

        // node id not found: widen context
        byte[] previous = context;
        context = new byte[previous.length + stepLength];

        writeElement(context, nodeId, delta);
        System.arraycopy(previous, 0, context, stepLength, previous.length);

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
        // strategy:
        //   map id -> count, timestamp pairs
        //      1) local id:  sum counts; keep highest timestamp
        //      2) remote id: keep highest count (reconcile)
        //   create an array sorted by timestamp
        //   create a context from sorted array
        Map<FBUtilities.ByteArrayWrapper, Pair<Long, Long>> contextsMap =
            new HashMap<FBUtilities.ByteArrayWrapper, Pair<Long, Long>>();
        for (byte[] context : contexts)
        {
            for (int offset = 0; offset < context.length; offset += stepLength)
            {
                FBUtilities.ByteArrayWrapper id = new FBUtilities.ByteArrayWrapper(
                        ArrayUtils.subarray(context, offset, offset + idLength));
                long count = FBUtilities.byteArrayToLong(context, offset + idLength);
                long timestamp = FBUtilities.byteArrayToLong(context, offset + idLength + countLength);

                if (!contextsMap.containsKey(id))
                {
                    contextsMap.put(id, new Pair<Long, Long>(count, timestamp));
                    continue;
                }

                // local id: min counts
                if (this.idWrapper.equals(id))
                {
                    Pair<Long, Long> countTimestampPair = contextsMap.get(id);
                    contextsMap.put(id, new Pair<Long, Long>(
                        Math.min(count, countTimestampPair.left),
                        // note: keep higher timestamp (for delete marker)
                        Math.max(timestamp, countTimestampPair.right)));
                    continue;
                }

                // remote id: keep lowest count
                if (((Pair<Long, Long>)contextsMap.get(id)).left > count)
                {
                    contextsMap.put(id, new Pair<Long, Long>(count, timestamp));
                }
            }
        }

        List<Map.Entry<FBUtilities.ByteArrayWrapper, Pair<Long, Long>>> contextsList =
            new ArrayList<Map.Entry<FBUtilities.ByteArrayWrapper, Pair<Long, Long>>>(
                    contextsMap.entrySet());
        Collections.sort(
            contextsList,
            new Comparator<Map.Entry<FBUtilities.ByteArrayWrapper, Pair<Long, Long>>>()
            {
                public int compare(
                    Map.Entry<FBUtilities.ByteArrayWrapper, Pair<Long, Long>> e1,
                    Map.Entry<FBUtilities.ByteArrayWrapper, Pair<Long, Long>> e2)
                {
                    // reversed
                    return e2.getValue().right.compareTo(e1.getValue().right);
                }
            });

        int length = contextsList.size();
        byte[] merged = new byte[length * stepLength];
        for (int i = 0; i < length; i++)
        {
            Map.Entry<FBUtilities.ByteArrayWrapper, Pair<Long, Long>> entry = contextsList.get(i);
            writeElementAtStepOffset(
                merged,
                i,
                entry.getKey().data,
                entry.getValue().left.longValue(),
                entry.getValue().right.longValue());
        }
        return merged;
    }

    // return an aggregated count across all node ids
    @Override
    public byte[] aggregateNodes(byte[] context)
    {
        long min = Long.MAX_VALUE;

        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            long count = FBUtilities.byteArrayToLong(context, offset + idLength);
            if (count < min)
            {
                min = count;
            }
        }

        return FBUtilities.toByteArray(min);
    }

    // remove the count for a given node id
    // TODO(asamet) - Implement for min
    @Override
    public byte[] cleanNodeCounts(byte[] context, InetAddress node)
    {
        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            if (FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength) != 0)
                continue;

            // node id found: remove node count
            byte[] truncatedContext = new byte[context.length - stepLength];
            System.arraycopy(context, 0, truncatedContext, 0, offset);
            System.arraycopy(
                context,
                offset + stepLength,
                truncatedContext,
                offset,
                context.length - (offset + stepLength));
            return truncatedContext;
        }

        return context;
    }
}
