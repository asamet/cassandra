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

package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.context.IContext.ContextRelationship;
import org.apache.cassandra.db.context.AbstractCounterContext;
import org.apache.cassandra.db.context.IncrementCounterContext;
import org.apache.cassandra.db.context.MaxCounterContext;
import org.apache.cassandra.db.context.MinCounterContext;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.utils.FBUtilities;

public class CounterClock implements IClock
{
    public static CounterClock MIN_INCR_CLOCK =
        new CounterClock(FBUtilities.toByteArray(Long.MIN_VALUE), IncrementCounterContext.instance());
    public static CounterClock MIN_MIN_CLOCK =
        new CounterClock(FBUtilities.toByteArray(Long.MIN_VALUE), MinCounterContext.instance());
    public static CounterClock MIN_MAX_CLOCK =
        new CounterClock(FBUtilities.toByteArray(Long.MIN_VALUE), MaxCounterContext.instance());

    public static ICompactSerializer2<IClock> INCR_SERIALIZER =
        new CounterClockSerializer(IncrementCounterContext.instance());
    public static ICompactSerializer2<IClock> MIN_SERIALIZER = 
        new CounterClockSerializer(MinCounterContext.instance());
    public static ICompactSerializer2<IClock> MAX_SERIALIZER = 
        new CounterClockSerializer(MaxCounterContext.instance());

    public byte[] context;
    public AbstractCounterContext contextManager;

    public CounterClock(byte[] context, AbstractCounterContext contextManager)
    {
        this.context = context;
        this.contextManager = contextManager; 
    }

    public byte[] context()
    {
        return context;
    }

    public void update(InetAddress node, long delta)
    {
        context = contextManager.update(context, node, delta);
    }

    public ClockRelationship compare(IClock other)
    {
        assert other instanceof CounterClock : "Wrong class type.";

        ContextRelationship rel = contextManager.compare(context, ((CounterClock)other).context());
        switch (rel)
        {
            case EQUAL:
                return ClockRelationship.EQUAL;
            case GREATER_THAN:
                return ClockRelationship.GREATER_THAN;
            case LESS_THAN:
                return ClockRelationship.LESS_THAN;
            default: // DISJOINT
                return ClockRelationship.DISJOINT;
        }
    }

    public ClockRelationship diff(IClock other)
    {
        assert other instanceof CounterClock : "Wrong class type.";

        ContextRelationship rel = contextManager.diff(context, ((CounterClock)other).context());
        switch (rel)
        {
            case EQUAL:
                return ClockRelationship.EQUAL;
            case GREATER_THAN:
                return ClockRelationship.GREATER_THAN;
            case LESS_THAN:
                return ClockRelationship.LESS_THAN;
            default: // DISJOINT
                return ClockRelationship.DISJOINT;
        }
    }

    public IClock getSuperset(List<IClock> otherClocks)
    {
        List<byte[]> contexts = new LinkedList<byte[]>();

        contexts.add(context);
        for (IClock clock : otherClocks)
        {
            assert clock instanceof CounterClock : "Wrong class type.";
            contexts.add(((CounterClock)clock).context);
        }

        return new CounterClock(contextManager.merge(contexts), this.contextManager);
    }

    public int size()
    {
        return DBConstants.intSize_ + context.length;
    }

    public void serialize(DataOutput out) throws IOException
    {
        // TODO(asamet) Make this cleaner, don't use an instance of
        if (contextManager instanceof IncrementCounterContext)
        {
            INCR_SERIALIZER.serialize(this, out);
        }
        else if (contextManager instanceof MinCounterContext)
        {
            MIN_SERIALIZER.serialize(this, out);
        }
        else if (contextManager instanceof MaxCounterContext)
        {
            MAX_SERIALIZER.serialize(this, out);
        }
        else
        {
            assert false;
        }
    }

    public String toString()
    {
        return contextManager.toString(context);
    }

    public void cleanNodeCounts(InetAddress node)
    {
        context = contextManager.cleanNodeCounts(context, node);
    }
}

class CounterClockSerializer implements ICompactSerializer2<IClock> 
{
    private AbstractCounterContext contextManager;

    public CounterClockSerializer(AbstractCounterContext contextManager)
    {
        this.contextManager = contextManager;
    }

    public void serialize(IClock c, DataOutput out) throws IOException
    {
        FBUtilities.writeByteArray(((CounterClock)c).context(), out);
    }

    public IClock deserialize(DataInput in) throws IOException
    {
        int length = in.readInt();
        if ( length < 0 )
        {
            throw new IOException("Corrupt (negative) value length encountered");
        }
        byte[] context = new byte[length];
        if ( length > 0 )
        {
            in.readFully(context);
        }
        return new CounterClock(context, contextManager);
    }
}
