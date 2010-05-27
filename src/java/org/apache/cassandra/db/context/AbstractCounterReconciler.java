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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.LinkedList;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.db.CounterClock;
import org.apache.cassandra.utils.FBUtilities;

public abstract class AbstractCounterReconciler extends AbstractReconciler
{
    protected abstract AbstractCounterContext getContextManager();

    private static IClock mergeClocks(Column left, Column right)
    {
        List<IClock> clocks = new LinkedList<IClock>();
        clocks.add(right.clock());
        return (IClock)left.clock().getSuperset(clocks);
    }

    public Column reconcile(Column left, Column right)
    {
        // note: called in addColumn(IColumn) to aggregate local node id's counts

        if (left.isMarkedForDelete())
        {
            if (right.isMarkedForDelete())
            {
                // delete + delete: keep later tombstone, higher clock
                int leftLocalDeleteTime  = FBUtilities.byteArrayToInt(left.value());
                int rightLocalDeleteTime = FBUtilities.byteArrayToInt(right.value());

                return new Column(
                    left.name(),
                    leftLocalDeleteTime >= rightLocalDeleteTime ? left.value() : right.value(),
                    mergeClocks(left, right),
                    true);
            }

            // delete + live: use compare() to determine which side to take
            // note: tombstone always wins ties.
            switch (left.clock().compare(right.clock()))
            {
                case EQUAL:
                case GREATER_THAN:
                    return left;
                default: // LESS_THAN
                    return right;
                // note: DISJOINT is not possible
            }
        }
        else if (right.isMarkedForDelete())
        {
            // live + delete: use compare() to determine which side to take
            // note: tombstone always wins ties.
            switch (left.clock().compare(right.clock()))
            {
                case GREATER_THAN:
                    return left;
                default: // EQUAL, LESS_THAN
                    return right;
                // note: DISJOINT is not possible
            }
        }
        else
        {
            // live + live: merge clocks; update value
            IClock clock = mergeClocks(left, right);
            byte[] value = getContextManager().aggregateNodes(((CounterClock)clock).context());

            return new Column(left.name(), value, clock, false);
        }
    }
}
