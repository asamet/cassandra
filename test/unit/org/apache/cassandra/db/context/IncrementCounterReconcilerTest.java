/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.context;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.Util;

import org.junit.Test;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.CounterClock;
import org.apache.cassandra.db.context.IncrementCounterContext;
import org.apache.cassandra.utils.FBUtilities;

public class IncrementCounterReconcilerTest
{   
    private static final IncrementCounterReconciler reconciler = new IncrementCounterReconciler();
    private static final IncrementCounterContext    icc        = new IncrementCounterContext();

    @Test
    public void testReconcileNormal()
    {
        CounterClock leftClock;
        CounterClock rightClock;

        Column left;
        Column right;
        Column reconciled;

        List<IClock> clocks;

        // normal + normal
        leftClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(10L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(27L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32L)
            ), icc);
        left = new Column(
            "x".getBytes(),
            icc.aggregateNodes(leftClock.context()),
            leftClock,
            false
            );

        rightClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(7L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(32L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(6), FBUtilities.toByteArray(2L)
            ), icc);
        right = new Column(
            "x".getBytes(),
            icc.aggregateNodes(rightClock.context()),
            rightClock,
            false
            );

        reconciled = reconciler.reconcile(left, right);

        clocks = new LinkedList<IClock>();
        clocks.add(rightClock);
        assert FBUtilities.compareByteArrays(
            ((CounterClock)leftClock.getSuperset(clocks)).context(),
            ((CounterClock)reconciled.clock()).context()
            ) == 0;

        // local:   27L+9L
        // 1:       128L
        // 5:       32L
        // 6:       2L
        // 9:       62L
        assert FBUtilities.compareByteArrays(
            FBUtilities.toByteArray((27L+9L)+128L+32L+2L+62L),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == false;
    }

    @Test
    public void testReconcileMixed()
    {
        // note: check priority of delete vs. normal
        //   if delete has a later timestamp, treat row as deleted
        //   if normal has a later timestamp, ignore delete
        CounterClock leftClock;
        CounterClock rightClock;

        Column left;
        Column right;
        Column reconciled;

        List<IClock> clocks;

        // normal + delete: normal has higher timestamp
        leftClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(44L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32L)
            ), icc);
        left = new Column(
            "x".getBytes(),
            "live".getBytes(),
            leftClock,
            false
            );

        rightClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(0L)
            ), icc);
        right = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(124).array(), // localDeleteTime secs
            rightClock,
            true
            );

        reconciled = reconciler.reconcile(left, right);

        assert FBUtilities.compareByteArrays(
            ((CounterClock)leftClock).context(),
            ((CounterClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            "live".getBytes(),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == false;
        
        // normal + delete: delete has higher timestamp
        leftClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(4L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32L)
            ), icc);
        left = new Column(
            "x".getBytes(),
            "live".getBytes(),
            leftClock,
            false
            );

        rightClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(100L), 
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(0L)
            ), icc);
        right = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(139).array(), // localDeleteTime secs
            rightClock,
            true
            );

        reconciled = reconciler.reconcile(left, right);

        assert FBUtilities.compareByteArrays(
            ((CounterClock)rightClock).context(),
            ((CounterClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            ByteBuffer.allocate(4).putInt(139).array(),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == true;

        // delete + normal: delete has higher timestamp
        leftClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(0L), FBUtilities.toByteArray(100L)
            ), icc);
        left = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(139).array(), // localDeleteTime secs
            leftClock,
            true
            );

        rightClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(4L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32L)
            ), icc);
        right = new Column(
            "x".getBytes(),
            "live".getBytes(),
            rightClock,
            false
            );

        reconciled = reconciler.reconcile(left, right);

        assert FBUtilities.compareByteArrays(
            ((CounterClock)leftClock).context(),
            ((CounterClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            ByteBuffer.allocate(4).putInt(139).array(),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == true;

        // delete + normal: normal has higher timestamp
        leftClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(1L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(0L)
            ), icc);
        left = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(124).array(), // localDeleteTime secs
            leftClock,
            true
            );

        rightClock = new CounterClock(Util.concatByteArrays(
            FBUtilities.toByteArray(44L),
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(3L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(128L),
            FBUtilities.toByteArray(9), FBUtilities.toByteArray(62L),
            FBUtilities.toByteArray(5), FBUtilities.toByteArray(32L)
            ), icc);
        right = new Column(
            "x".getBytes(),
            "live".getBytes(),
            rightClock,
            false
            );

        reconciled = reconciler.reconcile(left, right);

        assert FBUtilities.compareByteArrays(
            ((CounterClock)rightClock).context(),
            ((CounterClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            "live".getBytes(),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == false;
    }

    @Test
    public void testReconcileDeleted()
    {
        CounterClock leftClock;
        CounterClock rightClock;

        // note: merge clocks + take later localDeleteTime
        Column left;
        Column right;
        Column reconciled;

        List<IClock> clocks;

        // delete + delete
        leftClock = new CounterClock(FBUtilities.toByteArray(3L), icc);
        left = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(139).array(), // localDeleteTime secs
            leftClock,
            true
            );

        rightClock = new CounterClock(FBUtilities.toByteArray(6L), icc);
        right = new Column(
            "x".getBytes(),
            ByteBuffer.allocate(4).putInt(124).array(), // localDeleteTime secs
            rightClock,
            true
            );

        reconciled = reconciler.reconcile(left, right);

        clocks = new LinkedList<IClock>();
        clocks.add(rightClock);
        assert FBUtilities.compareByteArrays(
            ((CounterClock)leftClock.getSuperset(clocks)).context(),
            ((CounterClock)reconciled.clock()).context()
            ) == 0;

        assert FBUtilities.compareByteArrays(
            FBUtilities.toByteArray(139),
            reconciled.value()
            ) == 0;

        assert reconciled.isMarkedForDelete() == true;
    }
}
