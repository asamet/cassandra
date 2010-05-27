/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */
package org.apache.cassandra.db.context;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.Util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

/**
 * Note: these tests assume IPv4 (4 bytes) is used for id.
 *       if IPv6 (16 bytes) is used, tests will fail (but the code will work).
 *       however, it might be pragmatic to modify the code to just use
 *       the IPv4 portion of the IPv6 address-space.
 */
public class IncrementCounterContextTest
{
    private static final IncrementCounterContext icc = new IncrementCounterContext();

    private static final InetAddress idAddress;
    private static final byte[] id;
    private static final int idLength;
    private static final int countLength;
    private static final int timestampLength;
    private static final int stepLength;
    private static final int defaultEntries;

    static
    {
        idAddress       = FBUtilities.getLocalAddress();
        id              = idAddress.getAddress();
        idLength        = 4; // size of int
        countLength     = 8; // size of long
        timestampLength = 8; // size of long
        stepLength      = idLength + countLength + timestampLength;

        defaultEntries  = 10;
    }

    @Test
    public void testCreate()
    {
        byte[] context = icc.create();
        assert context.length == 0;
    }

    @Test
    public void testUpdateEmpty()
    {
        byte[] context;

        // update
        context = icc.create();
        context = icc.update(context, idAddress);

        assert context.length == stepLength;
        assert FBUtilities.compareByteSubArrays(
                context, 0, id, 0, idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context, idLength,
                FBUtilities.toByteArray(1L), 0,
                countLength) == 0;

        // update w/ delta
        context = icc.create();
        context = icc.update(context, idAddress, 3L);

        assert context.length == stepLength;
        assert FBUtilities.compareByteSubArrays(
                context, 0, id, 0, idLength) == 0;
        assert FBUtilities.compareByteSubArrays(
                context, idLength,
                FBUtilities.toByteArray(3L), 0,
                countLength) == 0;
    }

    @Test
    public void testUpdatePresent()
    {
        byte[] context;

        // update
        context = icc.create();
        context = icc.update(context, idAddress);

        context = icc.update(context, idAddress);
        assert context.length == stepLength;
        assert 2L == FBUtilities.byteArrayToLong(context, idLength);

        // update w/ delta
        context = icc.update(context, idAddress, 25L);
        assert context.length == stepLength;
        assert 27L == FBUtilities.byteArrayToLong(context, idLength);
    }

    @Test
    public void testUpdatePresentReorder() throws UnknownHostException
    {
        byte[] context;

        context = new byte[stepLength * defaultEntries];

        for (int i = 0; i < defaultEntries - 1; i++)
        {
            icc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L,
                System.currentTimeMillis());
        }
        icc.writeElementAtStepOffset(
            context,
            (defaultEntries - 1),
            id,
            1L,
            System.currentTimeMillis());

        context = icc.update(context, idAddress, 10L);

        assert context.length == stepLength * defaultEntries;
        assert 11L == FBUtilities.byteArrayToLong(context, idLength);
        for (int i = 1; i < defaultEntries; i++)
        {
            int offset = i * stepLength;
            assert i-1 == FBUtilities.byteArrayToInt(context,  offset);
            assert  1L == FBUtilities.byteArrayToLong(context, offset + idLength);
        }
    }

    @Test
    public void testUpdateNotPresent()
    {
        byte[] context = new byte[stepLength * 2];

        for (int i = 0; i < 2; i++)
        {
            icc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L,
                System.currentTimeMillis());
        }

        context = icc.update(context, idAddress, 328L);

        assert context.length == stepLength * 3;
        assert 328L == FBUtilities.byteArrayToLong(context, idLength);
        for (int i = 1; i < 3; i++)
        {
            int offset = i * stepLength;
            assert i-1 == FBUtilities.byteArrayToInt(context,  offset);
            assert  1L == FBUtilities.byteArrayToLong(context, offset + idLength);
        }
    }

    @Test
    public void testSwapElement()
    {
        byte[] context = new byte[stepLength * 3];

        for (int i = 0; i < 3; i++)
        {
            icc.writeElementAtStepOffset(
                context,
                i,
                FBUtilities.toByteArray(i),
                1L,
                System.currentTimeMillis());
        }
        icc.swapElement(context, 0, 2*stepLength);

        assert 2 == FBUtilities.byteArrayToInt(context, 0);
        assert 0 == FBUtilities.byteArrayToInt(context, 2*stepLength);

        icc.swapElement(context, 0, 1*stepLength);

        assert 1 == FBUtilities.byteArrayToInt(context, 0);
        assert 2 == FBUtilities.byteArrayToInt(context, 1*stepLength);
    }

    @Test
    public void testPartitionElements()
    {
        byte[] context = new byte[stepLength * 10];

        icc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L, System.currentTimeMillis());

        icc.partitionElements(context,
                0*stepLength,  // left
                9*stepLength,  // right (inclusive)
                2*stepLength); // pivot

        assert 5 == FBUtilities.byteArrayToInt(context, 0*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(context, 2*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(context, 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(context, 4*stepLength);
        assert 1 == FBUtilities.byteArrayToInt(context, 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(context, 6*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(context, 7*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(context, 8*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(context, 9*stepLength);
    }

    @Test
    public void testSortElementsById()
    {
        byte[] context = new byte[stepLength * 10];

        icc.writeElementAtStepOffset(context, 0, FBUtilities.toByteArray(5), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 1, FBUtilities.toByteArray(3), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 2, FBUtilities.toByteArray(6), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 3, FBUtilities.toByteArray(7), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 4, FBUtilities.toByteArray(8), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 5, FBUtilities.toByteArray(9), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 6, FBUtilities.toByteArray(2), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 7, FBUtilities.toByteArray(4), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 8, FBUtilities.toByteArray(1), 1L, System.currentTimeMillis());
        icc.writeElementAtStepOffset(context, 9, FBUtilities.toByteArray(3), 1L, System.currentTimeMillis());
        
        byte[] sorted = icc.sortElementsById(context);

        assert 1 == FBUtilities.byteArrayToInt(sorted, 0*stepLength);
        assert 2 == FBUtilities.byteArrayToInt(sorted, 1*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, 2*stepLength);
        assert 3 == FBUtilities.byteArrayToInt(sorted, 3*stepLength);
        assert 4 == FBUtilities.byteArrayToInt(sorted, 4*stepLength);
        assert 5 == FBUtilities.byteArrayToInt(sorted, 5*stepLength);
        assert 6 == FBUtilities.byteArrayToInt(sorted, 6*stepLength);
        assert 7 == FBUtilities.byteArrayToInt(sorted, 7*stepLength);
        assert 8 == FBUtilities.byteArrayToInt(sorted, 8*stepLength);
        assert 9 == FBUtilities.byteArrayToInt(sorted, 9*stepLength);
    }

    @Test
    public void testCompare()
    {
        byte[] left;
        byte[] right;

        // equality:
        //   left:  no local timestamp
        //   right: no local timestamp
        left  = new byte[0];
        right = new byte[0];

        assert IContext.ContextRelationship.EQUAL ==
            icc.compare(left, right);

        // equality:
        //   left, right: local timestamps equal
        left = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(5L)
            );
        right = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            );

        assert IContext.ContextRelationship.EQUAL ==
            icc.compare(left, right);

        // greater than:
        //   left:  local timestamp
        //   right: no local timestamp
        left = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(5L)
            );
        right = Util.concatByteArrays(
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            );

        assert IContext.ContextRelationship.GREATER_THAN ==
            icc.compare(left, right);

        // greater than:
        //   left's local timestamp > right's local timestamp
        left = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(11L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(5L)
            );
        right = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            );

        assert IContext.ContextRelationship.GREATER_THAN ==
            icc.compare(left, right);

        // less than:
        //   left:  no local timestamp
        //   right: local timestamp
        left = Util.concatByteArrays(
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(5L)
            );
        right = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            );

        assert IContext.ContextRelationship.LESS_THAN ==
            icc.compare(left, right);

        // less than:
        //   left's local timestamp < right's local timestamp
        left = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(32L), FBUtilities.toByteArray(9L),
            FBUtilities.toByteArray(1), FBUtilities.toByteArray(4L), FBUtilities.toByteArray(7L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(5L)
            );
        right = Util.concatByteArrays(
            FBUtilities.getLocalAddress().getAddress(), FBUtilities.toByteArray(2L), FBUtilities.toByteArray(122L),
            FBUtilities.toByteArray(3), FBUtilities.toByteArray(9L), FBUtilities.toByteArray(4L),
            FBUtilities.toByteArray(2), FBUtilities.toByteArray(1L), FBUtilities.toByteArray(1L)
            );

        assert IContext.ContextRelationship.LESS_THAN ==
            icc.compare(left, right);
    }

    @Test
    public void testDiff()
    {
        byte[] left = new byte[3 * stepLength];
        byte[] right;

        // equality: equal nodes, all counts same
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);
        right = ArrayUtils.clone(left);

        assert IContext.ContextRelationship.EQUAL ==
            icc.diff(left, right);

        // greater than: left has superset of nodes (counts equal)
        left = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3),  3L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6),  2L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9),  1L, 1L);
        icc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 0L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L, 1L);

        assert IContext.ContextRelationship.GREATER_THAN ==
            icc.diff(left, right);
        
        // less than: left has subset of nodes (counts equal)
        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);

        right = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  3L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  2L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  1L, 1L);
        icc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 0L, 1L);

        assert IContext.ContextRelationship.LESS_THAN ==
            icc.diff(left, right);

        // greater than: equal nodes, but left has higher counts
        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L, 1L);

        assert IContext.ContextRelationship.GREATER_THAN ==
            icc.diff(left, right);

        // less than: equal nodes, but right has higher counts
        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 3L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 3L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 3L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 3L, 1L);

        assert IContext.ContextRelationship.LESS_THAN ==
            icc.diff(left, right);

        // disjoint: right and left have disjoint node sets
        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 1L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);

        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(4), 1L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(2),  1L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  1L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(12), 1L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);

        // disjoint: equal nodes, but right and left have higher counts in differing nodes
        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 1L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 1L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);

        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 2L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 1L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 1L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);

        // disjoint: left has more nodes, but lower counts
        left = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3),  2L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6),  3L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9),  1L, 1L);
        icc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(12), 1L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 9L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 5L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);
        
        // disjoint: left has less nodes, but higher counts
        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 3L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L, 1L);

        right = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  4L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  3L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  2L, 1L);
        icc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 1L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);

        // disjoint: mixed nodes and counts
        left = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(9), 2L, 1L);

        right = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3),  4L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6),  3L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9),  2L, 1L);
        icc.writeElementAtStepOffset(right, 3, FBUtilities.toByteArray(12), 1L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);

        left = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(left, 0, FBUtilities.toByteArray(3), 5L, 1L);
        icc.writeElementAtStepOffset(left, 1, FBUtilities.toByteArray(6), 2L, 1L);
        icc.writeElementAtStepOffset(left, 2, FBUtilities.toByteArray(7), 2L, 1L);
        icc.writeElementAtStepOffset(left, 3, FBUtilities.toByteArray(9), 2L, 1L);

        right = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(right, 0, FBUtilities.toByteArray(3), 4L, 1L);
        icc.writeElementAtStepOffset(right, 1, FBUtilities.toByteArray(6), 3L, 1L);
        icc.writeElementAtStepOffset(right, 2, FBUtilities.toByteArray(9), 2L, 1L);

        assert IContext.ContextRelationship.DISJOINT ==
            icc.diff(left, right);
    }

    @Test
    public void testMerge()
    {
        // note: local counts aggregated; remote counts are reconciled (i.e. take max)

        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L, 1L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L, 2L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L, 3L);
        icc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L,
            3L);
        contexts.add(bytes);

        bytes = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(5), 5L, 4L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L, 5L);
        icc.writeElementAtStepOffset(
            bytes,
            0,
            FBUtilities.getLocalAddress().getAddress(),
            9L,
            6L);
        contexts.add(bytes);

        byte[] merged = icc.merge(contexts);

        // local node id's counts are aggregated
        assert 0  == FBUtilities.compareByteSubArrays(
            FBUtilities.getLocalAddress().getAddress(),
            0,
            merged, 0*stepLength,
            4);
        assert 12L == FBUtilities.byteArrayToLong(merged, 0*stepLength + idLength);
        assert  6L == FBUtilities.byteArrayToLong(merged, 0*stepLength + idLength + countLength);

        // remote node id counts are reconciled (i.e. take max)
        assert 4  == FBUtilities.byteArrayToInt(merged, 1*stepLength);
        assert 4L == FBUtilities.byteArrayToLong(merged, 1*stepLength + idLength);
        assert 5L == FBUtilities.byteArrayToLong(merged, 1*stepLength + idLength + countLength);

        assert 5  == FBUtilities.byteArrayToInt(merged, 2*stepLength);
        assert 5L == FBUtilities.byteArrayToLong(merged, 2*stepLength + idLength);
        assert 4L == FBUtilities.byteArrayToLong(merged, 2*stepLength + idLength + countLength);

        assert 2  == FBUtilities.byteArrayToInt(merged, 3*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, 3*stepLength + idLength);
        assert 2L == FBUtilities.byteArrayToLong(merged, 3*stepLength + idLength + countLength);

        assert 1  == FBUtilities.byteArrayToInt(merged, 4*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, 4*stepLength + idLength);
        assert 1L == FBUtilities.byteArrayToLong(merged, 4*stepLength + idLength + countLength);
    }

    @Test
    public void testAggregateNodes()
    {
        List<byte[]> contexts = new ArrayList<byte[]>();

        byte[] bytes = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L, 1L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L, 2L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L, 3L);
        icc.writeElementAtStepOffset(
            bytes,
            3,
            FBUtilities.getLocalAddress().getAddress(),
            3L,
            3L);
        contexts.add(bytes);

        bytes = new byte[3 * stepLength];
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(5), 5L, 4L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(4), 4L, 5L);
        icc.writeElementAtStepOffset(
            bytes,
            0,
            FBUtilities.getLocalAddress().getAddress(),
            9L,
            6L);
        contexts.add(bytes);

        byte[] merged = icc.merge(contexts);

        // 127.0.0.1: 12 (3+9)
        // 0.0.0.1:    1
        // 0.0.0.2:    2
        // 0.0.0.4:    4
        // 0.0.0.5:    5

        assert 24L == FBUtilities.byteArrayToLong(icc.aggregateNodes(merged));
    }

    @Test
    public void testCleanNodeCounts() throws UnknownHostException
    {
        byte[] bytes = new byte[4 * stepLength];
        icc.writeElementAtStepOffset(bytes, 0, FBUtilities.toByteArray(1), 1L, 1L);
        icc.writeElementAtStepOffset(bytes, 1, FBUtilities.toByteArray(2), 2L, 2L);
        icc.writeElementAtStepOffset(bytes, 2, FBUtilities.toByteArray(4), 3L, 3L);
        icc.writeElementAtStepOffset(bytes, 3, FBUtilities.toByteArray(8), 4L, 4L);

        assert 4  == FBUtilities.byteArrayToInt(bytes,  2*stepLength);
        assert 3L == FBUtilities.byteArrayToLong(bytes, 2*stepLength + idLength);
        assert 3L == FBUtilities.byteArrayToLong(bytes, 2*stepLength + idLength + countLength);

        bytes = icc.cleanNodeCounts(bytes, InetAddress.getByAddress(FBUtilities.toByteArray(4)));

        // node: 0.0.0.4 should be removed
        assert 3 * stepLength == bytes.length;

        // other nodes should be unaffected
        assert 1  == FBUtilities.byteArrayToInt(bytes,  0*stepLength);
        assert 1L == FBUtilities.byteArrayToLong(bytes, 0*stepLength + idLength);
        assert 1L == FBUtilities.byteArrayToLong(bytes, 0*stepLength + idLength + countLength);

        assert 2  == FBUtilities.byteArrayToInt(bytes,  1*stepLength);
        assert 2L == FBUtilities.byteArrayToLong(bytes, 1*stepLength + idLength);
        assert 2L == FBUtilities.byteArrayToLong(bytes, 1*stepLength + idLength + countLength);

        assert 8  == FBUtilities.byteArrayToInt(bytes,  2*stepLength);
        assert 4L == FBUtilities.byteArrayToLong(bytes, 2*stepLength + idLength);
        assert 4L == FBUtilities.byteArrayToLong(bytes, 2*stepLength + idLength + countLength);
    }
}
