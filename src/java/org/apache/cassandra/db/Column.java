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

import java.util.Collection;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.FBUtilities;


/**
 * Column is immutable, which prevents all kinds of confusion in a multithreaded environment.
 * (TODO: look at making SuperColumn immutable too.  This is trickier but is probably doable
 *  with something like PCollections -- http://code.google.com
 */

public class Column implements IColumn
{
    private static Logger logger_ = Logger.getLogger(Column.class);

    public static ColumnSerializer serializer(ColumnType columnType)
    {
        return new ColumnSerializer(columnType);
    }

    private final byte[] name;
    private final byte[] value;
    private final IClock clock;
    private final boolean isMarkedForDelete;

    Column(byte[] name)
    {
        this(name, ArrayUtils.EMPTY_BYTE_ARRAY);
    }

    Column(byte[] name, byte[] value)
    {
        // safe to set to null, only used for filter comparisons
        this(name, value, null);
    }

    public Column(byte[] name, byte[] value, IClock clock)
    {
        this(name, value, clock, false);
    }

    public Column(byte[] name, byte[] value, IClock clock, boolean isDeleted)
    {
        assert name != null;
        assert value != null;
        assert name.length <= IColumn.MAX_NAME_LENGTH;
        this.name = name;
        this.value = value;
        this.clock = clock;
        isMarkedForDelete = isDeleted;
    }

    public byte[] name()
    {
        return name;
    }

    public Column getSubColumn(byte[] columnName)
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public byte[] value()
    {
        return value;
    }

    public Collection<IColumn> getSubColumns()
    {
        throw new UnsupportedOperationException("This operation is unsupported on simple columns.");
    }

    public int getObjectCount()
    {
        return 1;
    }

    public IClock clock()
    {
        return clock;
    }

    public boolean isMarkedForDelete()
    {
        return isMarkedForDelete;
    }

    public IClock getMarkedForDeleteAt()
    {
        if (!isMarkedForDelete())
        {
            throw new IllegalStateException("column is not marked for delete");
        }
        return clock;
    }

    public IClock mostRecentLiveChangeAt()
    {
        return clock;
    }

    public int size()
    {
        /*
         * Size of a column is =
         *   size of a name (UtfPrefix + length of the string)
         * + 1 byte to indicate if the column has been deleted
         * + X bytes depending on IClock size
         * + 4 bytes which basically indicates the size of the byte array
         * + entire byte array.
        */

        /*
           * We store the string as UTF-8 encoded, so when we calculate the length, it
           * should be converted to UTF-8.
           */
        return
            IColumn.UtfPrefix_ + name.length +  // size of name
            DBConstants.boolSize_ +             // 1 byte to indicate if the column has been deleted
            clock.size() +                      // X bytes depending on IClock size
            DBConstants.intSize_ +              // 4 bytes which basically indicates the size of the byte array
            value.length;                       // entire byte array
    }

    /*
     * This returns the size of the column when serialized.
     * @see com.facebook.infrastructure.db.IColumn#serializedSize()
    */
    public int serializedSize()
    {
        return size();
    }

    public void addColumn(IColumn column)
    {
        throw new UnsupportedOperationException("This operation is not supported for simple columns.");
    }

    public IColumn diff(IColumn column)
    {
        // (don't need to worry about disjoint versions, since column created by resolve()
        // which will reconcile any disjoint versions.)
        if (ClockRelationship.GREATER_THAN == column.clock().compare(clock))
        {
            return column;
        }
        return null;
    }

//TODO: TEST
    public IColumn diffForCounter(IColumn column)
    {
        // need to use alternate code path for counters (data encapsulated in clock)
        if (ClockRelationship.GREATER_THAN == ((CounterClock)column.clock()).diff(clock))
        {
            return column;
        }
        return null;
    }

    public void updateDigest(MessageDigest digest)
    {
        digest.update(name);
        digest.update(value);
        DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            clock.serialize(buffer);
            buffer.writeBoolean(isMarkedForDelete);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        digest.update(buffer.getData(), 0, buffer.getLength());
    }

    public int getLocalDeletionTime()
    {
        assert isMarkedForDelete;
        return ByteBuffer.wrap(value).getInt();
    }

    // note that we do not call this simply compareTo since it also makes sense to compare Columns by name
    public ClockRelationship comparePriority(Column o)
    {
        if (isMarkedForDelete)
        {
            // tombstone always wins ties.
            ClockRelationship rel = clock.compare(o.clock());
            switch (rel)
            {
                case LESS_THAN:
                case EQUAL:
                    return ClockRelationship.LESS_THAN;
                default:
                    return rel;
            }
        }
        return clock.compare(o.clock());
    }

    public String getString(AbstractType comparator)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(comparator.getString(name));
        sb.append(":");
        sb.append(isMarkedForDelete());
        sb.append(":");
        sb.append(value.length);
        sb.append("@");
        sb.append(clock.toString());
        return sb.toString();
    }
}

