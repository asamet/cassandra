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

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.db.context.AbstractReconciler;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.ICompactSerializer2;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.FBUtilities;


public class ColumnFamily implements IColumnContainer
{
    /* The column serializer for this Column Family. Create based on config. */
    private static ColumnFamilySerializer serializer_ = new ColumnFamilySerializer();

    private static Logger logger_ = Logger.getLogger( ColumnFamily.class );

    ColumnType type_;
    AbstractReconciler reconciler;

    public static ColumnFamilySerializer serializer()
    {
        return serializer_;
    }

    public static ColumnFamily create(String tableName, String cfName)
    {
        ColumnType columnType = DatabaseDescriptor.getColumnFamilyType(tableName, cfName);
        AbstractType comparator = DatabaseDescriptor.getComparator(tableName, cfName);
        AbstractType subcolumnComparator = DatabaseDescriptor.getSubComparator(tableName, cfName);
        AbstractReconciler reconciler = DatabaseDescriptor.getReconciler(tableName, cfName);
        return new ColumnFamily(cfName, columnType, comparator, subcolumnComparator, reconciler);
    }

    private String name_;

    private transient ICompactSerializer2<IColumn> columnSerializer_;
    AtomicReference<IClock> markedForDeleteAt;

    AtomicInteger localDeletionTime = new AtomicInteger(Integer.MIN_VALUE);
    private ConcurrentSkipListMap<byte[], IColumn> columns_;

    public ColumnFamily(String cfName, ColumnType columnType, AbstractType comparator, AbstractType subcolumnComparator, AbstractReconciler reconciler)
    {
        name_ = cfName;
        type_ = columnType;
        columnSerializer_ = !columnType.isSuper() ? Column.serializer(type_) : SuperColumn.serializer(subcolumnComparator, type_, reconciler);
        this.reconciler = reconciler;
        markedForDeleteAt = new AtomicReference<IClock>(type_.minClock());
        columns_ = new ConcurrentSkipListMap<byte[], IColumn>(comparator);
    }

    public ColumnFamily cloneMeShallow()
    {
        ColumnFamily cf = new ColumnFamily(name_, type_, getComparator(), getSubComparator(), reconciler);
        cf.markedForDeleteAt = markedForDeleteAt;
        cf.localDeletionTime = localDeletionTime;
        return cf;
    }

    private AbstractType getSubComparator()
    {
        return (columnSerializer_ instanceof SuperColumnSerializer) ? ((SuperColumnSerializer)columnSerializer_).getComparator() : null;
    }

    public ColumnFamily cloneMe()
    {
        ColumnFamily cf = cloneMeShallow();
        cf.columns_ = columns_.clone();
    	return cf;
    }

    public ColumnFamily cloneForCounter()
    {
        if (this.isSuper() && type_.isCounter())
        {
            ConcurrentSkipListMap<byte[], IColumn> newColumns = new ConcurrentSkipListMap<byte[], IColumn>(columns_.comparator());
            ColumnFamily cf = cloneMeShallow();
            for (byte[] colName : columns_.keySet())
            {
                SuperColumn scToCopy = (SuperColumn) columns_.get(colName);
                newColumns.put(colName, scToCopy.cloneMe());
            }
            cf.columns_ = newColumns;
            return cf;
        }

        return cloneMe();
    }

    public String name()
    {
        return name_;
    }

    /*
     *  We need to go through each column
     *  in the column family and resolve it before adding
    */
    public void addAll(ColumnFamily cf)
    {
        for (IColumn column : cf.getSortedColumns())
        {
            addColumn(column);
        }
        delete(cf);
    }

    public ICompactSerializer2<IColumn> getColumnSerializer()
    {
    	return columnSerializer_;
    }

    int getColumnCount()
    {
    	int count = 0;
        if(!isSuper())
        {
            count = columns_.size();
        }
        else
        {
            for(IColumn column: columns_.values())
            {
                count += column.getObjectCount();
            }
        }
    	return count;
    }

    public boolean isSuper()
    {
        return type_.isSuper();
    }

    public void addColumn(QueryPath path, byte[] value, IClock clock)
    {
        addColumn(path, value, clock, false);
    }

    /** In most places the CF must be part of a QueryPath but here it is ignored. */
    public void addColumn(QueryPath path, byte[] value, IClock clock, boolean deleted)
	{
        assert path.columnName != null : path;
		IColumn column;
        if (path.superColumnName == null)
        {
            column = new Column(path.columnName, value, clock, deleted);
        }
        else
        {
            assert isSuper();
            column = new SuperColumn(path.superColumnName, getSubComparator(), type_, reconciler);
            column.addColumn(new Column(path.columnName, value, clock, deleted)); // checks subcolumn name
        }
		addColumn(column);
    }

    public void clear()
    {
    	columns_.clear();
    }

    /*
     * If we find an old column that has the same name
     * the ask it to resolve itself else add the new column .
    */
    public void addColumn(IColumn column)
    {
        if (type_.isCounter())
        {
            addColumnForCounter(column);
            return;
        }

        byte[] name = column.name();
        IColumn oldColumn = columns_.putIfAbsent(name, column);
        if (oldColumn != null)
        {
            if (oldColumn instanceof SuperColumn)
            {
                ((SuperColumn) oldColumn).putColumn(column);
            }
            else
            {
//TODO: REFACTOR? (pull this into Clock sub-classes)
                ClockRelationship rel = ((Column) oldColumn).comparePriority((Column)column);
                while (ClockRelationship.GREATER_THAN != rel)
                {
                    if (ClockRelationship.DISJOINT == rel)
                    {
                        column = reconciler.reconcile((Column)oldColumn, (Column)column);
                    }
                    if (columns_.replace(name, oldColumn, column))
                        break;
                    oldColumn = columns_.get(name);
                    rel = ((Column) oldColumn).comparePriority((Column)column);
                }
            }
        }
    }

    private void addColumnForCounter(IColumn newColumn)
    {
        byte[] name = newColumn.name();
        IColumn oldColumn = columns_.putIfAbsent(name, newColumn);
        // if not present already, then return
        if (oldColumn == null)
        {
            return;
        }

        // SuperColumn
        if (oldColumn instanceof SuperColumn)
        {
            ((SuperColumn)oldColumn).putColumn(newColumn);
            return;
        }

        // calculate reconciled col from old (existing) col and new col
        IColumn reconciledColumn = reconciler.reconcile((Column)oldColumn, (Column)newColumn);
        while (!columns_.replace(name, oldColumn, reconciledColumn))
        {
            // if unable to replace, then get updated old (existing) col
            oldColumn = columns_.get(name);
            // re-calculate reconciled col from updated old col and original new col
            reconciledColumn = reconciler.reconcile((Column)oldColumn, (Column)newColumn);
            // try to re-update value, again
        }
    }

    public void cleanForCounter()
    {
        cleanForCounter(FBUtilities.getLocalAddress());
    }

//TODO: REFACTOR? (modify: 1) where CF is sanitized to be on read side; 2) how CF is sanitized)
//TODO: TEST (clean remote replica counts for read repair)
    public void cleanForCounter(InetAddress node)
    {
//TODO: MODIFY: support SuperColumn-type CF
        for (IColumn column : getSortedColumns())
        {
            if (column instanceof SuperColumn)
            {
                for (IColumn subColumn : ((SuperColumn)column).getSubColumns())
                {
                    CounterClock subClock = (CounterClock)subColumn.clock();
                    subClock.cleanNodeCounts(node);
                    if (0 == subClock.context().length)
                    {
                        ((SuperColumn)column).remove(subColumn.name()); 
                    }
                }
                continue;
            }
            CounterClock clock = (CounterClock)column.clock();
            clock.cleanNodeCounts(node);
            if (0 == clock.context().length)
            {
                remove(column.name());
            }
        }
    }

    public IColumn getColumn(byte[] name)
    {
        return columns_.get(name);
    }

    public SortedSet<byte[]> getColumnNames()
    {
        return columns_.keySet();
    }

    public Collection<IColumn> getSortedColumns()
    {
        return columns_.values();
    }

    public Map<byte[], IColumn> getColumnsMap()
    {
        return columns_;
    }

    public void remove(byte[] columnName)
    {
    	columns_.remove(columnName);
    }

    @Deprecated // TODO this is a hack to set initial value outside constructor
    public void delete(int localtime, IClock clock)
    {
        localDeletionTime.set(localtime);
        markedForDeleteAt.set(clock);
    }

    public void delete(ColumnFamily cf2)
    {
        FBUtilities.atomicSetMax(localDeletionTime, cf2.getLocalDeletionTime()); // do this first so we won't have a column that's "deleted" but has no local deletion time
        FBUtilities.atomicSetMax(markedForDeleteAt, cf2.getMarkedForDeleteAt());
    }

    public boolean isMarkedForDelete()
    {
        IClock _markedForDeleteAt = markedForDeleteAt.get();
        //XXX: will never be DISJOINT compared to MIN_VALUE
        return _markedForDeleteAt.compare(type_.minClock()) == ClockRelationship.GREATER_THAN;
    }

    /*
     * This function will calculate the difference between 2 column families.
     * The external input is assumed to be a superset of internal.
     */
    public ColumnFamily diff(ColumnFamily cfComposite)
    {
    	ColumnFamily cfDiff = new ColumnFamily(cfComposite.name(), cfComposite.type_, getComparator(), getSubComparator(), cfComposite.reconciler);
//TODO: TEST
//        if (cfComposite.getMarkedForDeleteAt() > getMarkedForDeleteAt())
        // (don't need to worry about disjoint versions, since cfComposite created by resolve()
        // which will reconcile disjoint versions.)
        ClockRelationship rel = cfComposite.getMarkedForDeleteAt().compare(getMarkedForDeleteAt());
        if (ClockRelationship.GREATER_THAN == rel)
        {
            cfDiff.delete(cfComposite.getLocalDeletionTime(), cfComposite.getMarkedForDeleteAt());
        }

        // (don't need to worry about cfNew containing IColumns that are shadowed by
        // the delete tombstone, since cfNew was generated by CF.resolve, which
        // takes care of those for us.)
        Map<byte[], IColumn> columns = cfComposite.getColumnsMap();
        Set<byte[]> cNames = columns.keySet();
        for (byte[] cName : cNames)
        {
            IColumn columnInternal = columns_.get(cName);
            IColumn columnExternal = columns.get(cName);
            if (columnInternal == null)
            {
                cfDiff.addColumn(columnExternal);
            }
            else
            {
//TODO: TEST
//                IColumn columnDiff = columnInternal.diff(columnExternal);
                IColumn columnDiff = type_.isCounter() ? columnInternal.diffForCounter(columnExternal) : columnInternal.diff(columnExternal);
                if (columnDiff != null)
                { 
                    cfDiff.addColumn(columnDiff);
                }
            }
        }

        if (!cfDiff.getColumnsMap().isEmpty() || cfDiff.isMarkedForDelete())
        	  return cfDiff;
        else
            return null;
    }

    public AbstractType getComparator()
    {
        return (AbstractType)columns_.comparator();
    }

    public ColumnType getColumnType()
    {
        return type_;
    }

    int size()
    {
        int size = 0;
        for (IColumn column : columns_.values())
        {
            size += column.size();
        }
        return size;
    }

    public int hashCode()
    {
        return name().hashCode();
    }

    public boolean equals(Object o)
    {
        if ( !(o instanceof ColumnFamily) )
            return false;
        ColumnFamily cf = (ColumnFamily)o;
        return name().equals(cf.name());
    }

    public String toString()
    {
    	StringBuilder sb = new StringBuilder();
        sb.append("ColumnFamily(");
    	sb.append(name_);

        if (isMarkedForDelete()) {
            sb.append(" -delete at " + getMarkedForDeleteAt().toString() + "-");
        }

    	sb.append(" [");
        sb.append(getComparator().getColumnsString(getSortedColumns()));
        sb.append("])");

    	return sb.toString();
    }

    public static byte[] digest(ColumnFamily cf)
    {
        MessageDigest digest;
        try
        {
            digest = MessageDigest.getInstance("MD5");
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new AssertionError(e);
        }
        if (cf != null)
            cf.updateDigest(digest);

        return digest.digest();
    }

    public void updateDigest(MessageDigest digest)
    {
        for (IColumn column : columns_.values())
        {
            column.updateDigest(digest);
        }
    }

    public IClock getMarkedForDeleteAt()
    {
        return markedForDeleteAt.get();
    }

    public int getLocalDeletionTime()
    {
        return localDeletionTime.get();
    }

    public ColumnType type()
    {
        return type_;
    }

    String getComparatorName()
    {
        return getComparator().getClass().getCanonicalName();
    }

    String getSubComparatorName()
    {
        AbstractType subcolumnComparator = getSubComparator();
        return subcolumnComparator == null ? "" : subcolumnComparator.getClass().getCanonicalName();
    }

    public static AbstractType getComparatorFor(String table, String columnFamilyName, byte[] superColumnName)
    {
        return superColumnName == null
               ? DatabaseDescriptor.getComparator(table, columnFamilyName)
               : DatabaseDescriptor.getSubComparator(table, columnFamilyName);
    }

    public static ColumnFamily diff(ColumnFamily cf1, ColumnFamily cf2)
    {
        if (cf1 == null)
            return cf2;
        return cf1.diff(cf2);
    }

    public static ColumnFamily resolve(ColumnFamily cf1, ColumnFamily cf2)
    {
        if (cf1 == null)
            return cf2;
        cf1.resolve(cf2);
        return cf1;
    }

    public void resolve(ColumnFamily cf)
    {
        // Row _does_ allow null CF objects :(  seems a necessary evil for efficiency
        if (cf == null)
            return;
        addAll(cf);
    }
}
