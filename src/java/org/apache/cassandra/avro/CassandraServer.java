package org.apache.cassandra.avro;
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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.AvroRemoteException;
import org.apache.avro.util.Utf8;
import org.apache.cassandra.db.ColumnFamily;
//TODO: TEST
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
//TODO: TEST
import org.apache.cassandra.db.TimestampClock;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.log4j.Logger;
import static org.apache.cassandra.avro.RecordFactory.*;
import static org.apache.cassandra.avro.ErrorFactory.*;

public class CassandraServer implements Cassandra {
    private static Logger logger = Logger.getLogger(CassandraServer.class);

    private final static GenericArray<Column> EMPTY_SUBCOLUMNS = new GenericData.Array<Column>(0, Schema.parse("{\"type\":\"array\",\"items\":" + Column.SCHEMA$ + "}"));
    private final static Utf8 API_VERSION = new Utf8("0.0.0");

    @Override
    public ColumnOrSuperColumn get(Utf8 keyspace, Utf8 key, ColumnPath columnPath, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, NotFoundException, UnavailableException, TimedOutException {
        if (logger.isDebugEnabled())
            logger.debug("get");
        
        ColumnOrSuperColumn column = multigetInternal(keyspace.toString(), Arrays.asList(key.toString()), columnPath, consistencyLevel).get(key.toString());
        
        if ((column.column == null) && (column.super_column == null))
        {
            throw newNotFoundException("Path not found");
        }
        return column;
    }

    private Map<String, ColumnOrSuperColumn> multigetInternal(String keyspace, List<String> keys, ColumnPath cp, ConsistencyLevel level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        AvroValidation.validateColumnPath(keyspace, cp);
        
        // FIXME: This is repetitive.
        byte[] column, super_column;
        column = cp.column == null ? null : cp.column.array();
        super_column = cp.super_column == null ? null : cp.super_column.array();
        
        QueryPath path = new QueryPath(cp.column_family.toString(), column == null ? null : super_column);
        List<byte[]> nameAsList = Arrays.asList(column == null ? super_column : column);
        List<ReadCommand> commands = new ArrayList<ReadCommand>();
        for (String key: keys)
        {
            AvroValidation.validateKey(key);
            commands.add(new SliceByNamesReadCommand(keyspace, key, path, nameAsList));
        }
        
        Map<String, ColumnOrSuperColumn> columnFamiliesMap = new HashMap<String, ColumnOrSuperColumn>();
        Map<String, Collection<IColumn>> columnsMap = multigetColumns(commands, level);
        
        for (ReadCommand command: commands)
        {
            ColumnOrSuperColumn columnorsupercolumn;

            Collection<IColumn> columns = columnsMap.get(command.key);
            if (columns == null)
            {
               columnorsupercolumn = new ColumnOrSuperColumn();
            }
            else
            {
                assert columns.size() == 1;
                IColumn col = columns.iterator().next();


                if (col.isMarkedForDelete())
                {
                    columnorsupercolumn = new ColumnOrSuperColumn();
                }
                else
                {
                    columnorsupercolumn = col instanceof org.apache.cassandra.db.Column
//TODO: MODIFY: extend Avro spec to support Clock
//                                          ? newColumnOrSuperColumn(newColumn(col.name(), col.value(), col.timestamp()))
                                          ? newColumnOrSuperColumn(newColumn(col.name(), col.value(), ((TimestampClock)col.clock()).timestamp()))
                                          : newColumnOrSuperColumn(newSuperColumn(col.name(), avronateSubColumns(col.getSubColumns())));
                }

            }
            columnFamiliesMap.put(command.key, columnorsupercolumn);
        }

        return columnFamiliesMap;
    }
    
    private Map<String, Collection<IColumn>> multigetColumns(List<ReadCommand> commands, ConsistencyLevel level)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        Map<String, ColumnFamily> cfamilies = readColumnFamily(commands, level);
        Map<String, Collection<IColumn>> columnFamiliesMap = new HashMap<String, Collection<IColumn>>();
        
        for (ReadCommand command: commands)
        {
            ColumnFamily cfamily = cfamilies.get(command.key);
            if (cfamily == null)
                continue;

            Collection<IColumn> columns = null;
            if (command.queryPath.superColumnName != null)
            {
                IColumn column = cfamily.getColumn(command.queryPath.superColumnName);
                if (column != null)
                {
                    columns = column.getSubColumns();
                }
            }
            else
            {
                columns = cfamily.getSortedColumns();
            }

            if (columns != null && columns.size() != 0)
            {
                columnFamiliesMap.put(command.key, columns);
            }
        }
        
        return columnFamiliesMap;
    }
    
    protected Map<String, ColumnFamily> readColumnFamily(List<ReadCommand> commands, ConsistencyLevel consistency)
    throws InvalidRequestException, UnavailableException, TimedOutException
    {
        // TODO - Support multiple column families per row, right now row only contains 1 column family
        Map<String, ColumnFamily> columnFamilyKeyMap = new HashMap<String,ColumnFamily>();
        
        if (consistency == ConsistencyLevel.ZERO)
            throw newInvalidRequestException("Consistency level zero may not be applied to read operations");
        
        if (consistency == ConsistencyLevel.ALL)
            throw newInvalidRequestException("Consistency level all is not yet supported on read operations");
        
        List<Row> rows;
        try
        {
            rows = StorageProxy.readProtocol(commands, thriftConsistencyLevel(consistency));
        }
        catch (TimeoutException e) 
        {
            throw new TimedOutException();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        // FIXME: This suckage brought to you by StorageService and StorageProxy
        // which throw Thrift exceptions directly.
        catch (org.apache.cassandra.thrift.UnavailableException e)
        {
            throw new UnavailableException();
        }

        for (Row row: rows)
        {
            columnFamilyKeyMap.put(row.key, row.cf);
        }
        
        return columnFamilyKeyMap;
    }
    
    // Don't playa hate, avronate.
    public GenericArray<Column> avronateSubColumns(Collection<IColumn> columns)
    {
        if (columns == null || columns.isEmpty())
            return EMPTY_SUBCOLUMNS;
        
        GenericData.Array<Column> avroColumns = new GenericData.Array<Column>(columns.size(), Column.SCHEMA$);

        for (IColumn column : columns)
        {
            if (column.isMarkedForDelete())
                continue;
            
//TODO: MODIFY: create avro Clock
//            Column avroColumn = newColumn(column.name(), column.value(), column.timestamp());
            Column avroColumn = newColumn(column.name(), column.value(), ((TimestampClock)column.clock()).timestamp());
            avroColumns.add(avroColumn);
        }
        
        return avroColumns;
    }

    @Override
//TODO: FIX: extend Avro spec to support Clock
    public Void insert(Utf8 keyspace, Utf8 key, ColumnPath cp, ByteBuffer value, long timestamp, ConsistencyLevel consistencyLevel)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("insert");

        // FIXME: This is repetitive.
        byte[] column, super_column;
        column = cp.column == null ? null : cp.column.array();
        super_column = cp.super_column == null ? null : cp.super_column.array();
        String column_family = cp.column_family.toString();
        String keyspace_string = keyspace.toString();

        AvroValidation.validateKey(keyspace_string);
        AvroValidation.validateColumnPath(keyspace_string, cp);

        RowMutation rm = new RowMutation(keyspace_string, key.toString());
        try
        {
//TODO: MODIFY: create appropriate IClock impl
//            rm.add(new QueryPath(column_family, super_column, column), value.array(), timestamp);
            IClock cassandra_clock = new TimestampClock(timestamp);
            rm.add(new QueryPath(column_family, super_column, column), value.array(), cassandra_clock);
        }
        catch (MarshalException e)
        {
            throw newInvalidRequestException(e.getMessage());
        }
        doInsert(consistencyLevel, rm);

        return null;
    }

    private void doInsert(ConsistencyLevel consistency, RowMutation rm) throws UnavailableException, TimedOutException
    {
        if (consistency != ConsistencyLevel.ZERO)
        {
            try
            {
                StorageProxy.mutateBlocking(Arrays.asList(rm), thriftConsistencyLevel(consistency));
            }
            catch (TimeoutException e)
            {
                throw new TimedOutException();
            }
            catch (org.apache.cassandra.thrift.UnavailableException thriftE)
            {
                throw new UnavailableException();
            }
        }
        else
        {
            StorageProxy.mutate(Arrays.asList(rm));
        }
    }

    @Override
    public Void batch_insert(Utf8 keyspace, Utf8 key, Map<Utf8, GenericArray<ColumnOrSuperColumn>> cfmap, ConsistencyLevel consistency)
    throws AvroRemoteException, InvalidRequestException, UnavailableException, TimedOutException
    {
        if (logger.isDebugEnabled())
            logger.debug("batch_insert");

        String keyString = key.toString();
        String keyspaceString = keyspace.toString();

        AvroValidation.validateKey(keyString);

        for (Utf8 cfName : cfmap.keySet())
        {
            for (ColumnOrSuperColumn cosc : cfmap.get(cfName))
                AvroValidation.validateColumnOrSuperColumn(keyspaceString, cfName.toString(), cosc);
        }

        doInsert(consistency, getRowMutation(keyspaceString, keyString, cfmap));
        return null;
    }

    // FIXME: This is copypasta from o.a.c.db.RowMutation, (RowMutation.getRowMutation uses Thrift types directly).
    private static RowMutation getRowMutation(String keyspace, String key, Map<Utf8, GenericArray<ColumnOrSuperColumn>> cfmap)
    {
        RowMutation rm = new RowMutation(keyspace, key.trim());
        for (Map.Entry<Utf8, GenericArray<ColumnOrSuperColumn>> entry : cfmap.entrySet())
        {
            String cfName = entry.getKey().toString();
            for (ColumnOrSuperColumn cosc : entry.getValue())
            {
                if (cosc.column == null)
                {
                    assert cosc.super_column != null;
                    for (Column column : cosc.super_column.columns)
                    {
                        QueryPath path = new QueryPath(cfName, cosc.super_column.name.array(), column.name.array());
//TODO: MODIFY: create appropriate IClock impl
//                        rm.add(path, column.value.array(), column.timestamp);
                        IClock cassandra_clock = new TimestampClock(column.timestamp);
                        rm.add(path, column.value.array(), cassandra_clock);
                    }
                }
                else
                {
                    assert cosc.super_column == null;
                    QueryPath path = new QueryPath(cfName, null, cosc.column.name.array());
//TODO: MODIFY: create appropriate IClock impl
//                    rm.add(path, cosc.column.value.array(), cosc.column.timestamp);
                    IClock cassandra_clock = new TimestampClock(cosc.column.timestamp);
                    rm.add(path, cosc.column.value.array(), cassandra_clock);
                }
            }
        }
        return rm;
    }

    private org.apache.cassandra.thrift.ConsistencyLevel thriftConsistencyLevel(ConsistencyLevel consistency)
    {
        switch (consistency)
        {
            case ZERO: return org.apache.cassandra.thrift.ConsistencyLevel.ZERO;
            case ONE: return org.apache.cassandra.thrift.ConsistencyLevel.ONE;
            case QUORUM: return org.apache.cassandra.thrift.ConsistencyLevel.QUORUM;
            case DCQUORUM: return org.apache.cassandra.thrift.ConsistencyLevel.DCQUORUM;
            case DCQUORUMSYNC: return org.apache.cassandra.thrift.ConsistencyLevel.DCQUORUMSYNC;
            case ALL: return org.apache.cassandra.thrift.ConsistencyLevel.ALL;
        }
        return null;
    }

    @Override
    public Utf8 get_api_version() throws AvroRemoteException
    {
        return API_VERSION;
    }
}
