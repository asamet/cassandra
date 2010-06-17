package org.apache.cassandra.db;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.net.InetAddress;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

public class RepairOnWriteTask implements Runnable
{
    private RowMutation mutation;
    private static Logger logger = Logger.getLogger(RepairOnWriteTask.class);

    public RepairOnWriteTask(RowMutation mutation)
    {
        this.mutation = mutation;
    }

    public void run()
    {
        // construct SliceByNamesReadCommand for counter-type CFs
        List<ReadCommand> readCommands = new LinkedList<ReadCommand>();
        for (ColumnFamily columnFamily : mutation.getColumnFamilies())
        {
            ColumnType columnType = columnFamily.getColumnType();
            // TODO(asamet) - Min and Max counters can be done more efficiently here.  We don't
            // actually need to do a read for them, we can just dumbly publish the new min or max.
            if (!columnType.isCounter())
                continue;

            // CF type: regular
            if (!columnType.isSuper())
            {
                QueryPath queryPath = new QueryPath(columnFamily.name());
                ReadCommand readCommand = new SliceByNamesReadCommand(
                    mutation.getTable(),
                    mutation.key(),
                    queryPath,
                    columnFamily.getColumnNames()
                    );

                readCommands.add(readCommand);
                continue;
            }

            // CF type: super
            for (IColumn superColumn : columnFamily.getSortedColumns())
            {
                QueryPath queryPath = new QueryPath(columnFamily.name(), superColumn.name());

                // construct set of sub-column names
                Collection<IColumn> subColumns = superColumn.getSubColumns();
                Collection<byte[]> subColNames = new HashSet<byte[]>(subColumns.size());
                for (IColumn subCol : subColumns)
                {
                    subColNames.add(subCol.name());
                }

                ReadCommand readCommand = new SliceByNamesReadCommand(
                    mutation.getTable(),
                    mutation.key(),
                    queryPath,
                    subColNames
                    );

                readCommands.add(readCommand);
            }
        }

        if (0 == readCommands.size())
        {
            return;
        }

        try
        {
            // send repair to non-local replicas
            List<InetAddress> foreignReplicas = StorageService.instance.getLiveNaturalEndpoints(
                mutation.getTable(),
                mutation.key()
                );
            foreignReplicas.remove(FBUtilities.getLocalAddress()); // remove local replica


            // create a repair RowMutation
            RowMutation repairRowMutation = new RowMutation(mutation.getTable(), mutation.key());
            for (ReadCommand readCommand : readCommands)
            {
                Table table = Table.open(readCommand.table);
                Row row = readCommand.getRow(table);
                /*
                 * Clean out contexts for all nodes we're sending the repair to, otherwise we
                 * could send a context which is local to one of the foreign replicas, which
                 * would then add incorrectly add that to its own count, because local resolution
                 * sums.
                 */
                for (InetAddress foreignNode : foreignReplicas)
                {
                  row.cf.cleanForCounter(foreignNode);
                }
                // For every node we're sending a repair to, clean its counts out.
                repairRowMutation.add(row.cf);
            }

            for (InetAddress foreignReplica : foreignReplicas)
            {
                RowMutationMessage repairMessage = new RowMutationMessage(
                    repairRowMutation);
                Message message = repairMessage.makeRowMutationMessage(StorageService.Verb.REPAIR_ON_WRITE);
                MessagingService.instance.sendOneWay(message, foreignReplica);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
