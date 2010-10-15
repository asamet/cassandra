package org.apache.cassandra.tools;

import java.io.*;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
//TODO: TEST
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.IColumn;
//TODO: TEST
import org.apache.cassandra.db.TimestampClock;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.IteratingRow;
import org.apache.cassandra.io.SSTable;
import org.apache.cassandra.io.SSTableReader;
import org.apache.cassandra.io.SSTableScanner;
import org.apache.cassandra.io.util.BufferedRandomAccessFile;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;

import static org.apache.cassandra.utils.FBUtilities.bytesToHex;
import org.apache.commons.cli.*;

public class SSTableGenerateBloomFilter
{
    private static int INPUT_FILE_BUFFER_SIZE = 1024 * 1024 * 1024;

    private static Options options;
    private static CommandLine cmd;

    static
    {
        options = new Options();
    }

    public static void generateBloomFilter(String ssTableFile)
    throws IOException
    {
        File file = new File(SSTable.filterFilename(ssTableFile));
        if (file.exists())
        {
            System.out.println("bailing; found a filter file, already.");
            return;
        }

        System.out.println("counting keys:");
        IPartitioner partitioner = StorageService.getPartitioner();
        BufferedRandomAccessFile input = new BufferedRandomAccessFile(SSTable.indexFilename(ssTableFile), "r");
        long numKeys = 0L;
        while (!input.isEOF())
        {
            input.readUTF();
            input.readLong();

            numKeys++;
            System.out.println(" -- " + input.getFilePointer() + " / " + input.length());
        }
        input.close();

        System.out.println("creating BF:");
        BloomFilter recoveredBF = BloomFilter.getFilter(numKeys, 15);
        input = new BufferedRandomAccessFile(SSTable.indexFilename(ssTableFile), "r");
        while (!input.isEOF())
        {
            String diskKey = input.readUTF();
            input.readLong();

            recoveredBF.add(diskKey);
            System.out.println(" -- " + input.getFilePointer() + " / " + input.length());
        }
        input.close();

        // bloom filter
        System.out.println("creating BF:");
        FileOutputStream fos = new FileOutputStream(SSTable.filterFilename(ssTableFile));
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(recoveredBF, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();
    }

    public static void main(String[] args) throws IOException
    {
        String usage = String.format("Usage: %s <sstable>", SSTableGenerateBloomFilter.class.getName());
        
        CommandLineParser parser = new PosixParser();
        try
        {
            cmd = parser.parse(options, args);
        } catch (ParseException e1)
        {
            System.err.println(e1.getMessage());
            System.err.println(usage);
            System.exit(1);
        }

        if (cmd.getArgs().length != 1)
        {
            System.err.println("You must supply exactly one sstable");
            System.err.println(usage);
            System.exit(1);
        }

        String ssTableFileName = new File(cmd.getArgs()[0]).getAbsolutePath();

        generateBloomFilter(ssTableFileName);

        System.exit(0);
    }
}
