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

package org.apache.cassandra.utils;

import java.io.*;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.log4j.Logger;

import org.apache.commons.collections.iterators.CollatingIterator;
import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IClock;
import org.apache.cassandra.db.IClock.ClockRelationship;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

public class FBUtilities
{
    private static Logger logger_ = Logger.getLogger(FBUtilities.class);

    public static final BigInteger TWO = new BigInteger("2");

    private static volatile InetAddress localInetAddress_;

    public static String[] strip(String string, String token)
    {
        StringTokenizer st = new StringTokenizer(string, token);
        List<String> result = new ArrayList<String>();
        while ( st.hasMoreTokens() )
        {
            result.add( (String)st.nextElement() );
        }
        return result.toArray( new String[0] );
    }

    /**
     * Parses a string representing either a fraction, absolute value or percentage.
     */
    public static double parseDoubleOrPercent(String value)
    {
        if (value.endsWith("%"))
        {
            return Double.valueOf(value.substring(0, value.length() - 1)) / 100;
        }
        else
        {
            return Double.valueOf(value);
        }
    }

    public static InetAddress getLocalAddress()
    {
        if (localInetAddress_ == null)
            try
            {
                localInetAddress_ = DatabaseDescriptor.getListenAddress() == null
                                    ? InetAddress.getLocalHost()
                                    : DatabaseDescriptor.getListenAddress();
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        return localInetAddress_;
    }

    /**
     * @param fractOrAbs A double that may represent a fraction or absolute value.
     * @param total If fractionOrAbs is a fraction, the total to take the fraction from
     * @return An absolute value which may be larger than the total.
     */
    public static long absoluteFromFraction(double fractOrAbs, long total)
    {
        if (fractOrAbs < 0)
            throw new UnsupportedOperationException("unexpected negative value " + fractOrAbs);

        if (0 < fractOrAbs && fractOrAbs < 1)
        {
            // fraction
            return Math.max(1, (long)(fractOrAbs * total));
        }

        // absolute
        assert fractOrAbs >= 1 || fractOrAbs == 0;
        return (long)fractOrAbs;
    }

    /**
     * Given two bit arrays represented as BigIntegers, containing the given
     * number of significant bits, calculate a midpoint.
     *
     * @param left The left point.
     * @param right The right point.
     * @param sigbits The number of bits in the points that are significant.
     * @return A midpoint that will compare bitwise halfway between the params, and
     * a boolean representing whether a non-zero lsbit remainder was generated.
     */
    public static Pair<BigInteger,Boolean> midpoint(BigInteger left, BigInteger right, int sigbits)
    {
        BigInteger midpoint;
        boolean remainder;
        if (left.compareTo(right) < 0)
        {
            BigInteger sum = left.add(right);
            remainder = sum.testBit(0);
            midpoint = sum.shiftRight(1);
        }
        else
        {
            BigInteger max = TWO.pow(sigbits);
            // wrapping case
            BigInteger distance = max.add(right).subtract(left);
            remainder = distance.testBit(0);
            midpoint = distance.shiftRight(1).add(left).mod(max);
        }
        return new Pair(midpoint, remainder);
    }

    // copy bytes from int into bytes starting from offset
    public static void copyIntoBytes(byte[] bytes, int offset, int i)
    {
        bytes[offset]   = (byte)( ( i >>> 24 ) & 0xFF );
        bytes[offset+1] = (byte)( ( i >>> 16 ) & 0xFF );
        bytes[offset+2] = (byte)( ( i >>> 8  ) & 0xFF );
        bytes[offset+3] = (byte)(   i          & 0xFF );
    }

    // copy bytes from long into bytes starting from offset
    public static void copyIntoBytes(byte[] bytes, int offset, long l)
    {
        bytes[offset]   = (byte)( ( l >>> 56 ) & 0xFF );
        bytes[offset+1] = (byte)( ( l >>> 48 ) & 0xFF );
        bytes[offset+2] = (byte)( ( l >>> 40 ) & 0xFF );
        bytes[offset+3] = (byte)( ( l >>> 32 ) & 0xFF );
        bytes[offset+4] = (byte)( ( l >>> 24 ) & 0xFF );
        bytes[offset+5] = (byte)( ( l >>> 16 ) & 0xFF );
        bytes[offset+6] = (byte)( ( l >>> 8  ) & 0xFF );
        bytes[offset+7] = (byte)(   l          & 0xFF );
    }

    public static byte[] toByteArray(int i)
    {
        byte[] bytes = new byte[4];
        copyIntoBytes(bytes, 0, i);
        return bytes;
    }

    public static byte[] toByteArray(long l)
    {
        byte[] bytes = new byte[8];
        copyIntoBytes(bytes, 0, l);
        return bytes;
    }

    public static int byteArrayToInt(byte[] bytes)
    {
    	return byteArrayToInt(bytes, 0);
    }

    public static int byteArrayToInt(byte[] bytes, int offset)
    {
        if ( bytes.length - offset < 4 )
        {
            throw new IllegalArgumentException("An integer must be 4 bytes in size.");
        }
        int n = 0;
        for ( int i = 0; i < 4; ++i )
        {
            n <<= 8;
            n |= bytes[offset + i] & 0xFF;
        }
        return n;
    }

    public static long byteArrayToLong(byte[] bytes)
    {
        return byteArrayToLong(bytes, 0);
    }

    public static long byteArrayToLong(byte[] bytes, int offset)
    {
        if ( bytes.length - offset < 8 )
        {
            throw new IllegalArgumentException("A long must be 8 bytes in size.");
        }
        long n = 0;
        for ( int i = 0; i < 8; ++i )
        {
            n <<= 8;
            n |= bytes[offset + i] & 0xFF;
        }
        return n;
    }

    public static int compareByteArrays(byte[] bytes1, byte[] bytes2){
        if(null == bytes1){
            if(null == bytes2) return 0;
            else return -1;
        }
        if(null == bytes2) return 1;

        int minLength = Math.min(bytes1.length, bytes2.length);
        for(int i = 0; i < minLength; i++)
        {
            if(bytes1[i] == bytes2[i])
                continue;
            // compare non-equal bytes as unsigned
            return (bytes1[i] & 0xFF) < (bytes2[i] & 0xFF) ? -1 : 1;
        }
        if(bytes1.length == bytes2.length) return 0;
        else return (bytes1.length < bytes2.length)? -1 : 1;
    }

    // compare two byte[] at specified offsets for length
    public static int compareByteSubArrays(byte[] bytes1, int offset1, byte[] bytes2, int offset2, int length)
    {
        if ( null == bytes1 )
        {
            if ( null == bytes2) return 0;
            else return -1;
        }
        if (null == bytes2 ) return 1;

        assert bytes1.length >= (offset1 + length) : "The first byte array isn't long enough for the specified offset and length.";
        assert bytes2.length >= (offset2 + length) : "The second byte array isn't long enough for the specified offset and length.";
        for ( int i = 0; i < length; i++ )
        {
            byte byte1 = bytes1[offset1+i];
            byte byte2 = bytes2[offset2+i];
            if ( byte1 == byte2 )
                continue;
            // compare non-equal bytes as unsigned
            return (byte1 & 0xFF) < (byte2 & 0xFF) ? -1 : 1;
        }
        return 0;
    }

    /**
     * @return The bitwise XOR of the inputs. The output will be the same length as the
     * longer input, but if either input is null, the output will be null.
     */
    public static byte[] xor(byte[] left, byte[] right)
    {
        if (left == null || right == null)
            return null;
        if (left.length > right.length)
        {
            byte[] swap = left;
            left = right;
            right = swap;
        }

        // left.length is now <= right.length
        byte[] out = Arrays.copyOf(right, right.length);
        for (int i = 0; i < left.length; i++)
        {
            out[i] = (byte)((left[i] & 0xFF) ^ (right[i] & 0xFF));
        }
        return out;
    }

    public static BigInteger hash(String data)
    {
        byte[] result = hash("MD5", data.getBytes());
        BigInteger hash = new BigInteger(result);
        return hash.abs();        
    }

    public static byte[] hash(String type, byte[]... data)
    {
    	byte[] result = null;
    	try
        {
            MessageDigest messageDigest = MessageDigest.getInstance(type);
            for(byte[] block : data)
                messageDigest.update(block);
            result = messageDigest.digest();
    	}
    	catch (Exception e)
        {
            throw new RuntimeException(e);
    	}
    	return result;
	}

    // The given byte array is compressed onto the specified stream.
    // The method does not close the stream. The caller will have to do it.
    public static void compressToStream(byte[] input, ByteArrayOutputStream bos) throws IOException
    {
    	 // Create the compressor with highest level of compression
        Deflater compressor = new Deflater();
        compressor.setLevel(Deflater.BEST_COMPRESSION);

        // Give the compressor the data to compress
        compressor.setInput(input);
        compressor.finish();

        // Write the compressed data to the stream
        byte[] buf = new byte[1024];
        while (!compressor.finished())
        {
            int count = compressor.deflate(buf);
            bos.write(buf, 0, count);
        }
    }

    public static byte[] decompress(byte[] compressedData, int off, int len) throws IOException, DataFormatException
    {
    	 // Create the decompressor and give it the data to compress
        Inflater decompressor = new Inflater();
        decompressor.setInput(compressedData, off, len);

        // Create an expandable byte array to hold the decompressed data
        ByteArrayOutputStream bos = new ByteArrayOutputStream(compressedData.length);

        // Decompress the data
        byte[] buf = new byte[1024];
        while (!decompressor.finished())
        {
            int count = decompressor.inflate(buf);
            bos.write(buf, 0, count);
        }
        bos.close();

        // Get the decompressed data
        return bos.toByteArray();
    }

    public static void writeByteArray(byte[] bytes, DataOutput out) throws IOException
    {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] hexToBytes(String str)
    {
        assert str.length() % 2 == 0;
        byte[] bytes = new byte[str.length()/2];
        for (int i = 0; i < bytes.length; i++)
        {
            bytes[i] = (byte)Integer.parseInt(str.substring(i*2, i*2+2), 16);
        }
        return bytes;
    }

    public static String bytesToHex(byte... bytes)
    {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes)
        {
            int bint = b & 0xff;
            if (bint <= 0xF)
                // toHexString does not 0 pad its results.
                sb.append("0");
            sb.append(Integer.toHexString(bint));
        }
        return sb.toString();
    }

    public static String mapToString(Map<?,?> map)
    {
        StringBuilder sb = new StringBuilder("{");

        for (Map.Entry entry : map.entrySet())
        {
            sb.append(entry.getKey()).append(": ").append(entry.getValue()).append(", ");
        }

        return sb.append("}").toString();
    }

    public static void writeNullableString(String key, DataOutput dos) throws IOException
    {
        dos.writeBoolean(key == null);
        if (key != null)
        {
            dos.writeUTF(key);
        }
    }

    public static String readNullableString(DataInput dis) throws IOException
    {
        if (dis.readBoolean())
            return null;
        return dis.readUTF();
    }

    public static void renameWithConfirm(String tmpFilename, String filename) throws IOException
    {
        if (!new File(tmpFilename).renameTo(new File(filename)))
        {
            throw new IOException("rename failed of " + filename);
        }
    }

    public static <T extends Comparable<T>> CollatingIterator getCollatingIterator()
    {
        // CollatingIterator will happily NPE if you do not specify a comparator explicitly
        return new CollatingIterator(new Comparator<T>()
        {
            public int compare(T o1, T o2)
            {
                return o1.compareTo(o2);
            }
        });
    }

    public static void atomicSetMax(AtomicInteger atomic, int i)
    {
        int j;
        while (true)
        {
            if ((j = atomic.getAndSet(i)) <= i)
                break;
            i = j;
        }
    }

    public static void atomicSetMax(AtomicLong atomic, long i)
    {
        long j;
        while (true)
        {
            if ((j = atomic.getAndSet(i)) <= i)
                break;
            i = j;
        }
    }

    public static void atomicSetMax(AtomicReference<IClock> atomic, IClock newClock)
    {
    outer:
        while (true)
        {
            IClock oldClock = atomic.getAndSet(newClock);
            ClockRelationship rel = oldClock.compare(newClock);
            switch (rel)
            {
                case LESS_THAN:
                case EQUAL:
                    break outer;
                case GREATER_THAN:
                    newClock = oldClock;
                    break;
                default: // DISJOINT
                    List<IClock> clocks = new LinkedList<IClock>();
                    clocks.add(newClock);

                    newClock = oldClock.getSuperset(clocks);
            }
        }
    }

    public static void serialize(TSerializer serializer, TBase struct, DataOutput out)
    throws IOException
    {
        assert serializer != null;
        assert struct != null;
        assert out != null;
        byte[] bytes;
        try
        {
            bytes = serializer.serialize(struct);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static void deserialize(TDeserializer deserializer, TBase struct, DataInput in)
    throws IOException
    {
        assert deserializer != null;
        assert struct != null;
        assert in != null;
        byte[] bytes = new byte[in.readInt()];
        in.readFully(bytes);
        try
        {
            deserializer.deserialize(struct, bytes);
        }
        catch (TException ex)
        {
            throw new IOException(ex);
        }
    }

    public static void sortSampledKeys(List<DecoratedKey> keys, Range range)
    {
        if (range.left.compareTo(range.right) >= 0)
        {
            // range wraps.  have to be careful that we sort in the same order as the range to find the right midpoint.
            final Token right = range.right;
            Comparator<DecoratedKey> comparator = new Comparator<DecoratedKey>()
            {
                public int compare(DecoratedKey o1, DecoratedKey o2)
                {
                    if ((right.compareTo(o1.token) < 0 && right.compareTo(o2.token) < 0)
                        || (right.compareTo(o1.token) > 0 && right.compareTo(o2.token) > 0))
                    {
                        // both tokens are on the same side of the wrap point
                        return o1.compareTo(o2);
                    }
                    return -o1.compareTo(o2);
                }
            };
            Collections.sort(keys, comparator);
        }
        else
        {
            // unwrapped range (left < right).  standard sort is all we need.
            Collections.sort(keys);
        }
    }

    public static boolean equals(Object a, Object b)
    {
        if (a == null && b == null)
            return true;
        else if (a != null && b == null)
            return false;
        else if (a == null && b != null)
            return false;
        else
            return a.equals(b);
    }

    public static int encodedUTF8Length(String st)
    {
        int strlen = st.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++)
        {
            int c = st.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }
        return utflen;
    }

    // thin wrapper around byte[] to provide meaningful equals() and hashCode() operations
    // caveat: assumed that wrapped byte[] will not be modified
    public static final class ByteArrayWrapper
    {
        public final byte[] data;

        public ByteArrayWrapper(byte[] data)
        {
            if ( null == data )
            {
                throw new NullPointerException();
            }
            this.data = data;
        }

        @Override
        public boolean equals(Object other)
        {
            if ( !( other instanceof ByteArrayWrapper ) )
            {
                return false;
            }
            return Arrays.equals(data, ((ByteArrayWrapper)other).data);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(data);
        }

        @Override
        public String toString()
        {
            return ArrayUtils.toString(data);
        }
    }
}
