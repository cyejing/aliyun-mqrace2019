package io.openmessaging.arms.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author Born
 */
public class ByteUtil {

    public static byte[] toIntBytes(long x) {
        return new byte[]{ long3(x), long2(x), long1(x), long0(x)};
    }

    public static long getInt(byte[] bytes) {
        return makeInt(bytes[0],
                bytes[1],
                bytes[2],
                bytes[3]);
    }

    static private int makeInt(byte b3, byte b2, byte b1, byte b0) {
        return (((b3       ) << 24) |
                ((b2 & 0xff) << 16) |
                ((b1 & 0xff) <<  8) |
                ((b0 & 0xff)      ));
    }

    private static byte long7(long x) { return (byte)(x >> 56); }
    private static byte long6(long x) { return (byte)(x >> 48); }
    private static byte long5(long x) { return (byte)(x >> 40); }
    private static byte long4(long x) { return (byte)(x >> 32); }
    private static byte long3(long x) { return (byte)(x >> 24); }
    private static byte long2(long x) { return (byte)(x >> 16); }
    private static byte long1(long x) { return (byte)(x >>  8); }
    private static byte long0(long x) { return (byte)(x      ); }


}
