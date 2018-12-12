package com.zaxxer.influx4j.util;

import java.util.Formatter;

/**
 * Hex Dump Elf
 *
 * xxxx: 00 11 22 33 44 55 66 77   88 99 aa bb cc dd ee ff ................
 */
public final class HexDumpElf
{
    private static final int MAX_VISIBLE = 127;
    private static final int MIN_VISIBLE = 31;

    private HexDumpElf()
    {
        // private constructor
    }

    /**
     * @param displayOffset the display offset (left column)
     * @param data the byte array of data
     * @param offset the offset to start dumping in the byte array
     * @param len the length of data to dump
     * @return the dump string
     */
    public static String dump(final int displayOffset, final byte[] data, final int offset, final long len)
    {
        final StringBuilder sb = new StringBuilder();
        final Formatter formatter = new Formatter(sb);
        final StringBuilder ascii = new StringBuilder();

        int dataNdx = offset;
        final long maxDataNdx = offset + len;
        final long lines = (len + 16) / 16;
        for (int i = 0; i < lines; i++)
        {
            ascii.append(" |");
            formatter.format("%08x  ", displayOffset + (i * 16));

            for (int j = 0; j < 16; j++)
            {
                if (dataNdx < maxDataNdx)
                {
                    byte b = data[dataNdx++];
                    formatter.format("%02x ", b);
                    ascii.append((b > MIN_VISIBLE && b < MAX_VISIBLE) ? (char) b : ' ');
                }
                else
                {
                    sb.append("   ");
                }

                if (j == 7)
                {
                    sb.append(' ');
                }
            }

            ascii.append('|');
            sb.append(ascii).append('\n');
            ascii.setLength(0);
        }

        formatter.close();
        return sb.toString();
    }
}
