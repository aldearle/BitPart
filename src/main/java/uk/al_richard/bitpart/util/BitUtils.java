package uk.al_richard.bitpart.util;

public class BitUtils {

    public static int bits_to_longs(int size_in_bits) {

        int bytes_size = size_in_bits / 8;
        if(size_in_bits %8!=0) {
            bytes_size += 1;
        }
        return bytes_to_longs( bytes_size );
    }

    public static int bits_to_bytes(int size_in_bits) {

        int bytes_size = size_in_bits / 8;
        if(size_in_bits %8!=0) {
            bytes_size += 1;
        }
        return bytes_size;
    }


    private static int bytes_to_ints(int bytes_size) {
        int n_ints = bytes_size / Integer.BYTES;
        if(bytes_size %4!=0) {
            n_ints += 1;
        }
        return n_ints;
    }

    private static int bytes_to_longs(int bytes_size) {
        int n_longs = bytes_size / Long.BYTES;
        if(bytes_size %8!=0) {
            n_longs += 1;
        }
        return n_longs;
    }

}
