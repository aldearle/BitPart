package uk.al_richard.bitpart.util;

public class Filename {
    public static String getFileName() {
        try {
            throw new RuntimeException();
        } catch (RuntimeException e) {
            return e.getStackTrace()[1].getClassName();
        }
    }
}
