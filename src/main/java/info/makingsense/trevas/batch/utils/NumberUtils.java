package info.makingsense.trevas.batch.utils;

import java.text.NumberFormat;
import java.util.Locale;

public class NumberUtils {

    public static String formatMs(long ms) {
        NumberFormat numberInstance = NumberFormat.getInstance(Locale.FRANCE);
        return numberInstance.format(ms);
    }
}
