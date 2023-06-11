package info.makingsense.trevas.batch.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class TimeUtils {

    public static String getDateNowAsString() {
        return new SimpleDateFormat("EEE MMM dd hh:mm:ss yyyy", Locale.FRANCE).format(new Date());
    }

    public static String getDateNow() {
        return new SimpleDateFormat("yyyy-MM-dd-HH-mm", Locale.FRANCE).format(new Date());
    }
}
