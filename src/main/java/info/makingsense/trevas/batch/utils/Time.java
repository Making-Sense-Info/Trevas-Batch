package info.makingsense.trevas.batch.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Time {

    public static String getDateNow() {
        return new SimpleDateFormat("EEE MMM dd hh:mm:ss yyyy", Locale.FRANCE).format(new Date());
    }
}
