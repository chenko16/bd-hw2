package ru.mephi.chenko.spark.util;

import java.util.Calendar;
import java.util.Date;

public class DateUtil {

    /**
     * Round date
     * @param date Date to be rounded
     * @param minutes Value of minutes date will aggregated by
     * @return Rounded date
     */
    public static Date round(Date date, int minutes) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        int unroundedMinutes = calendar.get(Calendar.MINUTE);
        int mod = unroundedMinutes % minutes;
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.MINUTE, unroundedMinutes - mod);
        Date result = calendar.getTime();

        return result;
    }
}
