package ru.mephi.chenko.spark.util;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DateUtil {

    private static final Integer WINDOW_MINUTES_SIZE = 30;

    /**
     * Round date
     * @param date Date to be rounded
     * @param scale Scale for aggregation
     * @return Rounded date
     */
    public static Date round(Date date, String scale) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        Date result = date;

        if(scale.contains("m")) {
            int minutes = Integer.parseInt(scale.replace("m", ""));
            int unroundedMinutes = calendar.get(Calendar.MINUTE);
            int mod = unroundedMinutes % minutes;
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            calendar.set(Calendar.MINUTE, unroundedMinutes - mod);
            result = calendar.getTime();
        } else if(scale.contains("h")) {
            int hours = Integer.parseInt(scale.replace("h", ""));
            int unroundedHours = calendar.get(Calendar.MINUTE);
            int mod = unroundedHours % hours;
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.HOUR, unroundedHours - mod);
            result = calendar.getTime();
        }


        return result;
    }

    /**
     * Validate scale format
     * @param scale Scale for aggregation
     */
    public static void validateScale(String scale) throws IllegalAccessException {
        if(!scale.toLowerCase().matches("\\d+m") && !scale.toLowerCase().matches("\\d+h")) {
            throw new IllegalAccessException("Illegal scale format");
        }
    }

    /**
     * Returns window start time
     * @return window start time
     */
    public static Date getWindowStartTime() {
        return new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(WINDOW_MINUTES_SIZE));
    }

    /**
     * Returns window end time
     * @return window end time
     */
    public static Date getWindowEndTime() {
        return new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(WINDOW_MINUTES_SIZE));
    }
}
