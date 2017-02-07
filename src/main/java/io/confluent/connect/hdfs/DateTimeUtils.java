package io.confluent.connect.hdfs;

import org.joda.time.DateTimeZone;

public class DateTimeUtils {
    private static final long DAY_IN_MS = 24 * 60 * 60 * 1000;

    /**
     * Calculates next period of periodMs after currentTimeMs starting from midnight in given timeZone.
     * If the next period is in next day then 12am of next day will be returned
     * @param currentTimeMs time to calculate at
     * @param periodMs period in ms
     * @param timeZone timezone to get midnight time
     * @return timestamp in ms
     */
    public static long getNextTimeAdjustedByDay(long currentTimeMs, long periodMs, DateTimeZone timeZone) {
        long startOfDay = timeZone.convertLocalToUTC(timeZone.convertUTCToLocal(currentTimeMs) / DAY_IN_MS * DAY_IN_MS, true);
        long nextPeriodOffset = ((currentTimeMs - startOfDay) / periodMs + 1) * periodMs;
        long offset = Math.min(nextPeriodOffset, DAY_IN_MS);
        return startOfDay + offset;
    }
}
