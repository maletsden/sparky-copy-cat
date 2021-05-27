package analyzers.helpers;

import scala.Tuple3;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public final class DatesAnalyzer {
    public static final long MILLISECONDS_IN_DAY = 1000 * 60 * 60 * 24;
    public static final SimpleDateFormat SDF_YY_DD_FF = new SimpleDateFormat("yy.dd.MM");

    public static long diffInDays(Date d1, Date d2) {
        long d1_time = d1.getTime();
        long diff_in_milliseconds = d1_time - d2.getTime();
        return TimeUnit.DAYS.convert(diff_in_milliseconds, TimeUnit.MILLISECONDS);
    }


    public static Date dateStringToDate(SimpleDateFormat sdf, String date_s) {
        try {
            return sdf.parse(date_s);
        } catch (ParseException e) {
            e.printStackTrace();
            return new Date();
        }
    }

    public static Tuple3<Long, String, String> intervalsNumFomToday(
            Date today, String date_s, Integer interval_in_days
    ) {
        Date date = dateStringToDate(SDF_YY_DD_FF, date_s);

        long today_time = today.getTime();
        long diff_in_days = diffInDays(today, date);
        long diff_in_intervals = diff_in_days / interval_in_days;

        Date interval_start_date = new Date(
                today_time - ((MILLISECONDS_IN_DAY * interval_in_days) * diff_in_intervals)
        );
        Date interval_end_date = new Date(
                today_time - ((MILLISECONDS_IN_DAY * (interval_in_days - 1)) * (diff_in_intervals + 1))
        );

        return new Tuple3<>(
                diff_in_intervals,
                SDF_YY_DD_FF.format(interval_start_date),
                SDF_YY_DD_FF.format(interval_end_date)
        );
    }

    public static boolean isBefore(
            SimpleDateFormat sdf, String date1, String date2
    ) {
        Date d1 = dateStringToDate(sdf, date1);
        Date d2 = dateStringToDate(sdf, date2);

        return d1.before(d2);
    }
}
