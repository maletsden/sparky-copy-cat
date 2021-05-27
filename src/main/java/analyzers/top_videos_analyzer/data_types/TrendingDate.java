package analyzers.top_videos_analyzer.data_types;

import scala.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TrendingDate implements Serializable, Comparable<TrendingDate> {
    public final String date;
    public final Long views;
    public final Long likes;
    public final Long dislikes;

    public TrendingDate(String date_, Long views_, Long likes_, Long dislikes_) {
        date = date_;
        views = views_;
        likes = likes_;
        dislikes = dislikes_;
    }

    @Override
    public int compareTo(TrendingDate date2) {
        SimpleDateFormat sdf = new SimpleDateFormat("yy.dd.MM");

        Date d1;
        Date d2;

        try {
            d1 = sdf.parse(date);
            d2 = sdf.parse(date2.date);
        } catch (ParseException e) {
            e.printStackTrace();
            return 0;
        }

        if (d1.equals(d2)) {
            return 0;
        }

        return d1.before(d2) ? -1 : 1;
    }
}
