package analyzers.top_tags_per_month_analyzer.data_types;

import scala.Serializable;

import java.util.*;

public class MonthHasMapDescription implements Serializable {
    public final String start_date;
    public final String end_date;
    public HashMap<String, TagStat> tags;

    public MonthHasMapDescription(
            String start_date_, String end_date_, String tags_, String video_id
    ) {
        start_date = start_date_;
        end_date = end_date_;
        tags = new HashMap<>();

        if (tags_.equals("[none]")) return;

        for (String tag : tags_.split("\\|")) {
            tags.put(tag, new TagStat(tag, video_id));
        }
    }

    public MonthHasMapDescription mergeTags(MonthHasMapDescription desc) {
        desc.tags.forEach((key, value) -> tags.merge(key, value, TagStat::merge));

        return this;
    }
}
