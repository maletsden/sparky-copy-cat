package analyzers.top_tags_per_month_analyzer.data_types;

import scala.Serializable;

import java.util.*;

public class MonthDescription implements Serializable {
    public final String start_date;
    public final String end_date;
    public List<TagStat> tags;

    public MonthDescription(
            MonthHasMapDescription desc, Integer tags_num
    ) {
        start_date = desc.start_date;
        end_date = desc.end_date;
        tags = new ArrayList<>();

        saveOnlyPopularTags(desc, tags_num);
    }

    public void saveOnlyPopularTags(MonthHasMapDescription desc, Integer tags_num) {
        List<Map.Entry<String, TagStat>> list = new LinkedList<>(desc.tags.entrySet());

        list.sort((entry1, entry2) -> -entry1.getValue().compareTo(entry2.getValue()));

        List<Map.Entry<String, TagStat>> top_list = (tags_num >= 0 && tags_num < list.size())
                ? list.subList(0, tags_num)
                : list;

        top_list.forEach(entry -> tags.add(entry.getValue()));
    }
}
