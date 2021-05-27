package analyzers.top_tags_per_month_analyzer.data_types;

import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TagStat implements Serializable, Comparable<TagStat> {
    public final String tag;
    public Long number_of_videos;
    public final List<String> video_ids;

    public TagStat(
            String tag_, String video_id
    ) {
        tag = tag_;
        number_of_videos = 1L;
        video_ids = new ArrayList<>(Arrays.asList(video_id));
    }

    public TagStat merge(TagStat tag_stat) {
        video_ids.addAll(tag_stat.video_ids);
        number_of_videos += tag_stat.number_of_videos;

        return this;
    }


    @Override
    public int compareTo(TagStat tag_stat) {
        return (int) (number_of_videos - tag_stat.number_of_videos);
    }
}
