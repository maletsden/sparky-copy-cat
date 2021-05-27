package analyzers.top_videos_analyzer.data_types;

import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class VideoDescription implements Serializable, Comparable<VideoDescription> {
    public final String id;
    public final String title;
    public Long latest_views;
    public Long latest_likes;
    public Long latest_dislikes;
    public final List<TrendingDate> trending_days;

    public VideoDescription(
            String id_, String title_,
            Long latest_views_, Long latest_likes_, Long latest_dislikes_,
            TrendingDate trending_day
    ) {
        id = id_;
        title = title_;
        latest_views = latest_views_;
        latest_likes = latest_likes_;
        latest_dislikes = latest_dislikes_;
        trending_days = new ArrayList<>(Arrays.asList(trending_day));
    }

    public VideoDescription normalizeDescription() {
        trending_days.sort(Comparator.naturalOrder());

        TrendingDate lastDate = trending_days.get(trending_days.size() - 1);
        latest_views = lastDate.views;
        latest_likes = lastDate.likes;
        latest_dislikes = lastDate.dislikes;

        return this;
    }

    public VideoDescription merge(
            VideoDescription desc
    ) {
        trending_days.addAll(desc.trending_days);
        return this;
    }

    @Override
    public int compareTo(VideoDescription desc) {
        return trending_days.size() - desc.trending_days.size();
    }
}
