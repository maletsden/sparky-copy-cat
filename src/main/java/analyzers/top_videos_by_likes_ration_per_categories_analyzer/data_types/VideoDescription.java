package analyzers.top_videos_by_likes_ration_per_categories_analyzer.data_types;

import scala.Serializable;

public final class VideoDescription implements Serializable, Comparable<VideoDescription> {
    public final String video_id;
    public final String video_title;
    public Double ratio_likes_dislikes;
    public Long views;

    public VideoDescription(
            String video_id_, String video_title_,
            Long likes, Long dislikes, Long views_
    ) {
        video_id = video_id_;
        video_title = video_title_;
        ratio_likes_dislikes = ((double) likes) / ((double) dislikes);
        views = views_;
    }

    public VideoDescription merge(
            VideoDescription desc
    ) {
        if (ratio_likes_dislikes < desc.ratio_likes_dislikes) {
            ratio_likes_dislikes = desc.ratio_likes_dislikes;
            views = desc.views;
        }

        return this;
    }

    @Override
    public int compareTo(
            VideoDescription desc
    ) {
        return ratio_likes_dislikes.compareTo(desc.ratio_likes_dislikes);
    }
}
