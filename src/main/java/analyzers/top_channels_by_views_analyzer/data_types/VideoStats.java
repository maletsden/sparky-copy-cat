package analyzers.top_channels_by_views_analyzer.data_types;

import scala.Serializable;

public final class VideoStats implements Serializable {
    public final String video_id;
    public Long views;

    public VideoStats(
            String video_id_, Long views_
    ) {
        video_id = video_id_;
        views = views_;
    }
}

