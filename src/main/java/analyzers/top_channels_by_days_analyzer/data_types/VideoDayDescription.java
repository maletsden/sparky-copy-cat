package analyzers.top_channels_by_days_analyzer.data_types;

import java.io.Serializable;

public class VideoDayDescription implements Serializable {
    public final String video_id;
    public final String video_title;
    public Long trending_days;

    public VideoDayDescription(
            String video_id_, String video_title_
    ) {
        video_id = video_id_;
        video_title = video_title_;
        trending_days = 1L;
    }

    public VideoDayDescription merge(
            VideoDayDescription desc
    ) {
        trending_days += desc.trending_days;
        return this;
    }
}
