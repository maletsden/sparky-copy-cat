package analyzers.top_channels_by_days_analyzer.data_types;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ChannelDescription implements Serializable, Comparable<ChannelDescription> {
    public final String channel_name;
    public Long total_trending_days;
    public final List<VideoDayDescription> videos_days;

    public ChannelDescription(
            String channel_name_, VideoDayDescription video_day
    ) {
        channel_name = channel_name_;
        total_trending_days = video_day.trending_days;
        videos_days = new ArrayList<>(Arrays.asList(video_day));
    }

    public ChannelDescription merge(
            ChannelDescription desc
    ) {
        total_trending_days += desc.total_trending_days;
        videos_days.addAll(desc.videos_days);

        return this;
    }

    public ChannelDescription mergeOneVideoChannels(
            ChannelDescription desc
    ) {
        total_trending_days += desc.total_trending_days;
        videos_days.get(0).merge(desc.videos_days.get(0));

        return this;
    }

    @Override
    public int compareTo(ChannelDescription desc) {
        return (int) (total_trending_days - desc.total_trending_days);
    }
}
