package analyzers.top_channels_by_views_analyzer.data_types;

import analyzers.helpers.DatesAnalyzer;
import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ChannelDescription implements Serializable, Comparable<ChannelDescription> {
    public final String channel_name;
    public String start_date;
    public String end_date;
    public Long total_views;
    public List<VideoStats> videos_views;

    public ChannelDescription(
            String channel_name_, String start_date_, String end_date_,
            VideoStats video
    ) {
        channel_name = channel_name_;
        start_date = start_date_;
        end_date = end_date_;
        total_views = video.views;
        videos_views = new ArrayList<>(Arrays.asList(video));
    }

    public ChannelDescription mergeOneVideoChannel(
            ChannelDescription desc
    ) {
        updateDates(desc);

        if (desc.total_views > total_views) {
            total_views = desc.total_views;
            videos_views = desc.videos_views;
        }

        return this;
    }

    public ChannelDescription merge(
            ChannelDescription desc
    ) {
        updateDates(desc);

        total_views += desc.total_views;
        videos_views.addAll(desc.videos_views);

        return this;
    }

    private void updateDates(
            ChannelDescription desc
    ) {
        if (DatesAnalyzer.isBefore(DatesAnalyzer.SDF_YY_DD_FF, desc.start_date, start_date))
            start_date = desc.start_date;

        if (DatesAnalyzer.isBefore(DatesAnalyzer.SDF_YY_DD_FF, end_date, desc.end_date))
            end_date = desc.end_date;
    }

    @Override
    public int compareTo(
            ChannelDescription desc
    ) {
        return (int) (total_views - desc.total_views);
    }
}
