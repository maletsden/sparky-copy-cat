package analyzers.top_category_per_week_analyzer.data_types;

import scala.Serializable;

import java.util.Date;

public class WeekCategoryVideoDescription implements Serializable {
    public String start_date;
    public String end_date;
    public Integer category_id;
    public String category_name;
    public Date start_trending_date;
    public Date end_trending_date;
    public Long start_week_views;
    public Long end_week_views;
    public String video_id;
    public Long total_views;

    public WeekCategoryVideoDescription(
            String start_date_, String end_date_,
            Integer category_id_, String category_name_,
            Date start_trending_date_, Date end_trending_date_,
            Long start_week_views_, Long end_week_views_,
            String video_id_
    ) {
        start_date = start_date_;
        end_date = end_date_;
        category_id = category_id_;
        category_name = category_name_;
        start_trending_date = start_trending_date_;
        end_trending_date = end_trending_date_;
        start_week_views = start_week_views_;
        end_week_views = end_week_views_;
        video_id = video_id_;
    }

    public WeekCategoryVideoDescription merge(WeekCategoryVideoDescription desc) {

        if (desc.start_trending_date.before(start_trending_date)) {
            start_trending_date = desc.start_trending_date;
            start_week_views = desc.start_week_views;
        }

        if (end_trending_date.before(desc.end_trending_date)) {
            end_trending_date = desc.end_trending_date;
            end_week_views = desc.end_week_views;
        }

        return this;
    }

    public boolean isOneDayDescription() {
        return start_trending_date.before(end_trending_date);
    }

    public void setTotalViews() {
        total_views = end_week_views - start_week_views;
    }
}
