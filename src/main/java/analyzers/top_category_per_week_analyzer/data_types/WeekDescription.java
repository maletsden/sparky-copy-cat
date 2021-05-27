package analyzers.top_category_per_week_analyzer.data_types;

import scala.Serializable;

import java.util.List;

public class WeekDescription implements Serializable {
    public String start_date;
    public String end_date;
    public Integer category_id;
    public String category_name;
    public Long number_of_videos;
    public Long total_views;
    public List<String> video_ids;

    public WeekDescription(
            WeekCategoryDescription desc
    ) {
        start_date = desc.start_date;
        end_date = desc.end_date;
        category_id = desc.category_id;
        category_name = desc.category_name;
        number_of_videos = (long) desc.video_ids.size();
        total_views = desc.total_views;
        video_ids = desc.video_ids;
    }

    public WeekDescription mostPopularCategory(WeekDescription desc) {
        return total_views >= desc.total_views ? this : desc;
    }
}
