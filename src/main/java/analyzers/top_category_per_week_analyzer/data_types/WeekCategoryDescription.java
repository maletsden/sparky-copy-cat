package analyzers.top_category_per_week_analyzer.data_types;

import scala.Serializable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WeekCategoryDescription implements Serializable {
    public String start_date;
    public String end_date;
    public Integer category_id;
    public String category_name;
    public Long total_views;
    public List<String> video_ids;

    public WeekCategoryDescription(
            WeekCategoryVideoDescription desc
    ) {
        start_date = desc.start_date;
        end_date = desc.end_date;
        category_id = desc.category_id;
        category_name = desc.category_name;
        total_views = desc.total_views;
        video_ids = new ArrayList<>(Arrays.asList(desc.video_id));
    }

    public WeekCategoryDescription merge(WeekCategoryDescription desc) {

        total_views += desc.total_views;
        video_ids.addAll(desc.video_ids);

        return this;
    }
}
