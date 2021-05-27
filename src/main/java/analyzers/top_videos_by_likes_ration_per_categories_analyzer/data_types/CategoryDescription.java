package analyzers.top_videos_by_likes_ration_per_categories_analyzer.data_types;

import scala.Serializable;
import java.util.*;

public final class CategoryDescription implements Serializable {
    public final Integer category_id;
    public final String category_name;
    public List<VideoDescription> videos;

    public CategoryDescription(
            Integer category_id_, String category_name_, VideoDescription video
    ) {
        category_id = category_id_;
        category_name = category_name_;
        videos = new ArrayList<>(Arrays.asList(video));
    }


    public CategoryDescription mergeOneVideoDescription(
            CategoryDescription desc
    ) {
        videos.get(0).merge(desc.videos.get(0));

        return this;
    }

    public VideoDescription getFirstVideo() {
        return videos.get(0);
    }

    public CategoryDescription merge(
            CategoryDescription desc
    ) {
        videos.addAll(desc.videos);

        return this;
    }

    public CategoryDescription saveOnlyTopVideos(
            Integer top_videos_num
    ) {
        videos.sort(Comparator.reverseOrder());

        videos = videos.subList(0, Math.min(videos.size(), top_videos_num));

        return this;
    }
}
