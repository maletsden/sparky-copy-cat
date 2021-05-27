package analyzers.top_videos_by_likes_ration_per_categories_analyzer;

import analyzers.helpers.ObjectToJSONMapper;
import analyzers.top_videos_by_likes_ration_per_categories_analyzer.data_types.CategoryDescription;
import analyzers.top_videos_by_likes_ration_per_categories_analyzer.data_types.VideoDescription;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public final class TopVideosByLikesRationPerCategoriesAnalyzer {
    public static String[] getVideos(
            Dataset<Row> videos_with_categories, Integer top_videos_num
    ) {
        return videos_with_categories.javaRDD()
                .mapToPair(TopVideosByLikesRationPerCategoriesAnalyzer::rowToCategoryVideoMapper)
                .filter(pair -> {
                    VideoDescription video = pair._2.getFirstVideo();

                    return video.views > 100_000;
                })

                .reduceByKey(CategoryDescription::mergeOneVideoDescription)

                .mapToPair(pair -> new Tuple2<>(pair._1._1, pair._2))
                .reduceByKey(CategoryDescription::merge)

                .map(pair -> pair._2.saveOnlyTopVideos(top_videos_num))

                .map(ObjectToJSONMapper::objToJSON)
                .collect()
                .stream()
                .toArray(String[]::new);
    }

    public static Tuple2<Tuple2<Integer, String>, CategoryDescription> rowToCategoryVideoMapper(
            Row row
    ) {
        return new Tuple2<>(
                new Tuple2<>(
                        row.getAs("category_id"),
                        row.getAs("video_id")
                ),
                new CategoryDescription(
                        row.getAs("category_id"),
                        ((Row) row.getAs("snippet")).getAs("title"),
                        new VideoDescription(
                                row.getAs("video_id"),
                                row.getAs("title"),
                                row.getAs("likes"),
                                row.getAs("dislikes"),
                                row.getAs("views")
                        )
                )
        );
    }
}
