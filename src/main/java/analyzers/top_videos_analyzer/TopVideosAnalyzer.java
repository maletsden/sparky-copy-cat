package analyzers.top_videos_analyzer;

import analyzers.helpers.ObjectToJSONMapper;
import analyzers.top_videos_analyzer.data_types.TrendingDate;
import analyzers.top_videos_analyzer.data_types.VideoDescription;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import java.util.stream.Collectors;

public final class TopVideosAnalyzer {

    public static String[] getTopVideos(Dataset<Row> videos, Integer top_videos_num) {
        return videos.javaRDD()
                .mapToPair(TopVideosAnalyzer::keyVideoDescriptionMapper)
                .reduceByKey(VideoDescription::merge)
                .map(pair -> pair._2.normalizeDescription())

                // VideoDescription implements Comparable
                .top(top_videos_num)

                .stream()
                .map(ObjectToJSONMapper::objToJSON)
                .collect(Collectors.toList())
                .stream()
                .toArray(String[]::new);
    }

    private static Tuple2<String, VideoDescription> keyVideoDescriptionMapper(Row row) {
        return new Tuple2<>(
                row.getAs("video_id"),
                new VideoDescription(
                        row.getAs("video_id"),
                        row.getAs("title"),
                        row.getAs("views"),
                        row.getAs("likes"),
                        row.getAs("dislikes"),
                        new TrendingDate(
                                row.getAs("trending_date"),
                                row.getAs("views"),
                                row.getAs("likes"),
                                row.getAs("dislikes")
                        )
                )
        );
    }
}
