package analyzers.top_channels_by_views_analyzer;

import analyzers.helpers.ObjectToJSONMapper;
import analyzers.top_channels_by_views_analyzer.data_types.ChannelDescription;
import analyzers.top_channels_by_views_analyzer.data_types.VideoStats;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import java.util.stream.Collectors;

public final class TopChannelsByViewsAnalyzer {
    public static String[] getTopChannels(
            Dataset<Row> videos, Integer channels_num
    ) {
        return videos.javaRDD()
                .mapToPair(TopChannelsByViewsAnalyzer::rowToChannelVideoMapper)
                .reduceByKey(ChannelDescription::mergeOneVideoChannel)

                .mapToPair(pair -> new Tuple2<>(pair._1._1, pair._2))
                .reduceByKey(ChannelDescription::merge)

                .map(pair -> pair._2)

                // ChannelDescription implements Comparable
                .top(channels_num)

                .stream()
                .map(ObjectToJSONMapper::objToJSON)
                .collect(Collectors.toList())
                .stream()
                .toArray(String[]::new);
    }

    private static Tuple2<Tuple2<String, String>, ChannelDescription> rowToChannelVideoMapper(
            Row row
    ) {
        return new Tuple2<>(
                new Tuple2<>(
                        row.getAs("channel_title"),
                        row.getAs("video_id")
                ),
                new ChannelDescription(
                        row.getAs("channel_title"),
                        row.getAs("trending_date"),
                        row.getAs("trending_date"),
                        new VideoStats(
                                row.getAs("video_id"),
                                row.getAs("views")
                        )
                )
        );
    }
}
