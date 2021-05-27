package analyzers.top_channels_by_days_analyzer;

import analyzers.helpers.ObjectToJSONMapper;
import analyzers.top_channels_by_days_analyzer.data_types.ChannelDescription;
import analyzers.top_channels_by_days_analyzer.data_types.VideoDayDescription;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.stream.Collectors;

public final class TopChannelsByDaysAnalyzer {

    public static String[] getTopChannels(
            Dataset<Row> videos, Integer top_channels_num
    ) {
        return videos.javaRDD()
                .mapToPair(TopChannelsByDaysAnalyzer::rowToChannelVideoMapper)
                .reduceByKey(ChannelDescription::mergeOneVideoChannels)

                .mapToPair(pair -> new Tuple2<>(pair._1._1, pair._2))
                .reduceByKey(ChannelDescription::merge)
                .map(pair -> pair._2)

                .top(top_channels_num)

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
                  new VideoDayDescription(
                          row.getAs("video_id"),
                          row.getAs("title")
                  )
          )
        );
    }
}
