package analyzers.top_tags_per_month_analyzer;

import analyzers.helpers.DatesAnalyzer;
import analyzers.helpers.ObjectToJSONMapper;
import analyzers.top_tags_per_month_analyzer.data_types.MonthDescription;
import analyzers.top_tags_per_month_analyzer.data_types.MonthHasMapDescription;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Date;

public final class TopTagsPerMonthAnalyzer {

    public static String[] getTopTagsPerMonth(Dataset<Row> videos) {
        // to unsure that we use the same today date within the whole analyzing process
        Date today = new Date();

        return videos.javaRDD()
                .mapToPair(row -> rowToMonthVideoMapper(row, today))
                // join all tags, since YouTube allows to edit tags (so we can have different tags on different days)
                .reduceByKey((desc1, desc2) -> {
                    // we can ignore duplicated for speed,
                    // since in this case duplication mean 2 different objects, but identical contents
                    desc1.tags.putAll(desc2.tags);
                    return desc1;
                })

                .mapToPair(TopTagsPerMonthAnalyzer::MonthVideoToMonthMapper)
                .reduceByKey(MonthHasMapDescription::mergeTags)

                .map(pair ->
                        ObjectToJSONMapper.objToJSON(
                                new MonthDescription(pair._2, 10)
                        )

                )
                .collect()
                .stream()
                .toArray(String[]::new);
    }

    private static Tuple2<Tuple2<Long, String>, MonthHasMapDescription> rowToMonthVideoMapper(
            Row row, Date today
    ) {
        Tuple3<Long, String, String> month = DatesAnalyzer.intervalsNumFomToday(
                today, row.getAs("trending_date"), 30
        );

        return new Tuple2<>(
                new Tuple2<>(
                        month._1(),
                        row.getAs("video_id")
                ),
                new MonthHasMapDescription(
                        month._2(),
                        month._3(),
                        row.getAs("tags"),
                        row.getAs("video_id")
                )
        );
    }

    private static Tuple2<Long, MonthHasMapDescription> MonthVideoToMonthMapper(
            Tuple2<Tuple2<Long, String>, MonthHasMapDescription> pair
    ) {
        return new Tuple2<>(pair._1._1, pair._2);
    }

}
