package analyzers.top_category_per_week_analyzer;

import analyzers.helpers.DatesAnalyzer;
import analyzers.helpers.ObjectToJSONMapper;
import analyzers.top_category_per_week_analyzer.data_types.WeekCategoryDescription;
import analyzers.top_category_per_week_analyzer.data_types.WeekCategoryVideoDescription;
import analyzers.top_category_per_week_analyzer.data_types.WeekDescription;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Tuple2;
import scala.Tuple3;
import java.util.*;

public final class TopCategoryPerWeekAnalyzer {

    public static String[] getTopCategoryPerWeek(Dataset<Row> videos_with_categories) {
        // to unsure that we use the same today date within the whole analyzing process
        Date today = new Date();

        return videos_with_categories.javaRDD()

                .mapToPair(row -> rowToWeekCategoryVideoMapper(row, today))
                .reduceByKey(WeekCategoryVideoDescription::merge)
                .filter(pair -> pair._2.isOneDayDescription())
                .map(pair -> {
                    pair._2.setTotalViews();
                    return pair;
                })

                .mapToPair(TopCategoryPerWeekAnalyzer::weekCategoryVideoToWeekCategoryMapper)
                .reduceByKey(WeekCategoryDescription::merge)


                .mapToPair(TopCategoryPerWeekAnalyzer::weekCategoryToWeekMapper)
                .reduceByKey(WeekDescription::mostPopularCategory)

                .map(ObjectToJSONMapper::tuple2ToJSON)
                .collect()
                .stream()
                .toArray(String[]::new);
    }

    private static Tuple2<Tuple3<Long, Integer, String>, WeekCategoryVideoDescription> rowToWeekCategoryVideoMapper(
            Row row, Date today
    ) {
        Tuple3<Long, String, String> week = DatesAnalyzer.intervalsNumFomToday(
                today, row.getAs("trending_date"), 7
        );

        Date end_trending_date = DatesAnalyzer.dateStringToDate(
                DatesAnalyzer.SDF_YY_DD_FF, row.getAs("trending_date")
        );

        return new Tuple2<>(
                new Tuple3<>(
                        week._1(),
                        row.getAs("category_id"),
                        row.getAs("video_id")
                ),
                new WeekCategoryVideoDescription(
                        week._2(),
                        week._3(),
                        row.getAs("category_id"),
                        ((Row) row.getAs("snippet")).getAs("title"),
                        end_trending_date,
                        end_trending_date,
                        row.getAs("views"),
                        row.getAs("views"),
                        row.getAs("video_id")
                )
        );
    }

    private static Tuple2<Tuple2<Long, Integer>, WeekCategoryDescription> weekCategoryVideoToWeekCategoryMapper(
            Tuple2<Tuple3<Long, Integer, String>, WeekCategoryVideoDescription> pair
    ) {
        return new Tuple2<>(
                new Tuple2<>(pair._1._1(), pair._1._2()),
                new WeekCategoryDescription(pair._2)
        );
    }

    private static Tuple2<Long, WeekDescription> weekCategoryToWeekMapper(
            Tuple2<Tuple2<Long, Integer>, WeekCategoryDescription> pair
    ) {
        return new Tuple2<>(
                pair._1._1(),
                new WeekDescription(pair._2)
        );
    }
}
