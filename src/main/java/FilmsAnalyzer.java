import analyzers.top_category_per_week_analyzer.TopCategoryPerWeekAnalyzer;
import analyzers.top_channels_by_days_analyzer.TopChannelsByDaysAnalyzer;
import analyzers.top_channels_by_views_analyzer.TopChannelsByViewsAnalyzer;
import analyzers.top_tags_per_month_analyzer.TopTagsPerMonthAnalyzer;
import analyzers.helpers.SaverToJSONFile;
import analyzers.top_videos_analyzer.TopVideosAnalyzer;
import analyzers.top_videos_by_likes_ration_per_categories_analyzer.TopVideosByLikesRationPerCategoriesAnalyzer;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class FilmsAnalyzer {

    public static void main(String[] args) {
        if (args.length != 4) {
            throw new IllegalArgumentException("Exactly 4 arguments are required: " +
                    "<gcsBucketUri> <inputDirectoryUri> <outputDirectoryUri> " +
                    "<countryCodesForAnalyzingSeparatedByComma>");
        }

        String gcsBucketUri = args[0];
        if (!gcsBucketUri.endsWith("/")) gcsBucketUri += "/";
        String inputDirectory = args[1];
        if (!inputDirectory.endsWith("/")) inputDirectory += "/";
        String outputDirectory = args[2];
        if (!outputDirectory.endsWith("/")) outputDirectory += "/";
        String[] countryCodesForAnalyzing = args[3].split(",");


        SparkSession sparkSession = SparkSession.builder()
                .appName("Copy Cat")
                .getOrCreate();


        for (String code : countryCodesForAnalyzing) {
            String videosPath = gcsBucketUri + inputDirectory + code + "videos.csv";
            String categoriesPath = gcsBucketUri + inputDirectory + code + "_category_id.json";

            System.out.println("Started analyzing country with code - " + code);

            Dataset<Row> videos_df = sparkSession.read()
                    .option("header", "true")
                    .option("wholeFile", "true")
                    .option("multiline", "true")
                    .csv(videosPath.toString())
                    .withColumn("category_id", col("category_id").cast("int"))
                    .withColumn("views", col("views").cast("long"))
                    .withColumn("likes", col("likes").cast("long"))
                    .withColumn("dislikes", col("dislikes").cast("long"));

            Dataset<Row> categories_nested_df = sparkSession.read()
                    .option("header", "true")
                    .option("wholeFile", "true")
                    .option("multiline", "true")
                    .json(categoriesPath.toString());

            Dataset<Row> categories_df = categories_nested_df
                    .select(functions.explode(categories_nested_df.col("items")).as("categories"))
                    .select("categories.*");

            Dataset<Row> videos_with_categories_df = videos_df.join(
                    categories_df,
                    videos_df.col("category_id").equalTo(categories_df.col("id"))
            );


            {
                long start = System.currentTimeMillis();
                final Integer top_videos_num = 10;
                String[] results = TopVideosAnalyzer.getTopVideos(videos_df, top_videos_num);
                String filePath = outputDirectory + "1/" + code + "/result.json";
                saveResults(
                        gcsBucketUri, filePath, results,
                        "videos", start, "TopVideosAnalyzer"
                );
            }

            {
                long start = System.currentTimeMillis();
                String[] results = TopCategoryPerWeekAnalyzer.getTopCategoryPerWeek(videos_with_categories_df);
                String filePath = outputDirectory + "2/" + code + "/result.json";
                saveResults(
                        gcsBucketUri, filePath, results,
                        "weeks", start, "TopCategoryPerWeekAnalyzer"
                );
            }

            {
                long start = System.currentTimeMillis();
                String[] results = TopTagsPerMonthAnalyzer.getTopTagsPerMonth(videos_df);
                String filePath = outputDirectory + "3/" + code + "/result.json";
                saveResults(
                        gcsBucketUri, filePath, results,
                        "months", start, "TopTagsPerMonthAnalyzer"
                );
            }
            {
                long start = System.currentTimeMillis();
                final Integer top_channels_num = 20;
                String[] results = TopChannelsByViewsAnalyzer.getTopChannels(videos_df, top_channels_num);
                String filePath = outputDirectory + "4/" + code + "/result.json";
                saveResults(
                        gcsBucketUri, filePath, results,
                        "channels", start, "TopChannelsByViewsAnalyzer"
                );
            }
            {
                long start = System.currentTimeMillis();
                final Integer top_channels_by_days_num = 20;
                String[] results = TopChannelsByDaysAnalyzer.getTopChannels(videos_df, top_channels_by_days_num);
                String filePath = outputDirectory + "5/" + code + "/result.json";
                saveResults(
                        gcsBucketUri, filePath, results,
                        "channels", start, "TopChannelsByDaysAnalyzer"
                );
            }
            {
                long start = System.currentTimeMillis();
                final Integer top_videos_in_category = 10;
                String[] results = TopVideosByLikesRationPerCategoriesAnalyzer.getVideos(
                        videos_with_categories_df, top_videos_in_category
                );
                String filePath = outputDirectory + "6/" + code + "/result.json";
                saveResults(
                        gcsBucketUri, filePath, results,
                        "categories", start, "TopVideosByLikesRationPerCategoriesAnalyzer"
                );
            }

            System.out.println("Finished analyzing country with code - " + code);

        }

        sparkSession.close();
    }

    private static void saveResults(
            String gcsBucketUri, String filePath, String[] results, String fieldName, long start, String analyzerName
    ) {
        SaverToJSONFile.saveToGCSJSONFile(gcsBucketUri, filePath, results, fieldName);
        long end = System.currentTimeMillis();

        System.out.println(analyzerName + " successfully finished (" +
                "time consumed - " + (end - start) + " milliseconds, " +
                "result file size - " + SaverToJSONFile.getFileSize(gcsBucketUri, filePath) +
                ").");
    }
}
