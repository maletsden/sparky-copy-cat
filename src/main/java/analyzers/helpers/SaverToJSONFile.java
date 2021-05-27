package analyzers.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

public final class SaverToJSONFile {
    public static boolean saveToJSONFile(String filename, String[] results, String property_field_name) {
        try (FileWriter file_out = new FileWriter(filename)) {

            file_out.write("{\r\n\t\"" + property_field_name + "\": [\r\n");
            for (int i = 0; i < results.length - 1; ++i) {
                file_out.append(results[i]);
                file_out.append(",\r\n");
            }
            file_out.append(results[results.length - 1]);
            file_out.append("\r\n]\r\n}");

            return true;
        } catch (IOException e) {
            System.out.println("Top10VideosAnalyzer.saveToJSONFile:: ERROR: failed to save results.");
            e.printStackTrace();
        }

        return false;
    }

    public static boolean saveToGCSJSONFile(
            String gcsBucketUri, String filePath, String[] results, String property_field_name
    ) {

        try {
            Configuration configuration = new Configuration();
            FileSystem hdfs = FileSystem.get(new URI(gcsBucketUri), configuration);

            Path file = new Path(filePath);

            if (hdfs.exists(file)) {
                hdfs.delete(file, true);
            }

            OutputStream outputStream = hdfs.create(file);
            BufferedWriter bufferedWriter  = new BufferedWriter(
                    new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)
            );

            bufferedWriter.write("{\r\n\t\"" + property_field_name + "\": [\r\n");
            for (int i = 0; i < results.length - 1; ++i) {
                bufferedWriter.append(results[i]);
                bufferedWriter.append(",\r\n");
            }
            bufferedWriter.append(results[results.length - 1]);
            bufferedWriter.append("\r\n]\r\n}");

            bufferedWriter.close();
            hdfs.close();

            return true;
        } catch (URISyntaxException | IOException e) {
            e.printStackTrace();
            System.out.println("Failed to save file.");
        }


        return false;
    }

    public static long getFileSize(String gcsBucketUri, String filePath) {
        try {
            Configuration configuration = new Configuration();
            FileSystem hdfs = FileSystem.get(new URI(gcsBucketUri), configuration);

            Path file = new Path(filePath);
            ContentSummary summary = hdfs.getContentSummary(file);

            return summary.getLength();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }

        return -1;
    }

}
