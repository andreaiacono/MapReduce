package samples.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 03/05/14
 * Time: 0.18
 */
public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {

    public static List<Double[]> centroids;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        centroids = Utils.readCentroids(cacheFiles[0].toString());
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] xy = value.toString().split(" ");
        double x = Double.parseDouble(xy[0]);
        double y = Double.parseDouble(xy[1]);
        int index = 0;
        double minDistance = Double.MAX_VALUE;
        for (int j = 0; j < centroids.size(); j++) {
            double distance = Utils.euclideanDistance(centroids.get(j)[0], centroids.get(j)[1], x, y);
            if (distance < minDistance) {
                index = j;
                minDistance = distance;
            }
        }

        context.write(new IntWritable(index), value);
    }
}
