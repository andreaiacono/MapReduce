package samples.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 03/05/14
 * Time: 0.19
 */
public class KMeansReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Double mx = 0d;
        Double my = 0d;
        int counter = 0;

        for (Text value: values) {
            String[] temp = value.toString().split(" ");
            mx += Double.parseDouble(temp[0]);
            my += Double.parseDouble(temp[1]);
            counter ++;
        }

        mx = mx / counter;
        my = my / counter;
        String centroid = mx + " " + my;

        context.write(new Text(centroid), key);
    }

}
