package samples.mean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 3/7/14
 * Time: 9:19 AM
 */

/**
 * this mapreduce job computes the mean of the temperature of the month for every year
 */
public class Mean {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Mean <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Mean");
        job.setJarByClass(Mean.class);
        job.setMapperClass(MeanMapper.class);
        job.setReducerClass(MeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * The mapper reads one line at the time, splits it by a comma (data come from CSV file)
     */
    public static class MeanMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final int DATE = 0;
        private final int MIN = 1;
        private final int MAX = 2;

        private Map<String, List<Double>> maxMap = new HashMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // gets the fields of the CSV line
            String[] values = value.toString().split((","));

            // defensive check
            if (values.length != 3) {
                return;
            }

            // gets date and max temperature
            String date = values[DATE];
            String month = date.substring(3);
            Double max = Double.parseDouble(values[MAX]);

            if (!maxMap.containsKey(month)) {
                maxMap.put(month, new ArrayList<Double>());
            }

            // adds the max temperature for this row to the list of temperatures
            maxMap.get(month).add(max);
        }

        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {

            for (String month: maxMap.keySet()) {
                Double sum = 0d;
                List<Double> temperatures = maxMap.get(month);
                for (Double max: temperatures) {
                    sum += max;
                }
                context.write(new Text(month), new SumCount(sum, temperatures.size()));
            }
        }
    }

    /**
     *
     */
    public static class MeanReducer extends Reducer<Text, SumCount, Text, DoubleWritable> {

        private Map<Text, SumCount> sumCountMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {

            SumCount totalSumCount = new SumCount();

            for (SumCount sumCount : values) {
                totalSumCount.addSumCount(sumCount);
            }

            // puts the number of occurrences of this word into the map.
            // We need to create another Text object because the Text instance
            // we receive is the same for all the words
            sumCountMap.put(new Text(key), totalSumCount);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Text month: sumCountMap.keySet()) {
                double sum = sumCountMap.get(month).getSum().get();
                int count = sumCountMap.get(month).getCount().get();
                context.write(month, new DoubleWritable(sum/count));
            }
        }
    }

}
