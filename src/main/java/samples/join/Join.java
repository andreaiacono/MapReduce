package samples.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 13/04/14
 * Time: 16.15
 */
public class Join {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Join <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Join");
        job.setJarByClass(Join.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class JoinMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // gets filename of the input file for this record
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            String[] fields = value.toString().split(("\t"));

            if (filename.equals("forum_nodes_no_lf.tsv")) {

                if (fields.length > 5) {
                    
                    // skips the header row
                    if (!fields[0].equals("\"id\"")) {
                        String authorId = fields[3].substring(1, fields[3].length() - 1);
                        String type = fields[5].substring(1, fields[5].length() - 1);
                        if (type.equals("question")) {
                            context.write(new Text(authorId), one);
                        }
                    }
                }
            }
            else {

                // skips the header row
                if (!fields[0].equals("\"user_ptr_id\"")) {
                    String authorId = fields[0].substring(1, fields[0].length() - 1);
                    String reputation = fields[1].substring(1, fields[1].length() - 1);
                    try {
                        // we add two to the reputation, because we want the minimum value to be greater than 1,
                        // not to be confused with the "one" passed by the other branch of the if
                        int reputationValue = Integer.parseInt(reputation) + 2;
                        context.write(new Text(authorId), new IntWritable(reputationValue));
                    }
                    catch (NumberFormatException nfe) {
                        // just skips this record
                    }
                }
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, IntWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int postsNumber = 0;
            int reputation = 0;
            String authorId = key.toString();

            for (IntWritable value : values) {

                int intValue = value.get();
                if (intValue == 1) {
                    postsNumber ++;
                }
                else {
                    // we subtract two because of the comment on lines 80/81
                    reputation = intValue -2;
                }
            }

            context.write(new Text(authorId), new Text(reputation + "\t" + postsNumber));
        }
    }
}