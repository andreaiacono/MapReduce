package samples.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 24/04/14
 * Time: 13.58
 */
public class KMeans {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: KMeans <in> <out> <clusters_number>");
            System.exit(2);
        }

        configuration.set(Constants.INPUT_FILE_ARG, otherArgs[0]);

        int centroidsNumber = Integer.parseInt(otherArgs[2]);
        configuration.setInt(Constants.CENTROID_NUMBER_ARG, centroidsNumber);

        // creates random centroids
        List<Double[]> centroids = Utils.createRandomCentroids(centroidsNumber);
        String centroidsFile = Utils.getFormattedCentroids(centroids);

        // writes centroids on distributed cache
        Utils.writeCentroids(configuration, centroidsFile);
//        List<Double[]> c = Utils.readCentroids(Constants.CENTROIDS_FILE);
        boolean hasConverged = false;
        int iteration = 0;
        do {

            configuration.set(Constants.OUTPUT_FILE_ARG, otherArgs[1] + "-" + iteration);

            // executes hadoop job
            if (!launchJob(configuration, iteration++)) {

                // if an error has occurred stops iteration and terminates
                System.exit(1);
            }

            // reads reducer output file
            String newCentroids = Utils.readReducerOutput(configuration);
//            if (true) throw new IOException("reducer=[" + newCentroids + "]");

            // if the output of the reducer equals the old one
            if (centroidsFile.equals(newCentroids)) {

                // it means that the iteration is finished
                hasConverged = true;
            }
            else {

                // writes the reducers output to distributed cache
                Utils.writeCentroids(configuration, newCentroids);
            }

            centroidsFile = newCentroids;

        } while (!hasConverged);
    }


    /**
     * executes the job
     *
     * @return true if the job has converged, else false
     */
    private static boolean launchJob(Configuration configuration, int iteration) throws Exception {

        Job job = Job.getInstance(configuration);
        job.setJobName("KMeans");
        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.addCacheFile(new Path(Constants.CENTROIDS_FILE).toUri());

        FileInputFormat.addInputPath(job, new Path(configuration.get(Constants.INPUT_FILE_ARG)));
        FileOutputFormat.setOutputPath(job, new Path(configuration.get(Constants.OUTPUT_FILE_ARG)));

        return job.waitForCompletion(true);
    }

}
