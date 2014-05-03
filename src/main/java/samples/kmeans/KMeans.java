package samples.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

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
        configuration.set(Constants.OUTPUT_FILE_ARG, otherArgs[1]);

        int centroidsNumber = Integer.parseInt(otherArgs[2]);
        configuration.setInt(Constants.CENTROID_NUMBER_ARG, centroidsNumber);

        // writes centroids on distributed cache
        String centroids = Utils.getFormattedCentroids(Utils.createRandomCentroids(centroidsNumber), centroidsNumber);
        Utils.writeCentroids(configuration, centroids);

        boolean hasConverged = false;
        do {

            // executes hadoop job
            launchJob(configuration);

            // reads reducer output file
            String newCentroids = Utils.readCentroidsFromHDFS(configuration);

            // if the output of the reducer equals the old one
            if (centroids.equals(newCentroids)) {

                // it means that the iteration is finished
                hasConverged = true;
            }
            else {

                // writes the reducers output to distributed cache
                Utils.writeCentroids(configuration, newCentroids);
            }

            centroids = newCentroids;

        } while (!hasConverged);
    }


    /**
     * executes the job
     *
     * @return true if the job has converged, else false
     */
    private static boolean launchJob(Configuration configuration) throws Exception {

        Job job = Job.getInstance(configuration);
        job.setJobName("KMeans");
        job.setJarByClass(KMeans.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(configuration.get(Constants.INPUT_FILE_ARG)));
        FileOutputFormat.setOutputPath(job, new Path(configuration.get(Constants.OUTPUT_FILE_ARG)));

        job.addCacheFile(new Path(Constants.CENTROIDS_FILE).toUri());

        return job.waitForCompletion(true);
    }

}
