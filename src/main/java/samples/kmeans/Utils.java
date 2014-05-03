package samples.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 03/05/14
 * Time: 0.09
 */
public class Utils {

    public static List<Double[]> readCentroidsFromMapper(Mapper.Context context) throws IOException {

        URI[] cacheFiles = context.getCacheFiles();
        FileInputStream fis = new FileInputStream(cacheFiles[0].toString());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        String line;
        List<Double[]> centroids = new ArrayList<>();
        try {
            while ((line = reader.readLine()) != null) {
                String[] temp = line.split(" ");
                Double[] centroid = new Double[2];
                centroid[0] = Double.parseDouble(temp[0]);
                centroid[1] = Double.parseDouble(temp[1]);
                centroids.add(centroid);
            }
        }
        finally {
            reader.close();
        }
        return centroids;

    }

    public static String getFormattedCentroids(List<Double[]> centroids, int centroidsNumber) {

        StringBuilder centroidsBuilder = new StringBuilder();
        for (Double[] centroid : centroids) {
            centroidsBuilder.append(centroid[0].toString());
            centroidsBuilder.append(" ");
            centroidsBuilder.append(centroid[1].toString());
            centroidsBuilder.append(centroid[1].toString());
            centroidsBuilder.append("\t");
            centroidsBuilder.append("" + (int) (Math.random() * (centroidsNumber - 1)));
            centroidsBuilder.append("\n");
        }

        return centroidsBuilder.toString();
    }

    public static void writeCentroids(Configuration configuration, String formattedCentroids) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream fin = fs.create(new Path(Constants.CENTROIDS_FILE));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
        bw.append(formattedCentroids);
        bw.close();
    }

    public static List<Double[]> createRandomCentroids(int centroidsNumber) {

        List<Double[]> centroids = new ArrayList<>();

        // computes randomly centroids
        for (int j = 0; j < centroidsNumber; j++) {
            Double[] centroid = new Double[2];
            centroid[0] = Math.random() * 5;
            centroid[1] = Math.random() * 5;
            centroids.add(centroid);
        }

        return centroids;
    }


    public static double euclideanDistance(double x1, double y1, double x2, double y2) {
        return Math.sqrt(Math.pow(x1 - y1, 2) + Math.pow(x2 - y2, 2));
    }

    public static String readCentroidsFromHDFS(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream d = new FSDataInputStream(fs.open(new Path(Constants.CENTROIDS_FILE)));
        int centroidsNumber = configuration.getInt(Constants.CENTROID_NUMBER_ARG, 0);
        List<Double[]> centroids = new ArrayList<>();
        for (int j = 0; j < centroidsNumber; j++) {
            Double[] centroid = new Double[2];
            centroid[0] = d.readDouble();
            centroid[1] = d.readDouble();
            centroids.add(centroid);
        }

        return getFormattedCentroids(centroids, centroidsNumber);
    }

//    public static void writeCentroidsHDFS(Configuration configuration, List<Double[]> centroids) throws IOException {
//
//        FileSystem fs = FileSystem.get(configuration);
//        FSDataOutputStream fin = fs.create(new Path(Constants.CENTROIDS_FILE));
//        try {
//            for (Double[] centroid : centroids) {
//                fin.writeDouble(centroid[0]);
//                fin.writeDouble(centroid[1]);
//            }
//        }
//        finally {
//            if (fin != null) {
//                fin.close();
//            }
//        }
//    }
//
}
