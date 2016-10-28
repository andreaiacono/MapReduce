package samples.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 03/05/14
 * Time: 0.09
 */
public class Utils {

    public static List<Double[]> readCentroids(String filename) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
        return  readData(reader);
    }

    public static List<Double[]> getCentroids(String content) throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(content));
        return  readData(reader);
    }

    private static List<Double[]> readData(BufferedReader reader) throws IOException {
        List<Double[]> centroids = new ArrayList<>();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                String[] values = line.split("\t");
                String[] temp = values[0].split(" ");
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

    public static String getFormattedCentroids(List<Double[]> centroids) {

        int counter = 0;
        StringBuilder centroidsBuilder = new StringBuilder();
        for (Double[] centroid : centroids) {
            centroidsBuilder.append(centroid[0].toString());
            centroidsBuilder.append(" ");
            centroidsBuilder.append(centroid[1].toString());
            centroidsBuilder.append("\t");
            centroidsBuilder.append("" + counter++);
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
            centroid[0] = Math.random() * 2;
            centroid[1] = Math.random() * 2;
            centroids.add(centroid);
        }

        return centroids;
    }


    public static double euclideanDistance(double x1, double y1, double x2, double y2) {
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }

    public static String readReducerOutput(Configuration configuration) throws IOException {
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(configuration.get(Constants.OUTPUT_FILE_ARG) + "/part-r-00000")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            content.append(line).append("\n");
        }

        return content.toString();
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
