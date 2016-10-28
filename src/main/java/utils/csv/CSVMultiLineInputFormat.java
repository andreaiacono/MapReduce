package utils.csv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Configurable CSV line reader. Variant of NLineInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column. Also
 * implements the getSplits method so splits are made by lines
 *
 *
 * @author mvallebr
 *
 */
public class CSVMultiLineInputFormat extends FileInputFormat<LongWritable, List<Text>> {

    public static final String LINES_PER_MAP = "mapreduce.input.lineinputformat.linespermap";

    public static final int DEFAULT_LINES_PER_MAP = 1;

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache
     * .hadoop.mapreduce.InputSplit,
     * org.apache.hadoop.mapreduce.TaskAttemptContext)
     */
    @Override
    public RecordReader<LongWritable, List<Text>> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        Configuration conf = context.getConfiguration();
        String quote = conf.get(CSVLineRecordReader.FORMAT_DELIMITER, CSVLineRecordReader.DEFAULT_DELIMITER);
        String separator = conf.get(CSVLineRecordReader.FORMAT_SEPARATOR, CSVLineRecordReader.DEFAULT_SEPARATOR);
        if (null == quote || null == separator) {
            throw new IOException("CSVTextInputFormat: missing parameter delimiter/separator");
        }
        context.setStatus(split.toString());
        return new CSVLineRecordReader();
    }

    /**
     * Logically splits the set of input files for the job, splits N lines of
     * the input as one split.
     *
     * @see FileInputFormat#getSplits(JobContext)
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        int numLinesPerSplit = getNumLinesPerSplit(job);
        for (FileStatus status : listStatus(job)) {
            List<FileSplit> fileSplits = getSplitsForFile(status, job.getConfiguration(), numLinesPerSplit);
            splits.addAll(fileSplits);
        }
        return splits;
    }

    /**
     *
     * Uses CSVLineRecordReader to split the file in lines
     *
     * @param status
     *            file status
     * @param conf
     *            hadoop conf
     * @param numLinesPerSplit
     *            number of lines that should exist on each split
     * @return list of file splits to be processed.
     * @throws IOException
     */
    public static List<FileSplit> getSplitsForFile(FileStatus status, Configuration conf, int numLinesPerSplit)
            throws IOException {
        List<FileSplit> splits = new ArrayList<FileSplit>();
        Path fileName = status.getPath();
        if (status.isDir()) {
            throw new IOException("Not a file: " + fileName);
        }
        FileSystem fs = fileName.getFileSystem(conf);
        CSVLineRecordReader lr = null;
        try {
            FSDataInputStream in = fs.open(fileName);
            lr = new CSVLineRecordReader(in, conf);
            List<Text> line = new ArrayList<Text>();
            int numLines = 0;
            long begin = 0;
            long length = 0;
            int num = -1;
            while ((num = lr.readLine(line)) > 0) {
                numLines++;
                length += num;
                if (numLines == numLinesPerSplit) {
                    // To make sure that each mapper gets N lines,
                    // we move back the upper split limits of each split
                    // by one character here.
                    if (begin == 0) {
                        splits.add(new FileSplit(fileName, begin, length - 1, new String[] {}));
                    } else {
                        splits.add(new FileSplit(fileName, begin, length - 1, new String[] {}));
                    }
                    begin += length;
                    length = 0;
                    numLines = 0;
                }
            }
            if (numLines != 0) {
                splits.add(new FileSplit(fileName, begin, length, new String[] {}));
            }
        } finally {
            if (lr != null) {
                lr.close();
            }
        }
        return splits;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable(org
     * .apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }

    /**
     * Set the number of lines per split
     *
     * @param job
     *            the job to modify
     * @param numLines
     *            the number of lines per split
     */
    public static void setNumLinesPerSplit(Job job, int numLines) {
        job.getConfiguration().setInt(LINES_PER_MAP, numLines);
    }

    /**
     * Get the number of lines per split
     *
     * @param job
     *            the job
     * @return the number of lines per split
     */
    public static int getNumLinesPerSplit(JobContext job) {
        return job.getConfiguration().getInt(LINES_PER_MAP, DEFAULT_LINES_PER_MAP);
    }

}