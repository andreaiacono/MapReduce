package samples.mean;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: andrea
 * Date: 4/6/14
 * Time: 2:39 PM
 */

/**
 * SumCount is a utility wrapper around two values for computing the mean of a dataset.
 * The two values are:
 * - the sum of the values
 * - the number of values summed
 * so that dividing the first by the second will give us the mean.
 */
public class SumCount implements WritableComparable<SumCount> {

    DoubleWritable sum;
    IntWritable count;

    public SumCount() {
        set(new DoubleWritable(0), new IntWritable(0));
    }

    public SumCount(Double sum, Integer count) {
        set(new DoubleWritable(sum), new IntWritable(count));
    }

    public void set(DoubleWritable sum, IntWritable count) {
        this.sum = sum;
        this.count = count;
    }

    public DoubleWritable getSum() {
        return sum;
    }

    public IntWritable getCount() {
        return count;
    }

    public void addSumCount(SumCount sumCount) {
        set(new DoubleWritable(this.sum.get() + sumCount.getSum().get()), new IntWritable(this.count.get() + sumCount.getCount().get()));
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        sum.write(dataOutput);
        count.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        sum.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public int compareTo(SumCount sumCount) {

        // compares the first of the two values
        int comparison = sum.compareTo(sumCount.sum);

         // if they're not equal, return the value of compareTo between the "sum" value
        if (comparison != 0) {
            return comparison;
        }

        // else return the value of compareTo between the "count" value
        return count.compareTo(sumCount.count);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SumCount sumCount = (SumCount) o;

        return count.equals(sumCount.count) && sum.equals(sumCount.sum);
    }

    @Override
    public int hashCode() {
        int result = sum.hashCode();
        result = 31 * result + count.hashCode();
        return result;
    }
}
