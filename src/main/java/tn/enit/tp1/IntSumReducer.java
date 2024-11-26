package tn.enit.tp1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IntSumReducer
        extends Reducer<Text, NumPair, Text, Text> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<NumPair> values, Context context
    ) throws IOException, InterruptedException {
        int totalHours = 0;
        int totalCount = 0;

        for (NumPair pair : values) {
            totalHours += pair.getSum();
            totalCount += pair.getCount();
        }

        double average = totalCount == 0 ? 0 : (double) totalHours / totalCount;
        context.write(key, new Text(String.format("%.2f", average)));
    }
}
