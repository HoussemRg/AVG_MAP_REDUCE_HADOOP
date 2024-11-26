package tn.enit.tp1;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class Combiner extends Reducer<Text, NumPair, Text, NumPair>  {
    public void reduce(Text key, Iterable<NumPair> values, Context context) throws IOException, InterruptedException {
        int totalHours = 0;
        int totalCount = 0;

        for (NumPair pair : values) {
            totalHours += pair.getSum();
            totalCount += pair.getCount();
        }

        context.write(key, new NumPair(totalHours, totalCount));
    }
}
