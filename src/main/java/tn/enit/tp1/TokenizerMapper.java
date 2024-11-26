package tn.enit.tp1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, NumPair>{

    private Text maritalStatus = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length >= 14) {
            String status = fields[5].trim(); // Marital status
            int hours = Integer.parseInt(fields[12].trim()); // Hours-per-week

            maritalStatus.set(status);
            context.write(maritalStatus, new NumPair(hours, 1));
        }
    }
}
