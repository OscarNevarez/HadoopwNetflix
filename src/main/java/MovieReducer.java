import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class MovieReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    FloatWritable result = new FloatWritable();
    float sum;
    int count;

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {

        sum = 0;
        count = 0;

        for (FloatWritable value : values) {
            sum += value.get();
            count++;
        }

        result.set((float) sum / (float) count);
        context.write(key, result);
    }

}
