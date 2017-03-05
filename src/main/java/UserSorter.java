import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class UserSorter extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable nKey = new IntWritable();
    private IntWritable nValue = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] output =  value.toString().split("\t");

        nKey.set(Integer.parseInt(output[1]));
        nValue.set(Integer.parseInt(output[0]));
        context.write(nKey, nValue);
    }
}