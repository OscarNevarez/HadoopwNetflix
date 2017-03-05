import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class MovieSorter extends Mapper<LongWritable, Text, FloatWritable, IntWritable>{


    private IntWritable nValue = new IntWritable();
    private FloatWritable nKey = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\t");

        nKey.set(Float.parseFloat(values[1]));
        nValue.set(Integer.parseInt(values[0]));

        context.write(nKey, nValue);
    }
}