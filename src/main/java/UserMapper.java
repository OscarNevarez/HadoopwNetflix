import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class UserMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text nKey = new Text();
    private IntWritable increment = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] output = value.toString().split(",");

        if (output.length == 3){
            nKey.set(output[1]);
        } else{
            return;
        }

        context.write(nKey, increment);
    }
}
