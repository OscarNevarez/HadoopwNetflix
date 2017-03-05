import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

    /*
     * With two instance variables, the map() method is called once per input
     * record, so it pays to avoid unnecessary object creation.
     */
    private Text nKey = new Text();
    private FloatWritable nValue = new FloatWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // split row up by comma (see input format above)
        String[] output = value.toString().split(",");
        // 0 index ia movie id, 2 index is the rating

        // System.out.println("KEY: " + output[0] + " value: " + output[2]);

        nKey.set(output[0]);
        nValue.set(Float.valueOf(output[2]));
        context.write(nKey, nValue);
    }

}
