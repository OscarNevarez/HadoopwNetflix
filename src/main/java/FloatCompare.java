import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FloatCompare extends WritableComparator {

    protected FloatCompare()
    {
        super(FloatWritable.class, true);
    }

    @SuppressWarnings("rawtypes")

    @Override
    public int compare(WritableComparable value1, WritableComparable value2) {
        FloatWritable float_value1 = (FloatWritable)value1;
        FloatWritable float_value2 = (FloatWritable)value2;

        return -1 * float_value1.compareTo(float_value2);
    }
}
