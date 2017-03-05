import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Driver extends Configured implements Tool {
    //String outPutDirectory = "home/output";
    //String inputDirectory = "/home/exia/IdeaProjects/a3/a3-dataset/";
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.printf("usage: %s needs three arguments, input and output files\n", getClass().getSimpleName());
            return -1;
        }

        Job movieJob = new Job();
        movieJob.setJarByClass(Driver.class);
        movieJob.setJobName("Netflix Movie Job");

        FileInputFormat.addInputPath(movieJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(movieJob, new Path(args[1]+"/Movie_Output"));

        movieJob.setOutputKeyClass(Text.class);
        movieJob.setOutputValueClass(FloatWritable.class);
        movieJob.setOutputFormatClass(TextOutputFormat.class);
        movieJob.setMapperClass(MovieMapper.class);
        movieJob.setReducerClass(MovieReducer.class);
        int movies_status = movieJob.waitForCompletion(true) ? 0 : 1;

        if (!movieJob.isSuccessful()) {
            return -1;
        }
        Job movieJobSorter = new Job();
        movieJobSorter.setJarByClass(Driver.class);
        movieJobSorter.setJobName("Movie Sorter");
        FileInputFormat.addInputPath(movieJobSorter, new Path(args[1]+"/Movie_Output/part-r-00000"));
        FileOutputFormat.setOutputPath(movieJobSorter, new Path(args[1] + "/SortedMovie_Output"));
        movieJobSorter.setSortComparatorClass(FloatCompare.class);
        movieJobSorter.setOutputKeyClass(FloatWritable.class);
        movieJobSorter.setOutputValueClass(IntWritable.class);
        movieJobSorter.setOutputFormatClass(TextOutputFormat.class);
        movieJobSorter.setMapperClass(MovieSorter.class);

        if(! movieJobSorter.waitForCompletion(true)){
            return -1;
        }

        System.out.print("user job");
        Job userJob = new Job();
        userJob.setJarByClass(Driver.class);
        userJob.setJobName("User Job");
        FileInputFormat.addInputPath(userJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(userJob, new Path(args[1] + "/User_Output"));
        userJob.setOutputKeyClass(Text.class);
        userJob.setOutputValueClass(IntWritable.class);
        userJob.setOutputFormatClass(TextOutputFormat.class);
        userJob.setMapperClass(UserMapper.class);
        userJob.setReducerClass(UserReducer.class);

        if( ! userJob.waitForCompletion(true)){
            return -1;
        }

        System.out.println("user job finished");

        Job userJobSorter = new Job();
        userJobSorter.setJarByClass(Driver.class);
        userJobSorter.setJobName("User Sorter");
        FileInputFormat.addInputPath(userJobSorter, new Path( args[1] + "/User_Output/part-r-00000"));
        FileOutputFormat.setOutputPath(userJobSorter, new Path(args[1] + "/SortedUser_Output"));
        userJobSorter.setSortComparatorClass(IntCompare.class);
        userJobSorter.setOutputKeyClass(IntWritable.class);
        userJobSorter.setOutputValueClass(IntWritable.class);
        userJobSorter.setOutputFormatClass(TextOutputFormat.class);
        userJobSorter.setMapperClass(UserSorter.class);

        if( ! userJobSorter.waitForCompletion(true)){
            return -1;
        }


        return 0;
    }
}