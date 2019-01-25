import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class ImageProcesser {
    private static class CustomMapper extends Mapper<Text, BytesWritable, Text, IntArrayWritable>{

        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            String[] keywords = key.toString().split(" ");
            int[] data = Utils.bytes2ints(value.copyBytes(), 2);
            System.out.println(Arrays.toString(data));
            // 默认16位uint
            int[] shape = new int[]{Integer.valueOf(keywords[1]), Integer.valueOf(keywords[2])};
            ArrayList<Record> res = Utils.all_subgraph(data, shape[0], shape[1], 2, 0);
            byte[] slice = Utils.int2bytes(Integer.valueOf(keywords[0]), 16);
            for(Record r : res){
                int[] newvalue = new int[slice.length + r.value.length];
                System.arraycopy(slice, 0, newvalue, 0, slice.length);
                System.arraycopy(r.value, 0, newvalue, slice.length, r.value.length);
                context.write(new Text(r.descriptor), new IntArrayWritable(newvalue));
            }
        }
    }

    private static class IntArrayWritable implements WritableComparable<IntArrayWritable>{
        private int[] value;

        public IntArrayWritable(int[] value){
            this.value = value;
        }

        @Override
        public int compareTo(IntArrayWritable o) {
            return 0;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {

        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {

        }
    }

    private static class CustomReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>{

        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            BytesWritable bytes = new BytesWritable();
            bytes.set(new byte[]{1,2,3}, 0, 3);
            context.write(key, bytes);
        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.out.println("Usage: hadoop jar *.jar input output");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path input_path = new Path(args[0]);
        Path output_path = new Path(args[1]);
        if(fs.exists(output_path)){
            fs.delete(output_path, true);
        }

        Job job = Job.getInstance(conf, "Deconvolution");
        FileInputFormat.setInputPaths(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);
        LazyOutputFormat.setOutputFormatClass(job, org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        job.setJarByClass(ImageProcesser.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);
        job.setInputFormatClass(TIFFInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(TIFFOutputFormat.class);
        job.waitForCompletion(true);
    }
}
