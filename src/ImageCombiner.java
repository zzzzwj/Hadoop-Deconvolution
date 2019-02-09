import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.slf4j.helpers.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.lang.Math;

public class ImageCombiner {
    private static class CustomMapper extends Mapper<Text, BytesWritable, IntWritable, BytesWritable> {

        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            String[] keywords_1 = (((FileSplit)context.getInputSplit()).getPath().getName().split("\\.")[0]).split("-");
            String[] keywords_2 = key.toString().split(" ");
            int idx = Integer.valueOf(keywords_1[0]), idy = Integer.valueOf(keywords_1[1]), padding = Integer.valueOf(keywords_1[2]);
            int suby = Integer.valueOf(keywords_2[1]), subx = Integer.valueOf(keywords_2[2]);
            int slice = Integer.valueOf(keywords_2[0]);

            IntWritable newkey = new IntWritable(slice);
            byte[] flags = Utils.ints2bytes(new int[]{idx, idy, subx, suby, padding}, 2);
//            System.out.println("flags:" + Arrays.toString(Utils.bytes2ints(flags, 2)));
//            System.out.println("flags:" + Utils.printHexBinary(flags));
//            System.out.println(idx + " " + idy + " " + subx + " " + suby + " " + padding);
            byte[] data = value.copyBytes();
            byte[] newvalue = new byte[flags.length + data.length];
            System.arraycopy(flags, 0, newvalue, 0, flags.length);
            System.arraycopy(data, 0, newvalue, flags.length, data.length);
            context.write(newkey, new BytesWritable(newvalue));
        }
    }

    private static class CustomReducer extends Reducer<IntWritable, BytesWritable, Text, BytesWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            HashMap<ArrayList<Integer>, int[]> map = new HashMap<>();
            int count = 0;
            for(BytesWritable value : values){
                byte[] data = value.copyBytes();
                int[] flags = Utils.bytes2ints(Arrays.copyOfRange(data, 0, 4), 2);
//                System.out.println(Arrays.toString(Utils.bytes2ints(Arrays.copyOfRange(data, 0, 10), 2)));
                ArrayList<Integer> k = new ArrayList<>();
                for(int flag : flags)
                    k.add(flag);
                map.put(k, Utils.bytes2ints(Arrays.copyOfRange(data, 4, data.length), 2));
                count++;
            }
            int factor = (int)Math.round(Math.sqrt(count));

            ArrayList<int[]> dim_y = new ArrayList<>();
            int len_y = 0, len_x = 0;
            int padding = 0;
            for(int j = 0; j < factor; j++){
                len_x = 0;
                ArrayList<int[]> dim_x = new ArrayList<>();
                int suby = 0;
                for(int i = 0; i < factor; i++){
                    int[] buffer = map.get(new ArrayList<>(Arrays.asList(i, j)));
//                    System.out.println(Arrays.toString(Arrays.copyOfRange(buffer, 0, 3)));
                    int[] flag = Arrays.copyOfRange(buffer, 0, 3);
                    int[] data = Arrays.copyOfRange(buffer, 3, buffer.length);
                    int subx = flag[0];
                    suby = flag[1];
                    padding = flag[2];
                    int[] subidx = Utils.getCutgraphrange(i, subx, factor, padding);
//                    System.out.println("subx:"+subx+" suby:"+suby);
//                    System.out.println("data:"+Arrays.toString(data));
                    int[] subgraph = Utils.subgraph(data, subx, suby, subidx[0], 0, subidx[1] - subidx[0], suby);
                    subx = subidx[1] - subidx[0];
                    dim_x.add(subgraph);
                    len_x += subx;
                }
                int[] buffer = new int[len_x * suby];
                for(int i = 0; i < suby; i++){
                    int start_len = 0;
                    for(int[] img : dim_x){
                        int width = img.length / suby;
                        System.arraycopy(img, i * width, buffer, i * len_x + start_len, width);
                        start_len += width;
                    }
                }
                int[] subidx = Utils.getCutgraphrange(j, suby, factor, padding);
                int[] subgraph = Utils.subgraph(buffer, len_x, suby, 0, subidx[0], len_x, subidx[1] - subidx[0]);
//                System.out.println("graph:"+Arrays.toString(subgraph));
                suby =  subidx[1] - subidx[0];
                dim_y.add(subgraph);
                len_y += suby;
            }
            int[] res = new int[len_y * len_x];
            int len = 0;
            for(int[] data : dim_y) {
                System.arraycopy(data, 0, res, len, data.length);
                len += data.length;
            }
//            System.out.println(Arrays.toString(res));

            context.write(new Text(key.toString() + " " + len_x + " " + len_y), new BytesWritable(Utils.ints2bytes(res, 2)));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path input_path = new Path(args[0]);
        Path output_path = new Path(args[1]);
        if(fs.exists(output_path)){
            fs.delete(output_path, true);
        }

        Job job = Job.getInstance(conf, "Combine");
        FileInputFormat.setInputPaths(job, input_path);
        FileOutputFormat.setOutputPath(job, output_path);
        LazyOutputFormat.setOutputFormatClass(job, org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class);
        job.setJarByClass(ImageCombiner.class);
        job.setMapperClass(CustomMapper.class);
        job.setReducerClass(CustomReducer.class);
        job.setInputFormatClass(TIFFInputFormat.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(TIFFOutputFormat.class);
        job.waitForCompletion(true);

    }
}
