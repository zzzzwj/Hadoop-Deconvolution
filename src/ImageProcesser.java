import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;

public class ImageProcesser {
    private static class CustomMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>{

        @Override
        public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
//            System.out.println(((FileSplit)context.getInputSplit()).getPath().getName());
            String[] keywords = key.toString().split(" ");
            int[] data = Utils.bytes2ints(value.copyBytes(), 2);
//            System.out.println(Arrays.toString(data));
            // 默认16位uint
            int[] shape = new int[]{Integer.valueOf(keywords[1]), Integer.valueOf(keywords[2])};
            // 最后一位参数需要根据PSF进行设置，一般为PSF的边长 / factor， 输出带padding的子图
            int padding = Integer.valueOf(context.getConfiguration().get("kernel"));
            ArrayList<Record> res = Utils.all_subgraph(data, shape[1], shape[0], 2, padding);
            int slice = Integer.valueOf(keywords[0]);
            for(Record r : res){
                // 以子图在原图中的位置为key，将子图写入输出
                int[] newvalue = Utils.listappend(new int[]{slice}, r.value);
                context.write(new Text(r.descriptor), new BytesWritable(Utils.ints2bytes(newvalue, 4)));
            }
        }
    }

    private static class CustomReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>{

        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            TreeMap<Integer, int[]> map = new TreeMap<>();
//            System.out.println("key:" + key);
            int[] data = new int[]{};
            for(BytesWritable value : values){
                int[] array = Utils.bytes2ints(value.copyBytes(), 4);
                map.put(array[0], Arrays.copyOfRange(array, 1, array.length));
            }
            // 根据slice顺序将子图按z轴拼接，获得三维图像数据
            for(int k : map.keySet()){
                data = Utils.listappend(data, map.get(k));
            }
//            System.out.println("data:"+Arrays.toString(data));
            // Richardson Lucy Algorithm here
            Configuration conf = context.getConfiguration();
            String psf_path = conf.get("psf");
            int[] shape = new int[3];
            int[] psf = Utils.getPSF(new DataInputStream(new FileInputStream(new File(psf_path))), shape);
//            System.out.println(Arrays.toString(shape));
            int width = Integer.valueOf(key.toString().split(" ")[1]);
            int height = Integer.valueOf(key.toString().split(" ")[3]);
            System.out.println(psf.length);
            System.out.println(width + " " + height + " " + map.keySet().size() + " " + data.length);
            double[] res = Deconv.deconvolution(Utils.ints2doubles(data), map.keySet().size(), height, width,
                    Utils.ints2doubles(psf), shape[2], shape[1], shape[0]);
            data = Utils.doubles2ints(res);

            // 获得int类型的处理数据，以16位无符号整形写入文件
            key = new Text(map.keySet().size() + " " + key.toString());
            // must be 2 bytes, or you should change the param in RecordWriter either.
            context.write(key, new BytesWritable(Utils.ints2bytes(data, 2)));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path input_path = new Path(args[0]);
        String psf_path = args[1];
        Path output_path = new Path(args[2]);
        if(fs.exists(output_path)){
            fs.delete(output_path, true);
        }

        conf.set("psf", psf_path);
        conf.set("kernel", "40");

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
