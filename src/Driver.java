import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Driver {
    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.out.println("Usage: hadoop jar *.jar input output");
            System.exit(-1);
        }

        String tmpdir = "tmp_res";
        String[] ImageProcess_param = {args[0], tmpdir};
        String[] ImageCombiner_param = {tmpdir, args[1]};

        ImageProcesser.main(ImageProcess_param);
        ImageCombiner.main(ImageCombiner_param);

        // clear the buffer directories
//        Configuration conf = new Configuration();
//        FileSystem fs = FileSystem.get(conf);

//        fs.delete(new Path(ImageProcess_param[1]),true);

//        FSDataInputStream in = fs.open(new Path(args[1] + "/part-r-00000"));
//        IOUtils.copyBytes(in, System.out, 1024);
//        IOUtils.closeStream(in);
    }
}
