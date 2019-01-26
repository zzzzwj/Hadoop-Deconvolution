import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.HashMap;

public class Driver {

    private static HashMap<Integer, String> getFileList(FileSystem fs, String path) throws IOException {
        HashMap<Integer, String> map = new HashMap<>();
        Path s_path = new Path(path);
        if(fs.exists(s_path)){
            for(FileStatus status : fs.listStatus(s_path)){
                String filename = status.getPath().getName();
                if(filename.endsWith("bin"))
                    map.put(Integer.valueOf(filename.split("\\.")[0]), path + "/" + filename);
            }
        }
        fs.close();
        return map;
    }

    private static void CombineSubImg(FileSystem fs, HashMap<Integer, String> map, String output) throws IOException {
        int z = map.keySet().size();
        FSDataOutputStream out = fs.create(new Path(output));

        for(int i = 0; i < z; i++){
            FSDataInputStream in = fs.open(new Path(map.get(i)));
            byte[] header_info = new byte[16];
            in.read(0, header_info, 0, header_info.length);
            int[] head = Utils.bytes2ints(header_info, 4);
//            System.out.println(i);
//            System.out.println(Arrays.toString(head));
            if(i == 0){
                byte[] newheader = Utils.ints2bytes(new int[]{head[0], head[1], z, head[3]}, 4);
                out.write(newheader);
            }
            byte[] content = new byte[head[0] * head[1] * head[2] * head[3] / 8];
            in.read(16, content, 0, content.length);
            out.write(content);
            in.close();
        }
        out.close();
    }

    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.out.println("Usage: hadoop jar *.jar input output");
            System.exit(-1);
        }

        String tmpdir1 = "tmp_res";
        String tmpdir2 = "tmp_res2";
        String[] ImageProcess_param = {args[0], tmpdir1};
        String[] ImageCombiner_param = {tmpdir1, tmpdir2};

        ImageProcesser.main(ImageProcess_param);
        ImageCombiner.main(ImageCombiner_param);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        HashMap<Integer, String> map = getFileList(fs, tmpdir2);
        CombineSubImg(fs, map, args[1]);

        // 注释以保留中间结果， 主要用于调试
        fs.delete(new Path(ImageProcess_param[1]),true);
        fs.delete(new Path(ImageCombiner_param[1]), true);
    }
}
