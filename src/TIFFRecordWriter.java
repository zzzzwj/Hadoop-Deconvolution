import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class TIFFRecordWriter extends RecordWriter<Text, BytesWritable> {
    FileSystem fs;
    Path path;

    public TIFFRecordWriter(FileSystem fs, Path path){
        this.fs = fs;
        this.path = path;
    }

    @Override
    public void write(Text key, BytesWritable value) throws IOException, InterruptedException {
        boolean nullKey = key == null;
        boolean nullValue = value == null;

        if (nullKey || nullValue) {
            return;
        } else {
            String[] res = key.toString().split(" ");
            FSDataOutputStream out = fs.create(new Path(path.getName() + "/" + res[0] + ".bin"));
            try {
                // write header info
                out.write(Utils.int2bytes(Integer.valueOf(res[1]), 32));
                out.write(Utils.int2bytes(Integer.valueOf(res[2]), 32));
                out.write(Utils.int2bytes(1, 32));
                out.write(Utils.int2bytes(16, 32));
                // write img data
                out.write(value.copyBytes());
            } finally {
                out.close();
            }
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fs.close();
    }
}
