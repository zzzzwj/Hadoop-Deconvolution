import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TIFFRecordReader extends RecordReader<Text, BytesWritable>{

    //将作为key-value中的value值返回
    private BytesWritable value;
    private int[] shape = new int[]{0,0,0};
    private int bits, slice, size;
    private FSDataInputStream in;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        /*
         * 注意这儿，fileSplit中只是存放着待处理内容的位置 大小等信息，并没有实际的内容
         * 因此这里会通过fileSplit找到待处理文件，然后再读入内容到value中
         */
        FileSplit fileSplit = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        this.in = fs.open(file);

        // 读取文件头
        byte[] header = new byte[16];
        try {
            // 自定义格式的文件头长度为16bytes, 分别表示[x,y,z,bits]
            in.read(0, header, 0, header.length);
            this.shape[0] = Utils.bytes2int(header, 0);
            this.shape[1] = Utils.bytes2int(header, 4);
            this.shape[2] = Utils.bytes2int(header, 8);
            this.bits = Utils.bytes2int(header, 12) / 8;
            this.size = this.shape[0] * this.shape[1] * this.bits;
        } finally {
        }
        this.slice = 0;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(slice < shape[2]){
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return new Text(slice + " " + shape[0] + " " + shape[1]);
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        value = new BytesWritable();
        byte[] content=new byte[size];
        in.read(slice * size + 16, content, 0, size);
        value.set(content, 0, size);
        slice += 1;
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return slice * 1.0f / shape[2];
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeStream(in);
    }
}
