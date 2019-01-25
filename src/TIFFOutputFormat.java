import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TIFFOutputFormat extends FileOutputFormat<Text, BytesWritable> {

    @Override
    public RecordWriter<Text, BytesWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.newInstance(context.getConfiguration());
        Path path = getOutputPath(context);
        return new TIFFRecordWriter(fs, path);
    }
}
