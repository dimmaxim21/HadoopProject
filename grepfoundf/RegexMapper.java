package grepfoundf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMapper extends Mapper<LongWritable, Text, Text, Text>{
    private Pattern pattern;
    private Text    keyOut;

    @Override
    public void setup(Context context) throws IOException{

        pattern = Pattern.compile(context.getConfiguration().get("regex"));

        Path filePath = ((FileSplit) context.getInputSplit()).getPath();
        keyOut        = new Text(filePath.toString());
    }

    @Override
    public void map(LongWritable key, Text valueIn, Context context)
            throws IOException, InterruptedException {
        Matcher matcher = pattern.matcher(valueIn.toString());

        if (matcher.find())
            context.write(keyOut, valueIn);
    }
}