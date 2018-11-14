package grepfoundf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RegexReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text keyIn, Iterable<Text> valuesIn, Context context)
            throws IOException, InterruptedException {

        StringBuilder valueOut = new StringBuilder();

        for(Text value: valuesIn)
            valueOut.append("\n" + value.toString());
        valueOut.append("\n");

        context.write(keyIn, new Text(valueOut.toString()));
    }
}
