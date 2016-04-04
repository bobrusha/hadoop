import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;

public class SpectrumMapper extends Mapper<Text, RawSpectrum, Text, RawSpectrum> {
    private int counter = 0;

    public void map(Text key, RawSpectrum value, Context context) throws IOException, InterruptedException {
        // limit no. of spectra to save space
        context.write(key, value);
    }

}
