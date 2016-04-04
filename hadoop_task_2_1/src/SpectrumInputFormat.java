import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class SpectrumInputFormat extends InputFormat<Text, RawSpectrum> {
    public static final int NUMBER_OF_VARIABLES = 5;

    @Override
    public List<InputSplit> getSplits(JobContext ctx) throws IOException, InterruptedException {
        final Map<String, List<Path>> filesMap = new HashMap<>();
        for (Path path : FileInputFormat.getInputPaths(ctx)) {
            FileSystem fs = path.getFileSystem(ctx.getConfiguration());
            for (FileStatus file : fs.listStatus(path)) {
                final String name = file.getPath().getName();
                final Matcher matcher = RawSpectrum.FILENAME_PATTERN.matcher(name);
                if (!matcher.matches()) {
                    continue;
                }
                final String number = matcher.group(1);
                final String year = matcher.group(3);
                final String key = number + year;
                if (filesMap.containsKey(key)) {
                    filesMap.get(key).add(file.getPath());
                } else {
                    //noinspection ArraysAsListWithZeroOrOneArgument
                    ArrayList<Path> paths = new ArrayList<>();
                    paths.add(file.getPath());
                    filesMap.put(key, paths);
                }
            }
        }

        final List<InputSplit> splits = new ArrayList<>();
        for (List<Path> paths : filesMap.values()) {
            if (paths.size() == NUMBER_OF_VARIABLES) {
                Path[] arrPaths = new Path[paths.size()];
                paths.toArray(arrPaths);
                splits.add(new CombineFileSplit(arrPaths, getFileLengths(paths, ctx)));
            }
        }
        return splits;
    }

    @Override
    public RecordReader<Text, RawSpectrum> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new SpectrumRecordReader();
    }

    private static long[] getFileLengths(List<Path> files, JobContext context) throws IOException {
        long[] lengths = new long[files.size()];
        for (int i = 0; i < files.size(); ++i) {
            FileSystem fs = files.get(i).getFileSystem(context.getConfiguration());
            lengths[i] = fs.getFileStatus(files.get(i)).getLen();
        }
        return lengths;
    }
}
