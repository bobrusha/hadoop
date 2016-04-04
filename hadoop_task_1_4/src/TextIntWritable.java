import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.StringTokenizer;

/**
 * Created by Aleksandra on 07.11.15.
 */
public class TextIntWritable implements Writable {
    private String text = "";
    private Integer number = 0;

    public TextIntWritable() {
    }

    public TextIntWritable(String text, Integer number) {
        this.text = text;
        this.number = number;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(text);
        dataOutput.writeInt(number);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        text = dataInput.readUTF();
        number = dataInput.readInt();
    }

    public String getText() {
        return text;
    }

    public Integer getNumber() {
        return number;
    }

    @Override
    public String toString() {
        return text + " " + number;
    }
}
