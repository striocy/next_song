package hadoop.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//去重并统计artists
public class Mapper1 extends Mapper<LongWritable, Text,Text,LongWritable> {
    private final static LongWritable one = new LongWritable(1);
    private Text artistName = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(","); // 使用逗号分割CSV行

        // CSV的格式是：user_id, artistname, trackname, playlistname
        if (fields.length >= 2) {
            String artist = fields[1].trim(); // 获取artistname字段
            artistName.set(artist);
            context.write(artistName, one);
        }
    }
}
