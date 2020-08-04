package com.epam.bytesbyiptest;

import com.epam.bytesbyip.BytesByIPMapper;
import com.epam.bytesbyip.BytesByIPReducer;
import com.epam.bytesbyip.writable.FloatIntPairWritable;
import com.epam.bytesbyip.writable.IntPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class BytesByIPTest {
    private MapReduceDriver<LongWritable, Text, Text, IntPairWritable, Text, FloatIntPairWritable> mapReduceDriver;

    @Before
    public void setUp() {
        BytesByIPMapper mapper = new BytesByIPMapper();
        BytesByIPReducer reducer = new BytesByIPReducer();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip2 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 517 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:06:35:10 -0400] \"GET /next HTTP/1.1\" 301 312 \"-\" \"Java/1.6.0_04\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip2 - - [24/Apr/2011:10:07:17 -0400] \"HEAD /~strabal/TFE.mp3 HTTP/1.1\" 200 0 \"-\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.1; computerbild; computerbild)\""));
        mapReduceDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:10:29:50 -0400] \"GET / HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\""));
        mapReduceDriver.withOutput(new Text("ip1"), new FloatIntPairWritable((float) (40028 + 312) / 3, 40028 + 312));
        mapReduceDriver.withOutput(new Text("ip2"), new FloatIntPairWritable((float) 517 / 2, + 517));
        mapReduceDriver.runTest();
    }
}
