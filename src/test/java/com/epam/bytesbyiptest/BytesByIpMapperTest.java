package com.epam.bytesbyiptest;

import com.epam.bytesbyip.BytesByIPMapper;
import com.epam.bytesbyip.writable.IntPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

public class BytesByIpMapperTest {
    private MapDriver<LongWritable, Text, Text, IntPairWritable> mapDriver;

    @Before
    public void setUp() {
        BytesByIPMapper mapper = new BytesByIPMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("ip1 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 \"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\""));
        mapDriver.withInput(new LongWritable(), new Text("ip43 - - [24/Apr/2011:06:35:10 -0400] \"GET /next HTTP/1.1\" 301 312 \"-\" \"Java/1.6.0_04\""));
        mapDriver.withInput(new LongWritable(), new Text("ip108 - - [24/Apr/2011:10:07:17 -0400] \"HEAD /~strabal/TFE.mp3 HTTP/1.1\" 200 0 \"-\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.1; computerbild; computerbild)\""));
        mapDriver.withInput(new LongWritable(), new Text("ip13 - - [24/Apr/2011:10:29:50 -0400] \"GET / HTTP/1.1\" 304 - \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\""));
        mapDriver.withOutput(new Text("ip1"), new IntPairWritable(40028, 1));
        mapDriver.withOutput(new Text("ip43"), new IntPairWritable(312, 1));
        mapDriver.withOutput(new Text("ip108"), new IntPairWritable(0, 1));
        mapDriver.withOutput(new Text("ip13"), new IntPairWritable(0, 1));
        mapDriver.runTest();
    }
}
