package com.epam.bytesbyiptest;

import com.epam.bytesbyip.BytesByIPReducer;
import com.epam.bytesbyip.writable.FloatIntPairWritable;
import com.epam.bytesbyip.writable.IntPairWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BytesByIPReducerTest {
    private ReduceDriver<Text, IntPairWritable, Text, FloatIntPairWritable> reduceDriver;

    @Before
    public void setUp() {
        BytesByIPReducer reducer = new BytesByIPReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }

    @Test
    public void testReducer() throws IOException {
        List<IntPairWritable> ip1Values = new ArrayList<>();
        ip1Values.add(new IntPairWritable(300, 3));
        ip1Values.add(new IntPairWritable(50, 1));
        reduceDriver.withInput(new Text("ip1"), ip1Values);
        reduceDriver.withOutput(new Text("ip1"), new FloatIntPairWritable((float) 350 / 4, + 350));
        reduceDriver.runTest();
    }
}
