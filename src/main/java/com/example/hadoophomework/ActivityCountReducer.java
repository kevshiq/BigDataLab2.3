package com.example.hadoophomework;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;

public class ActivityCountReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Set<String> activeDatesCal = new HashSet<>();

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            String reportedDate = parts[0];
            String isActive = parts[1];
            if (isActive.equals("1")) {
                activeDatesCal.add(reportedDate);
            }
        }
        int totalActiveDays = activeDatesCal.size();
        result.set(String.valueOf(totalActiveDays));
        context.write(key, result);   
    }
}
