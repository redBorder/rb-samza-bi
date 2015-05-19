package net.redborder.samza.indexing.autoscaling;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;

public class AutoScalingUtils {
    public static Integer getPartitions(String dataSource) {
        String data [] = dataSource.split("_");
        return Integer.valueOf(data[data.length - 2]);
    }

    public static Integer getReplicas(String dataSource) {
        String data [] = dataSource.split("_");
        return Integer.valueOf(data[data.length - 1]);
    }

    public static String getTier(String dataSource){
        String data [] = dataSource.split("_");
        return data[data.length - 3];
    }

    public static String getDataSource(String dataSource){
        String data [] = dataSource.split("_");
        String subData [] = Arrays.copyOfRange(data, 0, data.length - 4);
        return StringUtils.join(subData, "_");
    }
}
