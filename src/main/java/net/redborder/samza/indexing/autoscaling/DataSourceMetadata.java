package net.redborder.samza.indexing.autoscaling;

public class DataSourceMetadata {
    private Integer maxPartitions;
    private Integer replicas;
    private Long events;

    public DataSourceMetadata(Integer maxPartitions, Integer replicas, Long events){
        this.maxPartitions = maxPartitions;
        this.replicas = replicas;
        this.events = events;
    }

    public Integer maxPartitions(){
        return maxPartitions == null || maxPartitions == 0 ? 1 : maxPartitions;
    }

    public Integer replicas(){
        return replicas == null || replicas == 0 ? 1 : replicas;
    }

    public Long events(){
        return events == null ? 0L : events;
    }
}
