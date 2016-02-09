package net.redborder.samza.indexing.autoscaling;

public class DataSourceMetadata {
    private Integer maxPartitions;
    private Integer replicas;
    private Long events;

    public DataSourceMetadata(Integer maxPartitions, Integer replicas, Object events){
        this.maxPartitions = maxPartitions;
        this.replicas = replicas;

        if(events instanceof  Long) {
            this.events = (Long) events;
        } else if(events instanceof Integer){
            this.events = ((Integer) events).longValue();
        }
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

    @Override
    public String toString() {
        return String.format("[Events[%s] Partitions[%s] Replicas[%s]]", events, maxPartitions, replicas);
    }
}
