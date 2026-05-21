package utils.java.Sensor;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Sensor {
    @JsonProperty("sensor_id")
    public String id;

    @JsonProperty("sensor_name")
    public String name;

    @JsonProperty("sensor_group")
    public int group;

    public Sensor() {}

    public String getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public int getGroup() {
        return this.group;
    }
}
