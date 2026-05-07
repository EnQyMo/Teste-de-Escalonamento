package utils.java.Beacon;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Beacon {
    @JsonProperty("beacon_uuid")
    public String uuid;

    @JsonProperty("beacon_group")
    public int group;

    public Beacon() {}

    public String getUuid() {
        return this.uuid;
    }

    public int getGroup() {
        return this.group;
    }
}