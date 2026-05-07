package main.java.EnQyMo;

import java.util.HashSet;
import java.util.Set;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ckafka.data.Swap;
import main.java.ckafka.GroupDefiner;
import main.java.ckafka.GroupSelection;

import utils.JsonParser;
import utils.JsonParseException;

import utils.java.Beacon.Beacon;

public class MyGroupDefiner implements GroupSelection {
    /** Logger */
    final Logger logger = LoggerFactory.getLogger(MyGroupDefiner.class);
    private JsonParser jsonParser;
    private Beacon[] beacons;

    public static void main(String[] args) {
        MyGroupDefiner MyGD = new MyGroupDefiner();
    }

    /**
     * Constructor
     */
    public MyGroupDefiner() {
        ObjectMapper objectMapper = new ObjectMapper();
        Swap swap = new Swap(objectMapper);

        this.jsonParser = new JsonParser();

        // Load beacons from JSON file
        try {
            this.beacons = jsonParser.parse("data/beacons.json", Beacon[].class);
            System.out.println("Loaded " + (beacons != null ? beacons.length : 0) + " beacons.");

            if (beacons != null) {
                for (Beacon beacon : beacons) {
                    System.out.println(" - UUID: " + beacon.getUuid() + ", Group: " + beacon.getGroup());
                }
            }
        } catch (JsonParseException e) {
            System.err.println("Error loading beacons: " + e.getMessage());
            e.printStackTrace();
        }

        new GroupDefiner(this, swap);
    }

    /**
     * Set with all the groups that this GroupDefiner controls.
     * @return set with all the groups that this GroupDefiner manages
     */
    public Set<Integer> groupsIdentification() {
        /**
         * 1001 -> Beacon 1, Beacon 2
         * 1002 -> Beacon 3
         * 1003 -> Beacon 4
         */
        Set<Integer> setOfGroups = new HashSet<Integer>();

       // Load groups from beacons JSON
        if (this.beacons != null) {
            for (Beacon beacon : this.beacons) {
                setOfGroups.add(beacon.getGroup());
            }
        }

        return setOfGroups;
    }

    /**
     * Function to get user group ID from beacons
     * @param beaconIds array of beacon identifiers
     * @return group ID or -1 if not found
     */
    private int getGroupIDFromBeacons(String[] beaconIds)
    {
        if (this.beacons == null) {
                    return -1;
        }

        for (String beaconId : beaconIds) {
            // Clean the beacon ID
            beaconId = beaconId.replace("\"", "").trim();

            // Search for the beacon in the loaded beacons array
            for (Beacon beacon : this.beacons) {
                if (beacon.getUuid().equals(beaconId)) {
                    return beacon.getGroup();
                }
            }
        }

        return -1;
    }

    /**
     * Set with all the groups related to this contextInfo.
     * Only groups controlled by this GroupDefiner.
     *
     * @param contextInfo context info
     * @return set with all the groups related to this contextInfo
     */
    public Set<Integer> getNodesGroupByContext(ObjectNode contextInfo) {
        // Initialize a set to store group IDs
        Set<Integer> setOfGroups = new HashSet<Integer>();
        System.out.println("#--------------# Receiving context #--------------#");

        // Extract the location from contextInfo
        String string_beacons = String.valueOf(contextInfo.get("beacons"));
        System.out.println("Beacons from context: " + string_beacons);

        // Remove brackets and spaces, then split into array
        string_beacons = string_beacons.replace("[", "").replace("]", "");
        String[] beacons = string_beacons.split(", ");


        if (beacons != null) {
            int group = this.getGroupIDFromBeacons(beacons);

            if (group != -1) {
                setOfGroups.add(group);
                System.out.println("Group ID " + group + " added based on beacons: " + Arrays.toString(beacons));
            } else {
                System.out.println("No valid group found for beacons: " + Arrays.toString(beacons));
            }

        } else {
            System.out.println("No beacons detected at location.");
        }

        // Log the final set of groups for debugging purposes
        System.out.println(setOfGroups);

        return setOfGroups; // Return the set of group IDs
}

    public String kafkaConsumerPrefix() {
        return "gd.one.consumer";
    }

    public String kafkaProducerPrefix() {
        return "gd.one.producer";
    }
}
