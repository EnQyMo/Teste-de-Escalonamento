package main.java.EnQyMo;

import utils.JsonParseException;
import utils.JsonParser;

import utils.java.Sensor.Sensor;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;

import ckafka.data.Swap;
import ckafka.data.SwapData;
import main.java.application.ModelApplication;

public class ProcessingNode extends ModelApplication{
    private JsonParser jsonParser;
    private ObjectMapper objectMapper;
    private Swap swap;
    private Sensor[] sensors;
    //private static final Logger logger = LoggerFactory.getLogger(ProcessingNode.class);

    private boolean fim = false;

    // Map<String, Consumer<String[]>> commandMap = new HashMap<>();

    /**
     * Constructor
     */
    public ProcessingNode() {
        this.objectMapper = new ObjectMapper();
        this.jsonParser = new JsonParser();
        this.swap = new Swap(objectMapper);
    }

    /**
     * Main
     * @param args command line arguments
     */
    public static void main(String[] args) {
        Scanner keyboard = new Scanner(System.in);
        ProcessingNode pn = new ProcessingNode();
        pn.runPN(keyboard);
    }

    public void runPN(Scanner keyboard) {
        System.out.println("Processing Node started. Type 'CTRL + C' to stop.");

        try {
            this.sensors = jsonParser.parse("data/sensors.json", Sensor[].class);
            System.out.println("Loaded " + (sensors != null ? sensors.length : 0) + " sensors.");

            System.out.println("Sensors:");
            if (sensors != null) {
                for (Sensor sensor : sensors) {
                    System.out.println(" - ID: " + sensor.getId() + ", Name: " + sensor.getName() + ", Group: " + sensor.getGroup());
                }
            }
        } catch (JsonParseException e) {
            System.err.println("Error loading sensors: " + e.getMessage());
            e.printStackTrace();
        }



        while(!fim) {
            // System.out.println("### n = " + n++ + " ###");

        }
        keyboard.close();
        System.out.println("END!");
        System.exit(0);
    }

    /**
     * Process sensor alert from JSON
     * @param analysis : AlertAnalysis object parsed from JSON
     */
    private void processAnalysis(AlertAnalysis analysis) {
        try {

            System.out.println("=== Alert Analysis ===");
            System.out.println("Alert ID: " + analysis.getAlertId());
            System.out.println("Timestamp: " + analysis.getTimestamp());
            System.out.println("Number of sensors: " + analysis.getSensors().size());

            for (SensorAlert sensorAlert : analysis.getSensors()) {
                System.out.println("\n--- Sensor: " + sensorAlert.getSensorId() + " ---");

                if (sensorAlert.getPollutants() == null || sensorAlert.getPollutants().isEmpty()) {
                    System.out.println("No pollutants data available for this sensor.");
                    continue;
                } else {
                    String sensorAlertId = sensorAlert.getSensorId();

                    int group = -1;
                    boolean found = false;
                    if (sensors != null) {
                        for (Sensor sensor : sensors) {
                            if (sensor.getId().equals(sensorAlertId)) {
                                group = sensor.getGroup();
                                found = true;
                                break;
                            }
                        }

                        if (found) {
                            System.out.println("Sensor ID " + sensorAlertId + " found in configuration.");
                            this.sendGroupcastMessage(sensorAlert.getPollutants().toString(), group, "GroupMessageTopic");
                        } else {
                            System.out.println("Warning: Sensor ID " + sensorAlertId + " NOT found in configuration.");
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error processing sensor alert: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Record the received Kafka message
     * @param record : ConsumerRecord containing the message
     */
    @Override
    public void recordReceived(ConsumerRecord record) {
        System.out.println(String.format("#--------------# Receiving message from %s #--------------#", record.key()));
        String message = "";

        try {
            // First, convert to string to detect message type
            message = new String((byte[]) record.value(), StandardCharsets.UTF_8);

            // Check if it's a direct JSON message (contains "analisys" field)
            if (message.trim().contains("\"analisys\"")) {
                System.out.println("(Direct JSON) Analysis message detected.");
                System.out.println("Message received: " + message);

                // Process as alert analysis
                System.out.println("Processing alert...");
                try {
                    Alert alert = objectMapper.readValue(message, Alert.class);
                    processAnalysis(alert.getAnalisys());
                } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                    System.err.println("Error parsing JSON message: " + e.getMessage());
                    e.printStackTrace();
                } catch (Exception e) {
                    System.err.println("Unexpected error processing JSON message: " + e.getMessage());
                    e.printStackTrace();
                }
            } else {
                // Try to parse as SwapData from ContextNet
                try {
                    SwapData data = swap.SwapDataDeserialization((byte[]) record.value());
                    message = new String(data.getMessage(), StandardCharsets.UTF_8);
                    System.out.println("(Swapped Data) Message received: " + message);

                    // Check if the inner message contains "analisys"
                    if (message.trim().contains("\"analisys\"")) {
                        System.out.println("Analysis command detected in swapped data. Processing alert...");
                        try {
                            Alert alert = objectMapper.readValue(message, Alert.class);
                            processAnalysis(alert.getAnalisys());
                        } catch (Exception e) {
                            System.err.println("Error processing analysis from swapped data: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Not a SwapData message. Message content: " + message);
                    System.out.println("Unknown message format. Ignoring.");
                }
            }

        } catch (Exception e) {
            System.err.println("Error processing record: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Send groupcast message
     * @param keyboard
     */
    private void sendGroupcastMessage(String messageText, Integer groupNumber, String topic) {
        String group = String.format("%d", groupNumber);

        System.out.println(String.format("Sending message %s to group %s.", messageText, group));
        try {
            // sendRecord(createRecord("GroupMessageTopic", group, swap.SwapDataSerialization(createSwapData(messageText))));

            SwapData data = createSwapData(messageText);
            data.setMessage(messageText.getBytes());
            data.setTopic(topic);

            sendRecord(createRecord("GroupMessageTopic", group, swap.SwapDataSerialization(data)));

        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error on SendGroupCastMessage", e);
        }
    }
}

// ==================== Private Classes for Alert Analysis ====================

/**
 * Wrapper class for the JSON structure with "analisys" field
 */
class Alert {
    @JsonProperty("analisys")
    private AlertAnalysis analisys;

    public Alert() {}

    public AlertAnalysis getAnalisys() {
        return this.analisys;
    }

    @Override
    public String toString() {
        return "Alert{" +
                "analisys=" + this.analisys +
                '}';
    }
}

/**
 * Main class representing the alert analysis structure
 */
class AlertAnalysis {
    @JsonProperty("alert_id")
    private String alertId;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("sensores")
    private List<SensorAlert> sensors;

    public AlertAnalysis() {}

    public String getAlertId() {
        return this.alertId;
    }

    public String getTimestamp() {
        return this.timestamp;
    }

    public List<SensorAlert> getSensors() {
        return this.sensors;
    }

    @Override
    public String toString() {
        return "AlertAnalysis{" +
                "alertId='" + this.alertId + '\'' +
                ", timestamp='" + this.timestamp + '\'' +
                ", sensors=" + this.sensors +
                '}';
    }
}

/**
 * Class representing a sensor alert with pollutants information
 */
class SensorAlert {
    @JsonProperty("sensor_id")
    private String sensorId;

    @JsonProperty("poluentes")
    private List<Pollutant> pollutants;

    public SensorAlert() {}

    public String getSensorId() {
        return this.sensorId;
    }

    public List<Pollutant> getPollutants() {
        return this.pollutants;
    }

    @Override
    public String toString() {
        return "SensorAlert{" +
                "sensorId='" + this.sensorId + '\'' +
                ", pollutants=" + this.pollutants +
                '}';
    }
}

/**
 * Class representing a pollutant with risk level and affected diseases
 */
class Pollutant {
    @JsonProperty("poluente")
    private String name;

    @JsonProperty("risk_level")
    private String riskLevel;

    @JsonProperty("affected_diseases")
    private AffectedDiseases affectedDiseases;

    public Pollutant() {}

    public String getName() {
        return this.name;
    }

    public String getRiskLevel() {
        return this.riskLevel;
    }

    public AffectedDiseases getAffectedDiseases() {
        return this.affectedDiseases;
    }

    @Override
    public String toString() {
        return "Pollutant{" +
                "name='" + this.name + '\'' +
                ", riskLevel='" + this.riskLevel + '\'' +
                ", affectedDiseases=" + this.affectedDiseases +
                '}';
    }
}

/**
 * Class representing the list of affected diseases
 */
class AffectedDiseases {
    @JsonProperty("disease")
    private List<String> disease;

    public AffectedDiseases() {}

    public List<String> getDisease() {
        return this.disease;
    }

    @Override
    public String toString() {
        return "AffectedDiseases{" +
                "disease=" + this.disease +
                '}';
    }
}
