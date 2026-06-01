package EnQyMo;

import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.ArrayList;
import java.util.Arrays;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ckafka.data.SwapData;
import lac.cnclib.net.NodeConnection;
import lac.cnclib.sddl.message.ApplicationMessage;
import lac.cnclib.sddl.message.Message;
import main.java.ckafka.mobile.CKMobileNode;
import main.java.ckafka.mobile.tasks.SendLocationTask;

/**
 * Mobile Node
 * <br>
 * Implements a mobile node that contains a student's information to check their attendance
 */
public class MobileNode extends CKMobileNode {
    // Valid user input options
    private static final String OPTION_GROUPCAST = "G";
    private static final String OPTION_PN = "P";
    private static final String OPTION_ALERT = "A";
    private static final String OPTION_RECORD = "T";
    private static final String OPTION_EXIT = "Z";
    private static final String OPTION_STRESS_TEST = "S";

    // The variable cannot be local because it is being used in a lambda function
    // Control the infinite loop until it ends
    private boolean fim = false;

    private static final int DEFAULT_STRESS_ALERT_ATTEMPTS = 10; // Number of alerts to send during stress test
    private static final int DEFAULT_STRESS_ALERT_MESSAGE_INTERVAL = 5000; // 5 seconds

    /**
     * main
     * @param args command line arguments
     */
    public static void main(String[] args) {
        Scanner keyboard = new Scanner(System.in);
        MobileNode mn = new MobileNode();
        mn.runMN(keyboard);

        // Calls close() to properly close MN method after shut down
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            close();
        }));
    }

    /**
     * Executes the Mobile Node.
     * <br>
     * Read user option from keyboard (unicast or groupcast message)<br>
     * Read destination receipt from keyboard (UUID or Group)<br>
     * Read message from keyboard<br>
     * Send message<br>
     */
    private void runMN(Scanner keyboard) {
        Map<String, Consumer<Scanner>> optionsMap = new HashMap<>();

        // Maps options to corresponding functions
        // optionsMap.put(OPTION_UNICAST, this::sendUnicastMessage);
        optionsMap.put(OPTION_PN, this::enterMessageToPN);
        optionsMap.put(OPTION_ALERT, this::sendAlertToPN);
        optionsMap.put(OPTION_RECORD, this::requestRecord);
        optionsMap.put(OPTION_GROUPCAST, this::sendGroupcastMessage);
        optionsMap.put(OPTION_STRESS_TEST, scanner -> this.sendAlertToPN(scanner,DEFAULT_STRESS_ALERT_ATTEMPTS,DEFAULT_STRESS_ALERT_MESSAGE_INTERVAL));
        optionsMap.put(OPTION_EXIT, scanner -> fim = true);

        // Main loop that continues until the 'fim' variable is true
        while (!fim) {

            // Requests the user's option
            System.out.print("(G) Groupcast | (P) Message to PN | (A) Send Alert to PN |\n" +
                             "(T) Request Record | (S) Send Stress Test to PN | (Z) to finish)? "); 
            String linha = keyboard.nextLine().trim().toUpperCase();
            System.out.printf("Your option was %s. ", linha);

            // Checks if the option is valid and executes the corresponding function
            if (optionsMap.containsKey(linha))
                optionsMap.get(linha).accept(keyboard);
            else
                System.out.println("Invalid option");
        }

        // Closes the scanner and ends the program
        keyboard.close();
        System.out.println("END!");
        System.exit(0);
    }

    /**
     * When connected, send location at each instant
     */
    @Override
    public void connected(NodeConnection nodeConnection) {
        try {
            logger.debug("Connected");
            final SendLocationTask sendlocationtask = new SendLocationTask(this);
            this.scheduledFutureLocationTask = this.threadPool.scheduleWithFixedDelay(
                    sendlocationtask, 5000, 60000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error scheduling SendLocationTask", e);
        }
    }

    /**
     * Reads the message via command line from the user.
     * <br>
     * Sends a unicast message
     */
    private void sendUnicastMessage(Scanner keyboard) {
        System.out.println("Unicast message. Enter the individual's UUID: ");
        String uuid = keyboard.nextLine();
        System.out.print("Enter the message: ");
        String messageText = keyboard.nextLine();
        System.out.println(String.format("Message from |%s| to %s.", messageText, uuid));
        // Create and send the message
        SwapData privateData = new SwapData();
        privateData.setMessage(messageText.getBytes(StandardCharsets.UTF_8));
        privateData.setTopic("PrivateMessageTopic");
        privateData.setRecipient(uuid);
        ApplicationMessage message = createDefaultApplicationMessage();
        message.setContentObject(privateData);
        sendMessageToGateway(message);
    }

    /**
     * Receives messages
     */
    @Override
    public void newMessageReceived(NodeConnection nodeConnection, Message message) {
        try {
            SwapData swp = fromMessageToSwapData(message);
            System.out.println("Topic: " + swp.getTopic());
            
            if ("PrivateMessageTopic".equals(swp.getTopic()) || "UniCast".equals(swp.getTopic())) {
                String content = new String(swp.getMessage(), StandardCharsets.UTF_8);
                if (content.startsWith("[STATS]")) {
                    String fileName = "stress_test_results.csv";
                    System.out.println("Received test stats from PN. Writing to " + fileName + "...");
                    String csvContent = content.substring(7);
                    try (java.io.PrintWriter pw = new java.io.PrintWriter(new java.io.FileWriter(fileName))) {
                        pw.print(csvContent);
                        System.out.println("Results successfully written to " + fileName + "!");
                    } catch (Exception e) {
                        logger.error("Failed to write stats to file", e);
                    }
                    return;
                }
            }

            if (swp.getTopic().equals("Ping")) {
                message.setSenderID(this.mnID);
                sendMessageToGateway(message);
            } else {
                String messageText = new String(swp.getMessage(), StandardCharsets.UTF_8);
                System.out.println("Message: " + messageText);
            }
        } catch (Exception e) {
            logger.error("Error reading message", e);
        }
    }

    /**
     * Sends [RECORD] command to the Processing Node to get the stress test results
     */
    private void requestRecord(Scanner keyboard) {
        System.out.println("Sending [RECORD] command to Processing Node...");
        try {
            SwapData data = new SwapData();
            data.setMessage("[RECORD]".getBytes(StandardCharsets.UTF_8));
            data.setTopic("AppModel");
            ApplicationMessage message = createDefaultApplicationMessage();
            message.setContentObject(data);
            sendMessageToGateway(message);
            System.out.println("Request sent! Waiting for response...");
        } catch (Exception e) {
            logger.error("Failed to send [RECORD] command", e);
        }
    }

    /**
     * Sends message to the stationary Processing Node
     */
    private void enterMessageToPN(Scanner keyboard) {
        System.out.print("Enter the message: ");
        String messageText = keyboard.nextLine();

        this.sendMessageToPN(messageText, "AppModel");
    }

    /**
     * Sends alert analysis JSON to the Processing Node
     */
    private void sendAlertToPN(Scanner keyboard) {
        String alertJson = "{\n" +
            "  \"analisys\": {\n" +
            "    \"alert_id\": \"alert_1706798417002\",\n" +
            "    \"timestamp\": \"2025-10-01T22:40:17.002-03:00\",\n" +
            "    \"sensores\": [\n" +
            "      {\n" +
            "        \"sensor_id\": \"IAQ_6227821\",\n" +
            "        \"poluentes\": [\n" +
            "          {\n" +
            "            \"poluente\": \"pm25\",\n" +
            "            \"risk_level\": \"moderate\",\n" +
            "            \"affected_diseases\": {\n" +
            "              \"disease\": [\n" +
            "                \"asma\",\n" +
            "                \"bronquite\",\n" +
            "                \"irritação respiratória\"\n" +
            "              ]\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"poluente\": \"pm4\",\n" +
            "            \"risk_level\": \"high\",\n" +
            "            \"affected_diseases\": {\n" +
            "              \"disease\": [\n" +
            "                \"irritação respiratória\",\n" +
            "                \"inflamação sistêmica leve\"\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

        System.out.println("Sending alert analysis to Processing Node...");
        this.sendMessageToPN(alertJson, "AppModel");
        System.out.println("Alert sent successfully!");
    }

    /**
     * Sends alert analysis JSON to the Processing Node multiple times with a 5 seconds interval
     * @param keyboard
     * @param numOfAlerts number of alerts to send
     * @param messageInterval interval between alerts in milliseconds
     */
    private void sendAlertToPN(Scanner keyboard, int numOfAlerts, int messageInterval) {
        String alertJson = "{\n" +
            "  \"analisys\": {\n" +
            "    \"alert_id\": \"alert_1706798417002\",\n" +
            "    \"timestamp\": \"2025-10-01T22:40:17.002-03:00\",\n" +
            "    \"sensores\": [\n" +
            "      {\n" +
            "        \"sensor_id\": \"IAQ_6227821\",\n" +
            "        \"poluentes\": [\n" +
            "          {\n" +
            "            \"poluente\": \"pm25\",\n" +
            "            \"risk_level\": \"moderate\",\n" +
            "            \"affected_diseases\": {\n" +
            "              \"disease\": [\n" +
            "                \"asma\",\n" +
            "                \"bronquite\",\n" +
            "                \"irritação respiratória\"\n" +
            "              ]\n" +
            "            }\n" +
            "          },\n" +
            "          {\n" +
            "            \"poluente\": \"pm4\",\n" +
            "            \"risk_level\": \"high\",\n" +
            "            \"affected_diseases\": {\n" +
            "              \"disease\": [\n" +
            "                \"irritação respiratória\",\n" +
            "                \"inflamação sistêmica leve\"\n" +
            "              ]\n" +
            "            }\n" +
            "          }\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "}";

        System.out.println("Starting to send alert analysis to Processing Node...");
        for (int i = 0; i < numOfAlerts; i++) {
            this.sendMessageToPN(alertJson, "AppModel");
            System.out.println("Alert " + i + " sent successfully!");
            if (i < numOfAlerts - 1) {
                try {
                    Thread.sleep(messageInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        System.out.println("All alerts sent.");
    }

    /**
     * Sends message to the Processing Node
     * @param messageText
     * @param topic
     *
     * OBS: For some reason, the message is only sent when the topic is "AppModel"
     */
    private void sendMessageToPN(String messageText, String topic) {
        ApplicationMessage message = createDefaultApplicationMessage();

        SwapData data = new SwapData();
        data.setMessage(messageText.getBytes(StandardCharsets.UTF_8));
        data.setTopic(topic);

        message.setContentObject(data);

        sendMessageToGateway(message);
    }

    @Override
    public void internalException(NodeConnection arg0, Exception arg1) {
        // TODO Auto-generated method stub
    }

    @Override
    public void unsentMessages(NodeConnection arg0, List<Message> arg1) {
        // TODO Auto-generated method stub
    }

    @Override
    public void disconnected(NodeConnection arg0) {
        // TODO Auto-generated method stub
    }

    /**
     * Send groupcast message
     *
     * @param keyboard
     */
    private void sendGroupcastMessage(Scanner keyboard) {
        String group;
        System.out.print("Groupcast message. Enter the group number: ");
        group = keyboard.nextLine();
        System.out.print("Enter the message: ");
        String messageText = keyboard.nextLine();
        System.out.println(String.format("Message from |%s| to group %s.", messageText, group));
        // Create and send the message
        SwapData groupData = new SwapData();
        groupData.setMessage(messageText.getBytes(StandardCharsets.UTF_8));
        groupData.setTopic("GroupMessageTopic");
        groupData.setRecipient(group);
        ApplicationMessage message = createDefaultApplicationMessage();
        message.setContentObject(groupData);
        sendMessageToGateway(message);
    }

    /**
     * Updates user groups by sending updated location to group definer
     * @param messageCount
     * @return context with updated location
     */
    @Override
    public SwapData newLocation(Integer messageCount) {
        ObjectMapper objMapper = new ObjectMapper();
        ObjectNode contextObj = objMapper.createObjectNode();

        String[] beacons = new String[] { "188c87bf-977f-4154-8682-958ec3335bdc" };

        contextObj.put("beacons", Arrays.toString(beacons));

        try {
            SwapData ctxData = new SwapData();
            ctxData.setContext(contextObj);
            ctxData.setDuration(60);
            return ctxData;
        } catch (Exception e) {
            logger.error("Failed to send context");
            return null;
        }
    }
}
