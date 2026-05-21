package StressTest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import ckafka.data.SwapData;
import lac.cnclib.net.NodeConnection;
import lac.cnclib.sddl.message.ApplicationMessage;
import lac.cnclib.sddl.message.Message;
import main.java.ckafka.mobile.CKMobileNode;
import main.java.ckafka.mobile.tasks.SendLocationTask;

/**
 * Autonomous stress-test mobile node.
 * <br>
 * Extends {@link CKMobileNode} with no keyboard/user interaction.
 * <ul>
 *   <li><b>Dynamic mode</b> ({@code staticBeaconIndex == null}): picks a random beacon on every
 *       {@code newLocation()} call, simulating node movement.</li>
 *   <li><b>Static mode</b> ({@code staticBeaconIndex = N}): always reports the same beacon,
 *       giving deterministic group membership for clean latency measurements.</li>
 * </ul>
 * When it receives a groupcast alert (content prefixed with {@code "alertKey|"}),
 * it immediately sends {@code "[ACK] alertKey group timestamp"} back to the PN via AppModel.
 */
public class StressMobileNode extends CKMobileNode {

    private final List<StressTestRunner.Beacon> beacons;
    private final Integer staticBeaconIndex;
    private final Random  random = new Random();

    /** Current group — updated in newLocation(), used in ack messages. */
    private volatile String currentGroup = "unknown";

    /**
     * Creates a stress mobile node.
     * The constructor calls {@code super()} which reads {@code properties.xml}
     * from the working directory to obtain the gateway address and UUID.
     * {@link StressTestRunner} writes a fresh {@code properties.xml} with a unique UUID
     * immediately before constructing each node.
     *
     * @param beacons           beacon list loaded from {@code beacons.json}
     * @param staticBeaconIndex {@code null} for dynamic mode; beacon index for static mode
     */
    public StressMobileNode(List<StressTestRunner.Beacon> beacons, Integer staticBeaconIndex) {
        super(); // reads properties.xml (gateway IP/port + UUID) from working directory
        this.beacons = beacons;
        this.staticBeaconIndex = staticBeaconIndex;
    }

    // ── CKMobileNode callbacks ─────────────────────────────────────────────────

    /**
     * Called when the node successfully connects to the gateway.
     * Schedules the periodic location reporting task.
     */
    @Override
    public void connected(NodeConnection nodeConnection) {
        try {
            logger.debug("Stress node connected: " + this.mnID);
            final SendLocationTask task = new SendLocationTask(this);
            this.scheduledFutureLocationTask = this.threadPool.scheduleWithFixedDelay(
                    task, 5000, 60000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error scheduling SendLocationTask", e);
        }
    }

    /**
     * Returns the location context for this node.
     * <ul>
     *   <li>Static mode: always returns {@code beacons[staticBeaconIndex]}</li>
     *   <li>Dynamic mode: picks a random beacon each call</li>
     * </ul>
     */
    @Override
    public SwapData newLocation(Integer messageCount) {
        StressTestRunner.Beacon beacon = (staticBeaconIndex != null)
                ? beacons.get(staticBeaconIndex)
                : beacons.get(random.nextInt(beacons.size()));

        currentGroup = String.valueOf(beacon.group);

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode contextObj = mapper.createObjectNode();
        contextObj.put("beacons", Arrays.toString(new String[]{ beacon.uuid }));

        try {
            SwapData ctxData = new SwapData();
            ctxData.setContext(contextObj);
            ctxData.setDuration(60);
            return ctxData;
        } catch (Exception e) {
            logger.error("Failed to build location context", e);
            return null;
        }
    }

    /**
     * Handles incoming messages from the gateway.
     * <ul>
     *   <li>If the content contains {@code "|"} (alert groupcast marker) → sends [ACK] to PN</li>
     *   <li>If topic is {@code "Ping"} → echoes back (same as standard MobileNode)</li>
     * </ul>
     */
    @Override
    public void newMessageReceived(NodeConnection nodeConnection, Message message) {
        try {
            SwapData swp = fromMessageToSwapData(message);
            String content = new String(swp.getMessage(), StandardCharsets.UTF_8);

            // Detect alert groupcast: content is prefixed with "alertKey|..."
            if ("GroupMessageTopic".equals(swp.getTopic()) && content.contains("|")) {
                String alertKey = content.split("\\|", 2)[0];
                logger.info("Alert received — alertKey=" + alertKey
                        + " group=" + currentGroup + " — sending ACK");
                sendAckToPN(alertKey);
                return;
            }

            // Ping handling (mirror of standard MobileNode behaviour)
            if ("Ping".equals(swp.getTopic())) {
                message.setSenderID(this.mnID);
                sendMessageToGateway(message);
            }

        } catch (Exception e) {
            logger.error("Error handling received message", e);
        }
    }

    @Override
    public void disconnected(NodeConnection nodeConnection) {
        logger.debug("Stress node disconnected: " + this.mnID);
    }

    @Override
    public void internalException(NodeConnection nodeConnection, Exception e) {
        logger.error("Internal exception on stress node " + this.mnID, e);
    }

    @Override
    public void unsentMessages(NodeConnection nodeConnection, List<Message> list) {
        // Not tracked in stress test
    }

    // ── Private helpers ────────────────────────────────────────────────────────

    /**
     * Sends an ACK back to the Processing Node via the existing {@code AppModel} Kafka topic.
     * Format: {@code "[ACK] <alertKey> <group> <ISO-timestamp>"}
     */
    private void sendAckToPN(String alertKey) {
        String ackText = "[ACK] " + alertKey
                + " " + currentGroup
                + " " + Instant.now().toString();
        try {
            ApplicationMessage msg = createDefaultApplicationMessage();
            SwapData data = new SwapData();
            data.setMessage(ackText.getBytes(StandardCharsets.UTF_8));
            data.setTopic("AppModel");
            msg.setContentObject(data);
            sendMessageToGateway(msg);
        } catch (Exception e) {
            logger.error("Failed to send ACK: " + ackText, e);
        }
    }
}
