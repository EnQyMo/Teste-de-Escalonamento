package StressTest;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Launches N autonomous stress-test mobile nodes against a ContextNet gateway.
 *
 * <p><b>Usage:</b>
 * <pre>
 *   java -jar stress-test.jar [options]
 *
 *   --nodes N            Number of simulated mobile nodes   (default: 10)
 *   --duration S         Test duration in seconds            (default: 60)
 *   --gateway-host H     Gateway hostname or IP              (default: 127.0.0.1)
 *   --gateway-port P     Gateway UDP port                    (default: 6200)
 *   --beacons-path PATH  Path to beacons.json                (default: ../group-definer/data/beacons.json)
 *   --static N           Pin all nodes to beacons[N] (0-based index); omit for random hopping
 * </pre>
 *
 * <p><b>UUID assignment strategy:</b>
 * Each node needs a unique UUID. Because {@link main.java.ckafka.mobile.CKMobileNode} reads
 * {@code properties.xml} synchronously in its constructor, this runner writes a fresh
 * {@code properties.xml} with a new random UUID immediately before constructing each node,
 * then waits {@value #NODE_STAGGER_MS} ms before the next one. This guarantees each node
 * reads its own UUID before the file is overwritten.
 */
public class StressTestRunner {

    // ── Beacon model (matches beacons.json structure) ─────────────────────────

    /**
     * Represents one entry in {@code beacons.json}.
     */
    public static class Beacon {
        @JsonProperty("beacon_uuid")  public String uuid;
        @JsonProperty("beacon_group") public int    group;
    }

    // ── Defaults ──────────────────────────────────────────────────────────────

    private static final int    DEFAULT_NODES        = 10;
    private static final int    DEFAULT_DURATION_S   = 60;
    private static final String DEFAULT_GATEWAY_HOST = "127.0.0.1";
    private static final int    DEFAULT_GATEWAY_PORT = 6200;
    private static final String DEFAULT_BEACONS_PATH = resolveDefaultBeaconsPath();

    /**
     * Milliseconds to wait between launching consecutive nodes.
     * Prevents thundering-herd on the gateway and ensures each node reads
     * its own unique UUID from {@code properties.xml} before it is overwritten.
     */
    private static final long NODE_STAGGER_MS = 100;

    // ── Entry point ───────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {

        // ── Parse CLI arguments ────────────────────────────────────────────────
        int     numNodes    = DEFAULT_NODES;
        int     durationSec = DEFAULT_DURATION_S;
        String  gatewayHost = DEFAULT_GATEWAY_HOST;
        int     gatewayPort = DEFAULT_GATEWAY_PORT;
        String  beaconsPath = DEFAULT_BEACONS_PATH;
        Integer staticIndex = null; // null = dynamic mode

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--nodes":        numNodes    = Integer.parseInt(args[++i]); break;
                case "--duration":     durationSec = Integer.parseInt(args[++i]); break;
                case "--gateway-host": gatewayHost = args[++i];                   break;
                case "--gateway-port": gatewayPort = Integer.parseInt(args[++i]); break;
                case "--beacons-path": beaconsPath = args[++i];                   break;
                case "--static":       staticIndex = Integer.parseInt(args[++i]); break;
                default:
                    System.err.println("Unknown argument: " + args[i]);
                    printUsageAndExit();
            }
        }

        // ── Load beacons ───────────────────────────────────────────────────────
        List<Beacon> beacons = loadBeacons(beaconsPath);
        if (beacons.isEmpty()) {
            System.err.println("ERROR: No beacons found in: " + beaconsPath);
            System.exit(1);
        }

        // ── Validate --static index ────────────────────────────────────────────
        if (staticIndex != null && (staticIndex < 0 || staticIndex >= beacons.size())) {
            System.err.printf("ERROR: --static %d is out of range [0, %d].%n",
                    staticIndex, beacons.size() - 1);
            System.exit(1);
        }

        // ── Print startup banner ───────────────────────────────────────────────
        System.out.println();
        System.out.println("  ContextNet Stress Test Runner");
        System.out.println("  ==============================");
        if (staticIndex != null) {
            Beacon b = beacons.get(staticIndex);
            System.out.printf("  Mode     : STATIC  — beacon[%d] = %s  (group %d)%n",
                    staticIndex, b.uuid, b.group);
        } else {
            System.out.printf("  Mode     : DYNAMIC — %d beacons available%n", beacons.size());
        }
        System.out.printf("  Nodes    : %d%n", numNodes);
        System.out.printf("  Duration : %d s%n", durationSec);
        System.out.printf("  Gateway  : %s:%d%n", gatewayHost, gatewayPort);
        System.out.println();

        // ── Register shutdown hook ─────────────────────────────────────────────
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                System.out.println("Shutdown hook: closing node connections.")));

        // ── Spawn nodes ────────────────────────────────────────────────────────
        List<StressMobileNode> nodes = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            String uuid = UUID.randomUUID().toString();
            // Write properties.xml with this node's UUID — must happen before super()
            writePropertiesXml(uuid, gatewayHost, gatewayPort);
            StressMobileNode node = new StressMobileNode(beacons, staticIndex);
            nodes.add(node);
            System.out.printf("  [%3d/%d] started  uuid=%s%n", i + 1, numNodes, uuid);
            Thread.sleep(NODE_STAGGER_MS);
        }

        System.out.printf("%n  All %d nodes running. Waiting %d s...%n%n", numNodes, durationSec);

        // ── Wait for test duration ─────────────────────────────────────────────
        Thread.sleep(durationSec * 1000L);

        // ── Shutdown ───────────────────────────────────────────────────────────
        System.out.println("  Test duration reached. Stopping...");
        System.exit(0); // triggers shutdown hook and closes gateway connections
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Loads beacon definitions from a JSON file.
     *
     * @param path path to {@code beacons.json}
     * @return list of {@link Beacon} objects
     */
    private static List<Beacon> loadBeacons(String path) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Beacon[] array = mapper.readValue(new File(path), Beacon[].class);
        List<Beacon> list = new ArrayList<>();
        for (Beacon b : array) list.add(b);
        System.out.println("  Beacons loaded from: " + path);
        for (int i = 0; i < list.size(); i++) {
            System.out.printf("    [%d] uuid=%-38s  group=%d%n", i, list.get(i).uuid, list.get(i).group);
        }
        return list;
    }

    /**
     * Writes a {@code properties.xml} in the current working directory.
     * <p>
     * {@link main.java.ckafka.mobile.CKMobileNode} reads this file synchronously in its
     * constructor to obtain the gateway address and node UUID. Because nodes are constructed
     * sequentially with a {@value #NODE_STAGGER_MS} ms gap, each node reads its own UUID
     * before the file is overwritten for the next one.
     *
     * @param uuid        unique UUID for the node about to be constructed
     * @param gatewayHost gateway hostname or IP
     * @param gatewayPort gateway UDP port
     */
    private static void writePropertiesXml(String uuid, String gatewayHost, int gatewayPort)
            throws IOException {
        String xml =
            "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n" +
            "<!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\">\n" +
            "<properties>\n" +
            "    <comment>Auto-generated by StressTestRunner — do not edit</comment>\n" +
            "    <entry key=\"gatewayIP\">"   + gatewayHost + "</entry>\n" +
            "    <entry key=\"gatewayPort\">" + gatewayPort + "</entry>\n" +
            "    <entry key=\"uuid\">"        + uuid        + "</entry>\n" +
            "    <entry key=\"placeTag\">STRESS</entry>\n" +
            "</properties>\n";
        try (PrintWriter pw = new PrintWriter(new FileWriter("properties.xml"))) {
            pw.print(xml);
        }
    }

    /**
     * Resolves the default path to {@code beacons.json} relative to the JAR's own location,
     * so the runner works correctly regardless of the working directory.
     * <p>
     * The JAR lives at {@code stress-test/target/stress-test.jar}; walking up two levels reaches
     * the project root ({@code ContextNet-main/}), then we descend into
     * {@code data/beacons.json}.
     */
    private static String resolveDefaultBeaconsPath() {
        try {
            // getLocation() returns the URL of the JAR (or classes dir when run from IDE)
            File jar = new File(
                    StressTestRunner.class.getProtectionDomain()
                            .getCodeSource().getLocation().toURI());
            File projectRoot = jar.getParentFile()  // target/
                                  .getParentFile(); // stress-test/
            return new File(new File(projectRoot, "data"), "beacons.json")
                    .getAbsolutePath();
        } catch (Exception e) {
            // Fallback: relative to working directory
            return "data" + File.separator + "beacons.json";
        }
    }

    private static void printUsageAndExit() {
        System.err.println();
        System.err.println("Usage: java -jar stress-test.jar [options]");
        System.err.println("  --nodes N            Number of mobile nodes        (default: 10)");
        System.err.println("  --duration S         Test duration in seconds       (default: 60)");
        System.err.println("  --gateway-host H     Gateway host/IP                (default: 127.0.0.1)");
        System.err.println("  --gateway-port P     Gateway UDP port               (default: 6200)");
        System.err.println("  --beacons-path PATH  Path to beacons.json           (default: ../group-definer/data/beacons.json)");
        System.err.println("  --static N           Pin all nodes to beacons[N]    (omit for dynamic random hopping)");
        System.exit(1);
    }
}
