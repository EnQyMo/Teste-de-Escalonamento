package SmartClassroom;

import java.nio.charset.StandardCharsets;
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

public class MobileNode extends CKMobileNode {

    private static final String OPTION_GROUPCAST = "G";
    private static final String OPTION_PN = "P";
    private static final String OPTION_ALERT = "A";
    private static final String OPTION_EXIT = "Z";

    private boolean fim = false;

    private List<Integer> userIDs;
    private Map<Integer, Boolean> recebeu_mensagem;

    // ✅ CONSTRUTOR
    public MobileNode(int quantidade_ids) {
        super();

        userIDs = new ArrayList<>();
        recebeu_mensagem = new HashMap<>();

        for (int i = 0; i < quantidade_ids; i++) {
            userIDs.add(i);
            recebeu_mensagem.put(i, false);
        }
    }

    // ✅ VERIFICA SE TODOS RECEBERAM
    private boolean todos_receberam() {
        for (Boolean recebeu : recebeu_mensagem.values()) {
            if (!recebeu) return false;
        }
        return true;
    }

    // ✅ SIMULA ENVIO
    private void update() {
        int tentativas = 0;

        while (!todos_receberam() && tentativas < 200) {
            int index = (int) (Math.random() * userIDs.size());
            int userId = userIDs.get(index);

            recebeu_mensagem.put(userId, true);

            System.out.println("Enviando alerta para user " + userId);

            tentativas++;
        }

        System.out.println("Finalizado em " + tentativas + " tentativas");
    }

    // ✅ MAIN
    public static void main(String[] args) {
        Scanner keyboard = new Scanner(System.in);

        MobileNode mn = new MobileNode(30); // muda pra 1, 5 ou 30

        mn.runMN(keyboard);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            close();
        }));
    }

    private void runMN(Scanner keyboard) {
        Map<String, Consumer<Scanner>> optionsMap = new HashMap<>();

        optionsMap.put(OPTION_PN, this::enterMessageToPN);
        optionsMap.put(OPTION_ALERT, this::sendAlertToPN);
        optionsMap.put(OPTION_GROUPCAST, this::sendGroupcastMessage);
        optionsMap.put(OPTION_EXIT, scanner -> fim = true);

        while (!fim) {
            System.out.print("(G) Groupcast | (P) Message to PN | (A) Send Alert to PN | (Z) to finish)? ");
            String linha = keyboard.nextLine().trim().toUpperCase();

            if (optionsMap.containsKey(linha)) {
                optionsMap.get(linha).accept(keyboard);
            } else {
                System.out.println("Invalid option");
            }
        }

        keyboard.close();
        System.out.println("END!");
        System.exit(0);
    }

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

    @Override
    public void newMessageReceived(NodeConnection nodeConnection, Message message) {
        try {
            SwapData swp = fromMessageToSwapData(message);

            if (swp.getTopic().equals("Ping")) {
                message.setSenderID(this.mnID);
                sendMessageToGateway(message);
            }

            if (swp.getTopic().equals("StudentAttendanceCheck")) {
                String str = new String(swp.getMessage(), StandardCharsets.UTF_8);
                System.out.println("Attendance check received: " + str);
            } else {
                String str = new String(swp.getMessage(), StandardCharsets.UTF_8);
                logger.info("Message: " + str);
            }

        } catch (Exception e) {
            logger.error("Error reading message");
        }
    }

    private void enterMessageToPN(Scanner keyboard) {
        System.out.print("Enter the message: ");
        String messageText = keyboard.nextLine();
        this.sendMessageToPN(messageText, "AppModel");
    }

    private void sendAlertToPN(Scanner keyboard) {

        // 🔥 aqui roda a simulação
        update();

        String alertJson = "{ \"msg\": \"alerta\" }";

        System.out.println("Sending alert...");
        this.sendMessageToPN(alertJson, "AppModel");
    }

    private void sendMessageToPN(String messageText, String topic) {
        ApplicationMessage message = createDefaultApplicationMessage();

        SwapData data = new SwapData();
        data.setMessage(messageText.getBytes(StandardCharsets.UTF_8));
        data.setTopic(topic);

        message.setContentObject(data);

        sendMessageToGateway(message);
    }

    private void sendGroupcastMessage(Scanner keyboard) {
        System.out.print("Group: ");
        String group = keyboard.nextLine();

        System.out.print("Message: ");
        String messageText = keyboard.nextLine();

        SwapData groupData = new SwapData();
        groupData.setMessage(messageText.getBytes(StandardCharsets.UTF_8));
        groupData.setTopic("GroupMessageTopic");
        groupData.setRecipient(group);

        ApplicationMessage message = createDefaultApplicationMessage();
        message.setContentObject(groupData);

        sendMessageToGateway(message);
    }

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

    @Override
    public void internalException(NodeConnection arg0, Exception arg1) {}

    @Override
    public void unsentMessages(NodeConnection arg0, List<Message> arg1) {}

    @Override
    public void disconnected(NodeConnection arg0) {}
}
