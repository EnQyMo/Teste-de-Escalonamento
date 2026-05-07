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
import java.util.Random;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

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

    // ---------- Novos campos para o teste de escalonamento ----------
    private boolean isProducer;                     // true = produtor, false = consumidor
    private int myId;                               // ID deste nó
    private List<Integer> allNodeIds;               // IDs de todos os nós do sistema
    private Map<Integer, Boolean> ackRecebidos;     // produtor: confirmações dos consumidores
    private Map<Integer, Boolean> recebeuMensagem;  // consumidor: se ele mesmo recebeu alerta
    // ----------------------------------------------------------------

    //  CONSTRUTOR ORIGINAL
    public MobileNode(int quantidade_ids) {
        super();
        this.isProducer = true; // fallback
        this.myId = 0;
        this.allNodeIds = new ArrayList<>();
        this.ackRecebidos = new HashMap<>();
        this.recebeuMensagem = new HashMap<>();
        for (int i = 0; i < quantidade_ids; i++) {
            allNodeIds.add(i);
            if (isProducer) ackRecebidos.put(i, false);
            else recebeuMensagem.put(i, false);
        }
        if (isProducer) ackRecebidos.put(myId, true);
        else recebeuMensagem.put(myId, false);
    }

    // NOVO CONSTRUTOR para testes 
    public MobileNode(int myId, boolean isProducer, List<Integer> allNodeIds) {
        super();
        this.myId = myId;
        this.isProducer = isProducer;
        this.allNodeIds = new ArrayList<>(allNodeIds);
        this.ackRecebidos = new HashMap<>();
        this.recebeuMensagem = new HashMap<>();

        for (int id : allNodeIds) {
            if (isProducer) {
                ackRecebidos.put(id, false);
            } else {
                recebeuMensagem.put(id, false);
            }
        }
        // O próprio nó não precisa receber alerta de si mesmo (produtor) ou já começa sem ter recebido (consumidor)
        if (isProducer) {
            ackRecebidos.put(myId, true);
        } else {
            recebeuMensagem.put(myId, false);
        }
    }

    //  VERIFICA SE TODOS RECEBERAM (para produtor)
    private boolean todosReceberam() {
        if (!isProducer) return false;
        for (boolean recebeu : ackRecebidos.values()) {
            if (!recebeu) return false;
        }
        return true;
    }

    //  SIMULA ENVIO LOCAL (mantido para compatibilidade, mas não usado nos testes distribuídos)
    private void update() {
        int tentativas = 0;
        Random rand = new Random();
        List<Integer> targets = new ArrayList<>();
        for (int id : allNodeIds) {
            if (id != myId) targets.add(id);
        }
        while (!todosReceberam() && tentativas < 200) {
            if (targets.isEmpty()) break;
            int index = rand.nextInt(targets.size());
            int userId = targets.get(index);
            // Em ambiente local, apenas marca como recebido
            if (isProducer) ackRecebidos.put(userId, true);
            System.out.println("Enviando alerta para user " + userId);
            tentativas++;
            try { Thread.sleep(100); } catch (InterruptedException e) { break; }
        }
        System.out.println("Finalizado em " + tentativas + " tentativas");
    }

    //  NOVO MÉTODO: produtor executa o teste distribuído
    private void runProducerTest() {
        if (!isProducer) {
            System.out.println("Este nó não é o produtor. Alerta não será enviado.");
            return;
        }

        int tentativas = 0;
        Random rand = new Random();

        // Lista de IDs alvo (todos exceto o produtor)
        List<Integer> targets = new ArrayList<>();
        for (int id : allNodeIds) {
            if (id != myId) targets.add(id);
        }

        System.out.println("targets: " + targets);

        while (!todosReceberam() && tentativas < 200) {
            // Filtra pendentes
            List<Integer> pendentes = new ArrayList<>();
            for (int id : targets) {
                if (!ackRecebidos.get(id)) pendentes.add(id);
            }
            if (pendentes.isEmpty()) break;

            int targetId = pendentes.get(rand.nextInt(pendentes.size()));

            // Envia alerta para targetId via rede
            enviarAlertaPara(targetId);
            System.out.println("[PROD] Enviado para " + targetId + ". Pendentes: " + pendentes);
            //System.out.println("Produtor enviou alerta para user " + targetId);

            tentativas++;

            // Aguarda um pouco para processamento assíncrono
            try { Thread.sleep(100); } catch (InterruptedException e) { break; }
        }

        System.out.println("Produtor finalizou em " + tentativas + " tentativas. Todos receberam? " + todosReceberam());
    }


    private void enviarAlertaPara(int targetId) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode alertJson = mapper.createObjectNode();
            alertJson.put("targetId", targetId);
            alertJson.put("producerId", myId);

            String jsonStr = mapper.writeValueAsString(alertJson);

            SwapData data = new SwapData();
            data.setMessage(jsonStr.getBytes(StandardCharsets.UTF_8));
            data.setTopic("GroupMessageTopic"); // antes era Alerta
            data.setRecipient(String.valueOf(targetId)); // envia direto pro nó se Deus quiser

            ApplicationMessage msg = createDefaultApplicationMessage();
            msg.setContentObject(data);
            sendMessageToGateway(msg);
        } catch (Exception e) {
            logger.error("Erro ao enviar alerta", e);
        }
    }

    // ENVIA CONFIRMAÇÃO DO CONSUMIDOR PARA O PRODUTOR (tópico "AlertAck")
    private void enviarConfirmacao(int producerId) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode ackJson = mapper.createObjectNode();
            ackJson.put("consumerId", myId);

            String jsonStr = mapper.writeValueAsString(ackJson);

            SwapData data = new SwapData();
            data.setMessage(jsonStr.getBytes(StandardCharsets.UTF_8));
            data.setTopic("GroupMessageTopic"); // antes era AlertAck

            data.setRecipient(String.valueOf(producerId));

            ApplicationMessage msg = createDefaultApplicationMessage();
            msg.setContentObject(data);
            sendMessageToGateway(msg);

            System.out.println("[CONS " + myId + "] Confirmação enviada para " + producerId);
        } catch (Exception e) {
            logger.error("Erro ao enviar confirmação", e);
        }
    }
 
    public static void main(String[] args) {
        Scanner keyboard = new Scanner(System.in);

        MobileNode mn;
        if (args.length >= 3) {
            // Modo distribuído: java MobileNode <myId> <isProducer> <id1,id2,...>
            int myId = Integer.parseInt(args[0]);
            boolean isProducer = Boolean.parseBoolean(args[1]);
            List<Integer> allIds = new ArrayList<>();
            for (String s : args[2].split(",")) {
                allIds.add(Integer.parseInt(s));
            }
            mn = new MobileNode(myId, isProducer, allIds);
        } else {
            // Modo standalone cria 30 IDs e assume produtor
            System.out.println("Nenhum argumento fornecido. Usando modo standalone com 30 nós (produtor).");
            mn = new MobileNode(30);
        }

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

    private void subscribeToTopic(String topic) {
    try {
        ApplicationMessage subMsg = createDefaultApplicationMessage();
        SwapData data = new SwapData();
        data.setTopic(topic);
        subMsg.setContentObject(data);
        sendMessageToGateway(subMsg);
    } catch (Exception e) {
        logger.error("Falha ao inscrever no tópico " + topic, e);
    }
   }

    @Override
    public void newMessageReceived(NodeConnection nodeConnection, Message message) {
        try {
            SwapData swp = fromMessageToSwapData(message);
            String topic = swp.getTopic();

            // Tratamento de mensagens de alerta e confirmação
            if ("Alert".equals(topic)) {
                String jsonStr = new String(swp.getMessage(), StandardCharsets.UTF_8);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(jsonStr);
                int targetId = json.get("targetId").asInt();
                int producerId = json.get("producerId").asInt();
                System.out.println("[CONS " + myId + "] Mensagem Alert recebida: targetId=" + targetId + ", producerId=" + producerId);

                if (targetId == myId && !isProducer) {
                    // Este nó é o destinatário do alerta
                    System.out.println("Consumidor " + myId + " recebeu alerta do produtor " + producerId);
                    recebeuMensagem.put(myId, true);
                    // Envia confirmação de volta
                    enviarConfirmacao(producerId);
                }
                return; // já processou, não continua para outros tópicos
            }

            if ("AlertAck".equals(topic)) {
                if (isProducer) {
                    String jsonStr = new String(swp.getMessage(), StandardCharsets.UTF_8);
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode json = mapper.readTree(jsonStr);
                    int consumerId = json.get("consumerId").asInt();
                    System.out.println("[PROD] Ack recebido de " + consumerId + ". AckRecebidos: " + ackRecebidos);
                    if (ackRecebidos.containsKey(consumerId)) {
                        ackRecebidos.put(consumerId, true);
                        System.out.println("Produtor recebeu confirmação do consumidor " + consumerId);
                    }
                }
                return;
            }
            
            if ("GroupMessageTopic".equals(topic)) {
                String jsonStr = new String(swp.getMessage(), StandardCharsets.UTF_8);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode json = mapper.readTree(jsonStr);
                int targetId = json.get("\"targetID\"").asInt();
                int producerId = json.get("\"producerId\"").asInt();
                if (targetId == myId && !isProducer) {
                  System.out.println("Consumidor " + myId + "recebeu alerta do produtor " + producerId);
                  recebeuMensagem.put(myId, true);
                  enviarConfirmacao(producerId);
                } else if (jsonStr.contains("\"consumerID\"")) {
                    if (isProducer) {
                      // ObjectMapper mapper = new ObjectMapper();
                      //JsonNode json = mapper.readTree(jsonStr);
                      int consumerId = json.get("consumerId").asInt();
                      System.out.println("[PROD] Ack recebido de " + consumerId);
                      ackRecebidos.put(consumerId, true);
                  }
                }
                return;
            }

            if ("AppModel".equals(topic)) {
                String jsonStr = new String(swp.getMessage(), StandardCharsets.UTF_8);
                if (jsonStr.contains("\"targetId\"")) {
                  ObjectMapper mapper = new ObjectMapper();
                  JsonNode json = mapper.readTree(jsonStr);
                
                  int targetId = json.get("targetId").asInt();
                  int producerId = json.get("producerId").asInt();
                
                  System.out.println("[CONS " + myId + "] Alerta recebido: targetId=" + targetId);
                
                  if (targetId == myId && !isProducer) {
                      System.out.println("Consumidor " + myId + " recebeu alerta do produtor " + producerId);
                      recebeuMensagem.put(myId, true);
                      enviarConfirmacao(producerId);
                  }
                } else if (jsonStr.contains("\"consumerId\"")) {
                    if (isProducer) {
                      ObjectMapper mapper = new ObjectMapper();
                      JsonNode json = mapper.readTree(jsonStr);
                  
                      int consumerId = json.get("consumerId").asInt();
                      System.out.println("[PROD] Ack recebido de " + consumerId);
                      if (ackRecebidos.containsKey(consumerId)) {
                          ackRecebidos.put(consumerId, true);
                      }
                    }
                  } else {
                      logger.info("Message: " + jsonStr);
                    }
              return;
            }

            // Tópicos existentes (Ping, StudentAttendanceCheck, etc.)
            if (swp.getTopic().equals("Ping")) {
                message.setSenderID(this.mnID);
                sendMessageToGateway(message);
            }

            if (swp.getTopic().equals("StudentAttendanceCheck")) {
                String str = new String(swp.getMessage(), StandardCharsets.UTF_8);
                System.out.println("Attendance check received: " + str);
            } else if (!"Alert".equals(topic) && !"AlertAck".equals(topic)) {
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
        if (isProducer) {
            runProducerTest();
        } else {
            System.out.println("Este nó é consumidor. Executando simulação local (sem enviar mensagens de rede).");
            update();
        }
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
