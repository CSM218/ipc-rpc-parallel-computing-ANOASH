package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker {

    private String workerId;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final ExecutorService taskPool = Executors.newFixedThreadPool(4);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private String runtimeToken;

    public Worker() {
        this.workerId = System.getenv("WORKER_ID");
        if (this.workerId == null) {
            this.workerId = "worker-" + System.currentTimeMillis();
        }
    }

    public Worker(String workerId) {
        this.workerId = workerId;
    }

    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());

            Message connect = new Message("CONNECT", workerId, null);
            connect.setPayloadFromString("INIT");
            sendMessage(connect);

            Message reg = new Message("REGISTER_WORKER", workerId, null);
            reg.setPayloadFromString("cores=" + Runtime.getRuntime().availableProcessors());
            sendMessage(reg);

            Message ack = receiveMessage();
            if (ack != null && "WORKER_ACK".equals(ack.messageType)) {
                runtimeToken = ack.getPayloadAsString();
                running.set(true);
                System.out.println("[" + workerId + "] Registered with master, token: " + runtimeToken);
            }

            Message caps = new Message("REGISTER_CAPABILITIES", workerId, null);
            caps.setPayloadFromString("MATRIX_MULTIPLY,BLOCK_MULTIPLY,SUM");
            sendMessage(caps);

        } catch (IOException e) {
            System.err.println("[" + workerId + "] Failed to join cluster: " + e.getMessage());
        }
    }

    public void execute() {
        while (running.get()) {
            try {
                Message request = receiveMessage();
                if (request == null) {
                    running.set(false);
                    break;
                }

                switch (request.messageType) {
                    case "HEARTBEAT":
                        Message hbAck = new Message("HEARTBEAT", workerId, null);
                        hbAck.setPayloadFromString("ACK");
                        sendMessage(hbAck);
                        break;

                    case "RPC_REQUEST":
                        handleTask(request);
                        break;

                    case "SHUTDOWN":
                        running.set(false);
                        break;

                    default:
                        break;
                }
            } catch (Exception e) {
                if (running.get()) {
                    System.err.println("[" + workerId + "] Error in execute loop: " + e.getMessage());
                }
                running.set(false);
            }
        }
        shutdown();
    }

    private void handleTask(Message request) {
        taskPool.submit(() -> {
            try {
                String payloadStr = request.getPayloadAsString();
                String[] parts = payloadStr.split("\\|", 3);
                String taskId = parts.length > 0 ? parts[0] : "";
                String taskType = parts.length > 1 ? parts[1] : "";
                String matrixData = parts.length > 2 ? parts[2] : "";

                String result = processMatrix(taskType, matrixData);

                Message response = new Message("TASK_COMPLETE", workerId, null);
                response.setPayloadFromString(taskId + "|" + result);
                synchronized (out) {
                    sendMessage(response);
                }
            } catch (Exception e) {
                try {
                    Message error = new Message("TASK_ERROR", workerId, null);
                    error.setPayloadFromString(e.getMessage());
                    synchronized (out) {
                        sendMessage(error);
                    }
                } catch (Exception ignored) {}
            }
        });
    }

    private String processMatrix(String taskType, String data) {
        String[] rows = data.split(";");
        if (rows.length < 2) return data;

        int splitIdx = -1;
        for (int i = 0; i < rows.length; i++) {
            if (rows[i].equals("#")) {
                splitIdx = i;
                break;
            }
        }

        if (splitIdx == -1) return data;

        int[][] matA = parseMatrix(rows, 0, splitIdx);
        int[][] matB = parseMatrix(rows, splitIdx + 1, rows.length);

        int[][] result = multiply(matA, matB);
        return matrixToString(result);
    }

    private int[][] parseMatrix(String[] rows, int start, int end) {
        int numRows = end - start;
        int[][] mat = new int[numRows][];
        for (int i = start; i < end; i++) {
            String[] vals = rows[i].split(",");
            mat[i - start] = new int[vals.length];
            for (int j = 0; j < vals.length; j++) {
                mat[i - start][j] = Integer.parseInt(vals[j].trim());
            }
        }
        return mat;
    }

    private int[][] multiply(int[][] a, int[][] b) {
        int rowsA = a.length;
        int colsA = a[0].length;
        int colsB = b[0].length;
        int[][] c = new int[rowsA][colsB];

        for (int i = 0; i < rowsA; i++) {
            for (int j = 0; j < colsB; j++) {
                int sum = 0;
                for (int k = 0; k < colsA; k++) {
                    sum += a[i][k] * b[k][j];
                }
                c[i][j] = sum;
            }
        }
        return c;
    }

    private String matrixToString(int[][] mat) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mat.length; i++) {
            for (int j = 0; j < mat[i].length; j++) {
                sb.append(mat[i][j]);
                if (j < mat[i].length - 1) sb.append(",");
            }
            if (i < mat.length - 1) sb.append(";");
        }
        return sb.toString();
    }

    private void sendMessage(Message msg) throws IOException {
        byte[] data = msg.pack();
        out.writeInt(data.length);
        out.write(data);
        out.flush();
    }

    private Message receiveMessage() throws IOException {
        int len = in.readInt();
        byte[] data = new byte[len];
        in.readFully(data);
        return Message.unpack(data);
    }

    public void shutdown() {
        running.set(false);
        taskPool.shutdownNow();
        try {
            if (socket != null && !socket.isClosed()) socket.close();
        } catch (IOException ignored) {}
    }

    public boolean isRunning() {
        return running.get();
    }

    public static void main(String[] args) {
        String host = System.getenv("MASTER_HOST");
        String portStr = System.getenv("MASTER_PORT");
        if (host == null) host = "localhost";
        int port = portStr != null ? Integer.parseInt(portStr) : 9999;

        Worker w = new Worker();
        w.joinCluster(host, port);
        w.execute();
    }
}
