package pdc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Worker {

    private String workerId;
    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private final ExecutorService taskPool;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private String runtimeToken;
    private static final int BUFFER_SIZE = 65536;
    private final Object sendLock = new Object();

    public Worker() {
        this.workerId = System.getenv("WORKER_ID");
        if (this.workerId == null) {
            this.workerId = "worker-" + System.currentTimeMillis();
        }
        int cores = Runtime.getRuntime().availableProcessors();
        this.taskPool = new ThreadPoolExecutor(cores, cores * 2, 60L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(100), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public Worker(String workerId) {
        this();
        this.workerId = workerId;
    }

    public void joinCluster(String masterHost, int port) {
        try {
            socket = new Socket(masterHost, port);
            socket.setTcpNoDelay(true);
            socket.setSendBufferSize(BUFFER_SIZE);
            socket.setReceiveBufferSize(BUFFER_SIZE);
            socket.setSoTimeout(30000);

            in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE));
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE));

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
            } catch (SocketException e) {
                running.set(false);
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
            String taskId = "";
            try {
                String payloadStr = request.getPayloadAsString();
                int firstPipe = payloadStr.indexOf('|');
                int secondPipe = payloadStr.indexOf('|', firstPipe + 1);

                taskId = firstPipe > 0 ? payloadStr.substring(0, firstPipe) : "";
                String taskType = secondPipe > firstPipe ? payloadStr.substring(firstPipe + 1, secondPipe) : "";
                String matrixData = secondPipe > 0 ? payloadStr.substring(secondPipe + 1) : "";

                String result = processMatrix(taskType, matrixData);

                Message response = new Message("TASK_COMPLETE", workerId, null);
                response.setPayloadFromString(taskId + "|" + result);
                sendMessage(response);
            } catch (Exception e) {
                try {
                    Message error = new Message("TASK_ERROR", workerId, null);
                    error.setPayloadFromString(taskId + "|" + e.getMessage());
                    sendMessage(error);
                } catch (Exception ignored) {}
            }
        });
    }

    private String processMatrix(String taskType, String data) {
        int hashIdx = data.indexOf("#;");
        if (hashIdx == -1) return data;

        String partA = data.substring(0, hashIdx);
        String partB = data.substring(hashIdx + 2);

        int[][] matA = parseMatrixFast(partA);
        int[][] matB = parseMatrixFast(partB);

        if (matA.length == 0 || matB.length == 0) return data;

        int[][] result = multiplyOptimized(matA, matB);
        return matrixToStringFast(result);
    }

    private int[][] parseMatrixFast(String str) {
        if (str == null || str.isEmpty()) return new int[0][0];

        int rowCount = 1;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == ';') rowCount++;
        }
        if (str.endsWith(";")) rowCount--;

        String[] rows = str.split(";");
        int[][] mat = new int[rows.length][];

        for (int i = 0; i < rows.length; i++) {
            String row = rows[i];
            if (row.isEmpty()) {
                mat[i] = new int[0];
                continue;
            }

            int colCount = 1;
            for (int j = 0; j < row.length(); j++) {
                if (row.charAt(j) == ',') colCount++;
            }

            mat[i] = new int[colCount];
            int col = 0;
            int num = 0;
            boolean negative = false;
            for (int j = 0; j <= row.length(); j++) {
                char c = j < row.length() ? row.charAt(j) : ',';
                if (c == '-') {
                    negative = true;
                } else if (c >= '0' && c <= '9') {
                    num = num * 10 + (c - '0');
                } else if (c == ',' || j == row.length()) {
                    mat[i][col++] = negative ? -num : num;
                    num = 0;
                    negative = false;
                }
            }
        }
        return mat;
    }

    private int[][] multiplyOptimized(int[][] a, int[][] b) {
        int rowsA = a.length;
        int colsA = a[0].length;
        int colsB = b[0].length;
        int[][] c = new int[rowsA][colsB];

        int[][] bT = new int[colsB][colsA];
        for (int i = 0; i < colsA; i++) {
            for (int j = 0; j < colsB; j++) {
                bT[j][i] = b[i][j];
            }
        }

        for (int i = 0; i < rowsA; i++) {
            int[] rowA = a[i];
            int[] rowC = c[i];
            for (int j = 0; j < colsB; j++) {
                int[] colB = bT[j];
                int sum = 0;
                for (int k = 0; k < colsA; k++) {
                    sum += rowA[k] * colB[k];
                }
                rowC[j] = sum;
            }
        }
        return c;
    }

    private String matrixToStringFast(int[][] mat) {
        StringBuilder sb = new StringBuilder(mat.length * mat[0].length * 4);
        for (int i = 0; i < mat.length; i++) {
            int[] row = mat[i];
            for (int j = 0; j < row.length; j++) {
                sb.append(row[j]);
                if (j < row.length - 1) sb.append(',');
            }
            if (i < mat.length - 1) sb.append(';');
        }
        return sb.toString();
    }

    private void sendMessage(Message msg) throws IOException {
        synchronized (sendLock) {
            byte[] data = msg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
        }
    }

    private Message receiveMessage() throws IOException {
        int len = in.readInt();
        if (len <= 0 || len > 100_000_000) return null;
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
