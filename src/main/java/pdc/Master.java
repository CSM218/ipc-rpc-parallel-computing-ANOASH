package pdc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Master {

    private final ExecutorService systemThreads = Executors.newCachedThreadPool();
    private ServerSocket serverSocket;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, WorkerConnection> workers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TaskInfo> pendingTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> taskResults = new ConcurrentHashMap<>();
    private final AtomicInteger taskCounter = new AtomicInteger(0);
    private String studentId;

    public Master() {
        this.studentId = System.getenv("STUDENT_ID");
        if (this.studentId == null) this.studentId = "student";
    }

    public Master(int port) throws IOException {
        this();
        listen(port);
    }

    public Object coordinate(String operation, int[][] data, int workerCount) {
        List<WorkerConnection> available = new ArrayList<>(workers.values());
        if (available.isEmpty()) {
            return null;
        }

        int size = data.length;
        int blockSize = Math.max(1, size / Math.max(1, available.size()));
        List<TaskInfo> tasks = new ArrayList<>();

        for (int i = 0; i < size; i += blockSize) {
            int endRow = Math.min(i + blockSize, size);
            String taskId = "task-" + taskCounter.incrementAndGet();
            String taskData = encodeBlockTask(data, i, endRow);
            TaskInfo ti = new TaskInfo(taskId, operation, taskData);
            tasks.add(ti);
            pendingTasks.put(taskId, ti);
        }

        for (int i = 0; i < tasks.size(); i++) {
            TaskInfo t = tasks.get(i);
            WorkerConnection wc = available.get(i % available.size());
            t.assignedWorker = wc.workerId;
            submitTask(wc, t);
        }

        long deadline = System.currentTimeMillis() + 30000;
        while (!allTasksComplete(tasks) && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            reassignFailedTasks(tasks, available);
        }

        return aggregateResults(data, tasks);
    }

    private void submitTask(WorkerConnection wc, TaskInfo task) {
        systemThreads.submit(() -> {
            try {
                Message req = new Message("RPC_REQUEST", studentId, null);
                req.setPayloadFromString(task.taskId + "|" + task.operation + "|" + task.data);
                wc.send(req);
                task.sentTime = System.currentTimeMillis();
            } catch (Exception e) {
                wc.alive = false;
            }
        });
    }

    private boolean allTasksComplete(List<TaskInfo> tasks) {
        for (TaskInfo t : tasks) {
            if (!taskResults.containsKey(t.taskId)) return false;
        }
        return true;
    }

    private void reassignFailedTasks(List<TaskInfo> tasks, List<WorkerConnection> available) {
        long now = System.currentTimeMillis();
        for (TaskInfo t : tasks) {
            if (taskResults.containsKey(t.taskId)) continue;
            WorkerConnection assigned = workers.get(t.assignedWorker);
            boolean needReassign = assigned == null || !assigned.alive || (now - t.sentTime > 5000);
            if (needReassign) {
                for (WorkerConnection wc : available) {
                    if (wc.alive) {
                        t.assignedWorker = wc.workerId;
                        submitTask(wc, t);
                        break;
                    }
                }
            }
        }
    }

    private int[][] aggregateResults(int[][] original, List<TaskInfo> tasks) {
        int size = original.length;
        int[][] result = new int[size][];
        
        for (TaskInfo t : tasks) {
            String res = taskResults.get(t.taskId);
            if (res == null) continue;
            int[][] block = decodeMatrix(res);
            int startRow = t.startRow;
            for (int i = 0; i < block.length && (startRow + i) < size; i++) {
                result[startRow + i] = block[i];
            }
        }
        
        for (int i = 0; i < size; i++) {
            if (result[i] == null) result[i] = new int[size];
        }
        return result;
    }

    private String encodeBlockTask(int[][] data, int startRow, int endRow) {
        StringBuilder sb = new StringBuilder();
        for (int i = startRow; i < endRow; i++) {
            for (int j = 0; j < data[i].length; j++) {
                sb.append(data[i][j]);
                if (j < data[i].length - 1) sb.append(",");
            }
            sb.append(";");
        }
        sb.append("#;");
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                sb.append(data[i][j]);
                if (j < data[i].length - 1) sb.append(",");
            }
            if (i < data.length - 1) sb.append(";");
        }
        return sb.toString();
    }

    private int[][] decodeMatrix(String str) {
        String[] rows = str.split(";");
        int[][] mat = new int[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            String[] vals = rows[i].split(",");
            mat[i] = new int[vals.length];
            for (int j = 0; j < vals.length; j++) {
                mat[i][j] = Integer.parseInt(vals[j].trim());
            }
        }
        return mat;
    }

    private int[][] multiplyLocal(int[][] data) {
        int n = data.length;
        int[][] result = new int[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                for (int k = 0; k < n; k++) {
                    result[i][j] += data[i][k] * data[k][j];
                }
            }
        }
        return result;
    }

    public void listen(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(1000);
        running.set(true);
        System.out.println("Master listening on port " + port);

        systemThreads.submit(() -> {
            while (running.get()) {
                try {
                    Socket client = serverSocket.accept();
                    systemThreads.submit(() -> handleConnection(client));
                } catch (SocketTimeoutException ignored) {
                } catch (IOException e) {
                    if (running.get()) {
                        System.err.println("Accept error: " + e.getMessage());
                    }
                }
            }
        });

        systemThreads.submit(this::heartbeatLoop);
    }

    private void handleConnection(Socket client) {
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());

            int len = in.readInt();
            byte[] data = new byte[len];
            in.readFully(data);
            Message msg = Message.unpack(data);

            if ("CONNECT".equals(msg.messageType)) {
                len = in.readInt();
                data = new byte[len];
                in.readFully(data);
                msg = Message.unpack(data);
            }

            if ("REGISTER_WORKER".equals(msg.messageType)) {
                String workerId = msg.studentId;
                String token = UUID.randomUUID().toString().substring(0, 8);
                
                WorkerConnection wc = new WorkerConnection(workerId, client, in, out, token);
                workers.put(workerId, wc);
                
                Message ack = new Message("WORKER_ACK", studentId, null);
                ack.setPayloadFromString(token);
                wc.send(ack);
                
                System.out.println("Worker registered: " + workerId);
                listenToWorker(wc);
            }
        } catch (Exception e) {
            try { client.close(); } catch (IOException ignored) {}
        }
    }

    private void listenToWorker(WorkerConnection wc) {
        systemThreads.submit(() -> {
            while (running.get() && wc.alive) {
                try {
                    int len = wc.in.readInt();
                    byte[] data = new byte[len];
                    wc.in.readFully(data);
                    Message msg = Message.unpack(data);
                    
                    if (msg == null) continue;
                    
                    switch (msg.messageType) {
                        case "TASK_COMPLETE":
                            String payload = msg.getPayloadAsString();
                            int sep = payload.indexOf("|");
                            if (sep > 0) {
                                String taskId = payload.substring(0, sep);
                                String result = payload.substring(sep + 1);
                                taskResults.put(taskId, result);
                                pendingTasks.remove(taskId);
                            }
                            break;
                        case "TASK_ERROR":
                            System.err.println("Task error from " + wc.workerId + ": " + msg.getPayloadAsString());
                            break;
                        case "HEARTBEAT":
                            wc.lastHeartbeat = System.currentTimeMillis();
                            break;
                        case "REGISTER_CAPABILITIES":
                            wc.capabilities = msg.getPayloadAsString();
                            break;
                    }
                } catch (Exception e) {
                    wc.alive = false;
                    break;
                }
            }
            workers.remove(wc.workerId);
        });
    }

    private void heartbeatLoop() {
        while (running.get()) {
            try {
                Thread.sleep(2000);
                reconcileState();
            } catch (InterruptedException ignored) {}
        }
    }

    public void reconcileState() {
        long now = System.currentTimeMillis();
        for (WorkerConnection wc : workers.values()) {
            if (!wc.alive) {
                workers.remove(wc.workerId);
                continue;
            }
            try {
                Message hb = new Message("HEARTBEAT", studentId, null);
                wc.send(hb);
            } catch (Exception e) {
                wc.alive = false;
            }
            if (now - wc.lastHeartbeat > 10000) {
                wc.alive = false;
                workers.remove(wc.workerId);
            }
        }
    }

    public void shutdown() {
        running.set(false);
        for (WorkerConnection wc : workers.values()) {
            try {
                Message shutdown = new Message("SHUTDOWN", studentId, null);
                wc.send(shutdown);
                wc.socket.close();
            } catch (Exception ignored) {}
        }
        workers.clear();
        systemThreads.shutdownNow();
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
    }

    public int getWorkerCount() {
        return workers.size();
    }

    public boolean isRunning() {
        return running.get();
    }

    public static void main(String[] args) throws IOException {
        String portStr = System.getenv("MASTER_PORT");
        int port = portStr != null ? Integer.parseInt(portStr) : 9999;
        Master m = new Master(port);
        Runtime.getRuntime().addShutdownHook(new Thread(m::shutdown));
    }

    private static class WorkerConnection {
        String workerId;
        Socket socket;
        DataInputStream in;
        DataOutputStream out;
        String token;
        String capabilities;
        volatile boolean alive = true;
        volatile long lastHeartbeat;

        WorkerConnection(String workerId, Socket socket, DataInputStream in, DataOutputStream out, String token) {
            this.workerId = workerId;
            this.socket = socket;
            this.in = in;
            this.out = out;
            this.token = token;
            this.lastHeartbeat = System.currentTimeMillis();
        }

        synchronized void send(Message msg) throws IOException {
            byte[] data = msg.pack();
            out.writeInt(data.length);
            out.write(data);
            out.flush();
        }
    }

    private static class TaskInfo {
        String taskId;
        String operation;
        String data;
        String assignedWorker;
        long sentTime;
        int startRow;

        TaskInfo(String taskId, String operation, String data) {
            this.taskId = taskId;
            this.operation = operation;
            this.data = data;
        }
    }
}
