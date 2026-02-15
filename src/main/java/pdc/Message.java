package pdc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Message {
    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public byte[] payload;

    public Message() {
        this.magic = "CSM218";
        this.version = 1;
        this.timestamp = System.currentTimeMillis();
    }

    public Message(String messageType, String studentId, byte[] payload) {
        this();
        this.messageType = messageType;
        this.studentId = studentId;
        this.payload = payload;
    }

    public byte[] pack() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            
            byte[] magicBytes = magic != null ? magic.getBytes(StandardCharsets.UTF_8) : new byte[0];
            dos.writeInt(magicBytes.length);
            dos.write(magicBytes);
            
            dos.writeInt(version);
            
            byte[] typeBytes = messageType != null ? messageType.getBytes(StandardCharsets.UTF_8) : new byte[0];
            dos.writeInt(typeBytes.length);
            dos.write(typeBytes);
            
            byte[] senderBytes = studentId != null ? studentId.getBytes(StandardCharsets.UTF_8) : new byte[0];
            dos.writeInt(senderBytes.length);
            dos.write(senderBytes);
            
            dos.writeLong(timestamp);
            
            int payloadLen = payload != null ? payload.length : 0;
            dos.writeInt(payloadLen);
            if (payloadLen > 0) {
                dos.write(payload);
            }
            
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Pack failed", e);
        }
    }

    public static Message unpack(byte[] data) {
        if (data == null || data.length == 0) return null;
        
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);
            
            Message msg = new Message();
            
            int magicLen = dis.readInt();
            if (magicLen > 0) {
                byte[] magicBytes = new byte[magicLen];
                dis.readFully(magicBytes);
                msg.magic = new String(magicBytes, StandardCharsets.UTF_8);
            }
            
            msg.version = dis.readInt();
            
            int typeLen = dis.readInt();
            if (typeLen > 0) {
                byte[] typeBytes = new byte[typeLen];
                dis.readFully(typeBytes);
                msg.messageType = new String(typeBytes, StandardCharsets.UTF_8);
            }
            
            int senderLen = dis.readInt();
            if (senderLen > 0) {
                byte[] senderBytes = new byte[senderLen];
                dis.readFully(senderBytes);
                msg.studentId = new String(senderBytes, StandardCharsets.UTF_8);
            }
            
            msg.timestamp = dis.readLong();
            
            int payloadLen = dis.readInt();
            if (payloadLen > 0) {
                msg.payload = new byte[payloadLen];
                dis.readFully(msg.payload);
            }
            
            return msg;
        } catch (IOException e) {
            return null;
        }
    }

    public void validate() throws IllegalStateException {
        if (!"CSM218".equals(magic)) {
            throw new IllegalStateException("Invalid magic: " + magic);
        }
        if (version != 1) {
            throw new IllegalStateException("Unsupported version: " + version);
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalStateException("Message type required");
        }
    }

    public String getPayloadAsString() {
        return payload != null ? new String(payload, StandardCharsets.UTF_8) : "";
    }

    public void setPayloadFromString(String str) {
        this.payload = str != null ? str.getBytes(StandardCharsets.UTF_8) : null;
    }
}
