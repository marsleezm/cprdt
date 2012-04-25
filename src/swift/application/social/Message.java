package swift.application.social;

import swift.crdt.interfaces.Copyable;

public class Message implements Copyable, java.io.Serializable {
    private String msg;
    private String sender;
    private String receiver;
    private long date;

    // needed for krio
    public Message() {
    }

    public Message(String msg, String sender, String receiver, long date) {
        this.msg = msg;
        this.sender = sender;
        this.receiver = receiver;
        this.date = date;
    }

    public Object copy() {
        return new Message(msg, sender, receiver, date);
    }
}
