package swift.application.swiftlinks;

import java.util.Date;

import swift.crdt.core.CRDTIdentifier;
import swift.crdt.core.Copyable;

public class User implements Copyable {
    CRDTIdentifier userId;
    String username;
    String password;
    String email;
    
    CRDTIdentifier linkList;
    CRDTIdentifier commentList;
    CRDTIdentifier subredditList;

    /** DO NOT USE: Empty constructor needed for Kryo */
    public User() {
    }

    public User(String username, String password, String email) {
        this.username = username;
        this.password = password;
        this.email = email;
        this.userId = NamingScheme.forUser(username);
        this.linkList = NamingScheme.forLinksOfUser(username);
        this.commentList = NamingScheme.forCommentsOfUser(username);
        this.subredditList = NamingScheme.forSubredditsOfUser(username);
    }
    
    public String getUsername() {
        return username;
    }

    @Override
    public Object copy() {
        User copyObj = new User(username, password, email);
        return copyObj;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(username).append(", ");
        sb.append(password).append("; ");
        sb.append(email);
        return sb.toString();
    }
}
