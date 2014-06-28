package swift.application.reddit;

public class Subreddit {
    String name;
    String creator;
    
    // For Kryo
    public Subreddit() {
    }
    
    public Subreddit(String name, String creator) {
        this.name = name;
        this.creator = creator;
    }
}
