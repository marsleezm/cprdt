package swift.application.reddit;

public class Subreddit {
    String name;
    
    // For Kryo
    public Subreddit() {
    }
    
    public Subreddit(String name) {
        this.name = name;
    }
}
