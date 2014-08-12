package swift.application.swiftlinks;

public class Forum {
    String name;
    String creator;
    
    // For Kryo
    public Forum() {
    }
    
    public Forum(String name, String creator) {
        this.name = name;
        this.creator = creator;
    }
}
