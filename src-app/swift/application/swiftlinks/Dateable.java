package swift.application.swiftlinks;


/**
 * 
 * @author Iwan Briquemont
 *
 */
public interface Dateable<T> extends Comparable<T> {
    public long getDate();
}
