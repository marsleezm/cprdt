package sys.stats;

public class Tally extends Tally0 {

    public Tally() {
        this("");
    }

    public Tally(String name) {
        super(name);
    }

    public String toString() {
        return String.format("#%s, %s", super.numberObs(), super.numberObs() > 2 ? super.average() : super.sum());
    }
}