package swift.application.reddit;

public class VoteUtils {
    // See http://amix.dk/blog/post/19588
    public static double confidence(int upvotes, int downvotes) {
        int n = upvotes + downvotes;
        if (n == 0) {
            return 0.;
        }
        double z =  1.281551565545; // 80% confidence
        double phat = ((double) upvotes) / n;
        return Math.sqrt(phat+z*z/(2*n)-z*((phat*(1-phat)+z*z/(4*n))/n))/(1+z*z/n);
    }
    
    public static double hotness(long date, int upvotes, int downvotes) {
        int score = upvotes - downvotes;
        double order = Math.log10(Math.max((double)Math.abs(score), 1.));
        int sign = 0;
        if (score > 0) {
            sign = 1;
        } else {
            if (score < 0) {
                sign = -1;
            }
        }
        long seconds = date - 1134028003;
        return order + (sign * seconds / 45000);
    }
    
    public static double controversy(int upvotes, int downvotes) {
        return (double)(upvotes + downvotes) / Math.max(Math.abs(upvotes - downvotes), 1);
    }
}
