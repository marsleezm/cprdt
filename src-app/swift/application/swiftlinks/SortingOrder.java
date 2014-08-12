package swift.application.swiftlinks;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

/**
 * Sorting orders available on reddit
 * 
 * @author Iwan Briquemont
 * 
 */
public enum SortingOrder {
    CONFIDENCE, 
    TOP, 
    HOT, 
    CONTROVERSIAL, 
    NEW, 
    OLD;
    
    private SortingOrder() {
    }
    
    public <V extends Dateable<V>> Comparator<V> getComparator(Map<V,VoteableThing> voteFinder) {
        switch (this) {
            case CONFIDENCE:
                return new ConfidenceComparator<V>(voteFinder);
            case TOP:
                return new TopComparator<V>(voteFinder);
            case HOT:
                return new HotnessComparator<V>(voteFinder);
            case CONTROVERSIAL:
                return new ControversyComparator<V>(voteFinder);
            case OLD:
                return new DateComparator<V>(true);
            default: // NEW
                return new DateComparator<V>(true);
        }
    }
    
    static abstract class VoteableComparator <V extends Dateable<V>> implements Comparator<V>, Serializable {
        private static final long serialVersionUID = 1L;
        
        private Map<V,VoteableThing> voteFinder;
        
        public VoteableComparator(Map<V,VoteableThing> voteFinder) {
            this.voteFinder = voteFinder;
        }
        
        public int compare(V a, V b) {
            return this.compareImpl(voteFinder.get(a), voteFinder.get(b));
        }
        
        protected abstract int compareImpl(VoteableThing a, VoteableThing b);
    }
   
    static class ConfidenceComparator<V extends Dateable<V>> extends VoteableComparator<V> {
        private static final long serialVersionUID = 1L;

        public ConfidenceComparator(Map<V, VoteableThing> voteFinder) {
            super(voteFinder);
        }

        public int compareImpl(VoteableThing a, VoteableThing b) {
            double confidenceA = a.getConfidence();
            double confidenceB = a.getConfidence();
            if (confidenceA > confidenceB) {
                return -1;
            } else {
                if (confidenceA < confidenceB) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }
    
    // Rank by hotness "hottest" first
    static class HotnessComparator<V extends Dateable<V>> extends VoteableComparator<V> {
        private static final long serialVersionUID = 1L;
        
        public HotnessComparator(Map<V, VoteableThing> voteFinder) {
            super(voteFinder);
        }

        public int compareImpl(VoteableThing a, VoteableThing b) {
            double scoreA = a.getHotness();
            double scoreB = b.getHotness();
            if (scoreA > scoreB) {
                return -1;
            } else {
                if (scoreA < scoreB) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }
    
    // Rank by score, biggest score first
    static class TopComparator<V extends Dateable<V>> extends VoteableComparator<V>  {
        private static final long serialVersionUID = 1L;
        
        public TopComparator(Map<V, VoteableThing> voteFinder) {
            super(voteFinder);
        }

        public int compareImpl(VoteableThing a, VoteableThing b) {
            int scoreA = a.getScore();
            int scoreB = b.getScore();
            if (scoreA > scoreB) {
                return -1;
            } else {
                if (scoreA < scoreB) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }
    
    static class ControversyComparator<V extends Dateable<V>> extends VoteableComparator<V> {
        private static final long serialVersionUID = 1L;
        
        public ControversyComparator(Map<V, VoteableThing> voteFinder) {
            super(voteFinder);
        }

        public int compareImpl(VoteableThing a, VoteableThing b) {
            double controversyA = a.getControversy();
            double controversyB = b.getControversy();
            if (controversyA > controversyB) {
                return -1;
            } else {
                if (controversyA < controversyB) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }
    
    static class DateComparator<V extends Dateable<V>> implements Comparator<V>, Serializable {
        private static final long serialVersionUID = 1L;
        
        boolean newestFirst;
        
        public DateComparator(boolean newestFirst) {
            this.newestFirst = newestFirst;
        }

        public int compare(V a, V b) {
            long dateA = a.getDate();
            long dateB = b.getDate();
            if (dateA < dateB) {
                return (newestFirst)?1:-1;
            } else {
                if (dateA > dateB) {
                    return (newestFirst)?-1:1;
                } else {
                    return 0;
                }
            }
        }
    }
}
