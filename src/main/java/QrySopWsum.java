
/**
 *  Author: Yuqi Liu
 */

import java.io.*;
import java.util.ArrayList;

/**
 *  The AND operator for all retrieval models.
 */
public class QrySopWsum extends QrySop {
    public ArrayList<Double> weight = new ArrayList<Double>();
    public double total_wight = 0;
    private double getTotalWeight() {
        double d  = 0;
        for (double weights:weight) {
            d += weights;
        }
        return d;
    }

    /**
     *  Indicates whether the query has a match. And means all doc must contains the term
     *  @param r The retrieval model that determines what is a match
     *  @return True if the query matches, otherwise false.
     */
    public boolean docIteratorHasMatch (RetrievalModel r) {
        if (r instanceof RetrievalModelIndri) {
            return this.docIteratorHasMatchMin(r);
        } else {
            throw new IllegalArgumentException
                    (r.getClass().getName() + " doesn't support the WSUM operator.");
        }
    }

    /**
     *  Get a score for the document that docIteratorHasMatch matched.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    public double getScore (RetrievalModel r) throws IOException {
        total_wight = getTotalWeight();
        if (r instanceof RetrievalModelIndri) {
            return this.getScoreIndri((RetrievalModelIndri)r);
        }
        else {
            throw new IllegalArgumentException
                    (r.getClass().getName() + " doesn't support the AND operator.");
        }
    }

    /**
     *  getScore for the UnrankedBoolean retrieval model.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    private double getScoreIndri(RetrievalModelIndri r) throws IOException {
        total_wight = getTotalWeight();
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {
            double p_and = 0.0;
            for (int i = 0; i < this.args.size(); i ++) {
                Qry qry = this.args.get(i);
                if (qry.docIteratorHasMatch(r) && qry.docIteratorGetMatch() == this.docIteratorGetMatch()) {
                    QrySop s_qry = (QrySop) qry;
                    p_and += s_qry.getScore(r) * weight.get(i) / total_wight;
                } else {
                    QrySop s_qry = (QrySop) qry;
                    p_and += s_qry.getDefaultScore(r, this.docIteratorGetMatch()) * weight.get(i) / total_wight;
                }
            }
            return p_and;
        }
    }


    public double getDefaultScore(RetrievalModel r1, int docid) throws IOException {
        total_wight = getTotalWeight();
        RetrievalModelIndri r = (RetrievalModelIndri) r1;
        double p_and = 0.0;
        double q = (double)this.args.size();
        for (int i = 0; i < this.args.size(); i ++) {
            Qry qry = this.args.get(i);
            // MIN for AND operation
            if (qry.docIteratorHasMatch(r) && qry.docIteratorGetMatch() == docid) {
                QrySop s_qry = (QrySop) qry;
                p_and += s_qry.getScore(r) * weight.get(i) / total_wight;
            } else {
                QrySop s_qry = (QrySop) qry;
                p_and +=  s_qry.getDefaultScore(r, docid) * weight.get(i) / total_wight;
            }
        }
        return p_and;
    }
}

