/**
 *  Author: Yuqi Liu
 */

import java.io.*;

/**
 *  The AND operator for all retrieval models.
 */
public class QrySopAnd extends QrySop {

    /**
     *  Indicates whether the query has a match. And means all doc must contains the term
     *  @param r The retrieval model that determines what is a match
     *  @return True if the query matches, otherwise false.
     */
    public boolean docIteratorHasMatch (RetrievalModel r) {
        if (r instanceof RetrievalModelIndri) {
            return this.docIteratorHasMatchMin(r);
        }
        return this.docIteratorHasMatchAll (r);
    }

    /**
     *  Get a score for the document that docIteratorHasMatch matched.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    public double getScore (RetrievalModel r) throws IOException {

        if (r instanceof RetrievalModelUnrankedBoolean) {
            return this.getScoreUnrankedBoolean (r);
        } else if (r instanceof RetrievalModelRankedBoolean) {
            return this.getScoreRankedBoolean (r);
        } else if (r instanceof RetrievalModelIndri) {
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
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {
            double p_and = 1.0;
            double q = this.args.size();
            for (Qry qry : this.args) {
                // MIN for AND operation
                if (qry.docIteratorHasMatch(r) && qry.docIteratorGetMatch() == this.docIteratorGetMatch()) {
                    QrySop s_qry = (QrySop) qry;
                    p_and *= Math.pow(s_qry.getScore(r), 1.0 / q);
                } else {
                    QrySop s_qry = (QrySop) qry;
                    p_and *=  Math.pow(s_qry.getDefaultScore(r, this.docIteratorGetMatch()), 1.0 / q);
                }
            }
            return p_and;
        }
    }


    public double getDefaultScore(RetrievalModel r1, int docid) throws IOException {
        RetrievalModelIndri r = (RetrievalModelIndri) r1;
        double p_and = 1.0;
        double q = (double)this.args.size();
        for (Qry qry : this.args) {
            // MIN for AND operation
            if (qry.docIteratorHasMatch(r) && qry.docIteratorGetMatch() == docid) {
                QrySop s_qry = (QrySop) qry;
                p_and *= Math.pow(s_qry.getScore(r), 1.0 / q);
            } else {
                QrySop s_qry = (QrySop) qry;
                p_and *=  Math.pow(s_qry.getDefaultScore(r, docid), 1.0 / q);
            }
        }
        return p_and;
    }

    /**
     *  getScore for the UnrankedBoolean retrieval model.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    private double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {
            return 1.0;
        }
    }

    /**
     * getScore for Ranked model. Use Min for all possible score.
     * @param r rankedModel
     * @return score
     * @throws IOException
     */
    private double getScoreRankedBoolean (RetrievalModel r) throws IOException {
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {
            double min = Integer.MAX_VALUE;
            for (Qry qry : this.args) {
                // MIN for AND operation
                if (qry.docIteratorHasMatch(r) && qry.docIteratorGetMatch() == this.docIteratorGetMatch()) {
                    QrySop s_qry = (QrySop) qry;
                    min = Math.min(min, s_qry.getScore(r));
                }
            }
            return min;
        }
    }
}
