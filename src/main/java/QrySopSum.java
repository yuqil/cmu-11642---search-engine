/**
 *  Author: Yuqi Liu
 */

import java.io.*;

/**
 *  The AND operator for all retrieval models.
 */
public class QrySopSum extends QrySop {

    /**
     *  Indicates whether the query has a match. And means all doc must contains the term
     *  @param r The retrieval model that determines what is a match
     *  @return True if the query matches, otherwise false.
     */
    public boolean docIteratorHasMatch (RetrievalModel r) {
        return this.docIteratorHasMatchMin (r);
    }

    /**
     *  Get a score for the document that docIteratorHasMatch matched.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    public double getScore (RetrievalModel r) throws IOException {

        if (r instanceof RetrievalModelBM25) {
            return this.getScoreBM25(r);
        } else {
            throw new IllegalArgumentException
                    (r.getClass().getName() + " doesn't support the SUM operator.");
        }
    }

    /**
     *  getScore for the BM25 retrieval model.
     *  @param r The retrieval model that determines how scores are calculated.
     *  @return The document score.
     *  @throws IOException Error accessing the Lucene index
     */
    private double getScoreBM25 (RetrievalModel r) throws IOException {
        if (! this.docIteratorHasMatchCache()) {
            return 0.0;
        } else {
            double sum = 0;
            for (Qry qry : this.args) {
                if (qry.docIteratorHasMatch(r) && qry.docIteratorGetMatch() == this.docIteratorGetMatch()) {
                    QrySop s_qry = (QrySop) qry;
                    sum += s_qry.getScore(r);
                }
            }
            return sum;
        }
    }

    public double getDefaultScore(RetrievalModel r1, int docid)
            throws IOException {
        return 0.0;
    }
}
