/**
 *  Copyright (c) 2016, Carnegie Mellon University.  All Rights Reserved.
 */

import java.io.*;
import java.lang.IllegalArgumentException;
import java.lang.Math.*;

/**
 *  The SCORE operator for all retrieval models.
 */
public class QrySopScore extends QrySop {

  /**
   *  Document-independent values that should be determined just once.
   *  Some retrieval models have these, some don't.
   */
  
  /**
   *  Indicates whether the query has a match.
   *  @param r The retrieval model that determines what is a match
   *  @return True if the query matches, otherwise false.
   */
  public boolean docIteratorHasMatch (RetrievalModel r) {
    return this.docIteratorHasMatchFirst (r);
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
    } else if (r instanceof RetrievalModelBM25) {
      return this.getScoreBM25((RetrievalModelBM25)r);
    } else if (r instanceof  RetrievalModelIndri) {
      return this.getScoreIndri((RetrievalModelIndri)r);
    }
    else {
      throw new IllegalArgumentException
        (r.getClass().getName() + " doesn't support the SCORE operator.");
    }
  }


  /**
   *  getScore for the Indri retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreIndri (RetrievalModelIndri r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      // get inverted list
      QryIop qry = (QryIop)this.args.get(0);
      double mu = (double)r.mu;
      double lambda = r.lambda;
      double doclen = (double)Idx.getFieldLength(qry.getField(), qry.docIteratorGetMatch());
      double tf = (double)qry.docIteratorGetMatchPosting().tf;
      long C = Idx.getSumOfFieldLengths(qry.getField());
      long ctf = qry.getCtf();

      double score = 0.0;

      double dirichlet = (tf + mu * (ctf / (double) C)) / (mu + doclen);

      score = (1 - lambda) * dirichlet + lambda * (ctf / (double) C);

      return score;
    }
  }

  public double getDefaultScore(RetrievalModel r1, int docid)
          throws IOException {
    RetrievalModelIndri r = (RetrievalModelIndri) r1;
    // get inverted list
    QryIop qry = (QryIop)this.args.get(0);
    double mu = (double)r.mu;
    double lambda = r.lambda;
    double doclen = (double)Idx.getFieldLength(qry.getField(), docid);
    double tf = 0;
    long C = Idx.getSumOfFieldLengths(qry.getField());
    long ctf = qry.getCtf();

    double score = 0.0;

    double dirichlet = (tf + mu * (ctf / (double) C)) / (mu + doclen);

    score = (1 - lambda) * dirichlet + lambda * (ctf / (double) C);

    return score;
  }


  /**
   *  getScore for the BM25 retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreBM25 (RetrievalModelBM25 r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      // get inverted list
      QryIop qry = (QryIop)this.args.get(0);

      // get parameters
      int df = qry.getDf();
      long N = Idx.getNumDocs();
      int tf = qry.docIteratorGetMatchPosting().tf;
      double avglen = Idx.getSumOfFieldLengths(qry.getField()) / (double) Idx.getDocCount(qry.getField());
      int doclen = Idx.getFieldLength(qry.getField(), qry.docIteratorGetMatch());

      // calculate BM25 score
      double RSJ = Math.log((double) (N - df + 0.5) / (double) (df + 0.5));
      RSJ = Math.max(0.0, RSJ);
      double user_weight = (double)(r.k_3 + 1) / (r.k_3 + 1) ;
      double tf_weight = (double)tf / (double) (tf + r.k_1 * ((1 - r.b) + r.b * doclen / avglen));

      double result = user_weight * RSJ * tf_weight;
      return result;
    }
  }




  /**
   *  getScore for the Unranked retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreUnrankedBoolean (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      return 1.0;
    }
  }


  /**
   *  getScore for the Ranked retrieval model.
   *  @param r The retrieval model that determines how scores are calculated.
   *  @return The document score.
   *  @throws IOException Error accessing the Lucene index
   */
  public double getScoreRankedBoolean (RetrievalModel r) throws IOException {
    if (! this.docIteratorHasMatchCache()) {
      return 0.0;
    } else {
      // the document score is the term frequency
      QryIop qry = (QryIop)this.args.get(0);
      return qry.docIteratorGetMatchPosting().tf;
    }
  }


  /**
   *  Initialize the query operator (and its arguments), including any
   *  internal iterators.  If the query operator is of type QryIop, it
   *  is fully evaluated, and the results are stored in an internal
   *  inverted list that may be accessed via the internal iterator.
   *  @param r A retrieval model that guides initialization
   *  @throws IOException Error accessing the Lucene index.
   */
  public void initialize (RetrievalModel r) throws IOException {
    Qry q = this.args.get (0);
    q.initialize (r);
  }

}
