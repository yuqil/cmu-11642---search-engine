/**
 *  Author: Yuqi Liu <yuqil@andrew.cmu.edu>
 */

import java.util.Map;

/**
 *  An object that stores parameters for the BM25
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelIndri extends RetrievalModel {
    public int mu;
    public double lambda;

    public RetrievalModelIndri(Map<String, String> parameters) {
        try {
            int mu = Integer.parseInt(parameters.get("Indri:mu"));
            if (mu < 0.0) {
                throw new IllegalArgumentException("Error: Indri:mu= Acceptable values are integers >= 0.");
            }
            this.mu = mu;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error: Indri:mu= Acceptable values are integers >= 0.");
        }


        try {
            double lambda = Double.parseDouble(parameters.get("Indri:lambda"));
            if (lambda < 0.0 || lambda > 1.0) {
                throw new IllegalArgumentException("Error: Indri:lambda= Acceptable values are between 0.0 and 1.0.");
            }
            this.lambda = lambda;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error: Indri:lambda= Acceptable values are between 0.0 and 1.0.");
        }
    }

    public String defaultQrySopName () {
        return new String ("#and");
    }

}
