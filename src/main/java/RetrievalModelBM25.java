/**
 *  Author: Yuqi Liu <yuqil@andrew.cmu.edu>
 */

import java.util.Map;

/**
 *  An object that stores parameters for the BM25
 *  retrieval model (there are none) and indicates to the query
 *  operators how the query should be evaluated.
 */
public class RetrievalModelBM25 extends RetrievalModel {
    public double k_1;
    public double k_3;
    public double b;

    public RetrievalModelBM25(Map<String, String> parameters) {
        try {
            double k_1 = Double.parseDouble(parameters.get("BM25:k_1"));
            if (k_1 < 0.0) {
                throw new IllegalArgumentException("Error: BM25:k_1= Acceptable values are numbers >= 0.0.");
            }
            this.k_1 = k_1;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error: BM25:k_1= Acceptable values are numbers >= 0.0.");
        }


        try {
            double k_3 = Double.parseDouble(parameters.get("BM25:k_3"));
            if (k_3 < 0.0) {
                throw new IllegalArgumentException("Error: BM25:k_3= Acceptable values are numbers >= 0.0.");
            }
            this.k_3 = k_3;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error: BM25:k_3= Acceptable values are numbers >= 0.0.");
        }


        try {
            double b = Double.parseDouble(parameters.get("BM25:b"));
            if (b < 0.0 || b > 1.0) {
                throw new IllegalArgumentException("Error: BM25:b= Acceptable values are between 0.0 and 1.0.");
            }
            this.b = b;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error: BM25:b= Acceptable values are between 0.0 and 1.0.");
        }

        System.out.println("BM 25 parameters: " + k_1 + " " + k_3 + " " + b);
    }

    public String defaultQrySopName () {
        return new String ("#sum");
    }

}
