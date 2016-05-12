import org.apache.lucene.index.Term;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.util.BytesRef;

/**
 * A class for letor algorithm
 * Created by yuqil on 4/3/16.
 */
public class Letor extends RetrievalModel {
    /* TRAIN */
    private static Map<String, String> parameters;
    private static List<Integer> features = new ArrayList<Integer>();
    private static List<Integer> qids = new ArrayList<Integer>();
    private static Map<Integer, String> querys = new HashMap<Integer, String>();
    private static Map<Integer, ArrayList<Vector>> rv_judge = new HashMap<Integer, ArrayList<Vector>>();
    private static Map<String, Double> pageranks = new HashMap<String, Double>();
    private static Map<Integer, List<String>> expand_terms_train = new HashMap<Integer, List<String>>();
    private static Set<String> top_domain;
    private Pattern p = Pattern.compile("^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");

    /* TEST */
    private static List<Integer> qids_test = new ArrayList<Integer>();
    private static Map<Integer, String> querys_test = new HashMap<Integer, String>();
    private static final int MAX_FEATURE = 18;
    private static Map<Integer, ScoreList> initial_retrieval = new HashMap<Integer, ScoreList>();
    private static Map<Integer, List<String>> expand_terms_test = new HashMap<Integer, List<String>>();

    /* BM25 */
    private static double k_1;
    private static double k_3;
    private static double b;
    private static Map<String, Double> field_lengths = new HashMap<String, Double>();
    private static long N;

    /* Indri */
    private static int mu;
    private static double lambda;


    public Letor (Map<String, String> parameters) throws IOException {
        this.parameters = parameters;
        if (parameters.containsKey("BM25:k_1")) {
            this.k_1 = Double.parseDouble(parameters.get("BM25:k_1"));
            this.k_3 = Double.parseDouble(parameters.get("BM25:k_3"));
            this.b = Double.parseDouble(parameters.get("BM25:b"));
            N = Idx.getNumDocs();
            field_lengths.put("body", Idx.getSumOfFieldLengths("body") / (double)Idx.getDocCount("body"));
            field_lengths.put("title",Idx.getSumOfFieldLengths("title") / (double) Idx.getDocCount("title"));
            field_lengths.put("url", Idx.getSumOfFieldLengths("url") / (double)Idx.getDocCount("url"));
            field_lengths.put("inlink", Idx.getSumOfFieldLengths("inlink") / (double)Idx.getDocCount("inlink"));
        }

        if (parameters.containsKey("Indri:mu")) {
            mu = Integer.parseInt(parameters.get("Indri:mu"));
            lambda = Double.parseDouble(parameters.get("Indri:lambda"));
        }

        // get disabled feature
        Set<Integer> disabled_features = new HashSet<Integer>();
        if (parameters.containsKey("letor:featureDisable")) {
            String disabled = parameters.get("letor:featureDisable");
            String[] nums = disabled.split(",");
            for (String num : nums) {
                int id = Integer.parseInt(num);
                disabled_features.add(id);
            }
        }

        // get features
        for (int i = 1; i <= MAX_FEATURE; i ++) {
            if (!disabled_features.contains(i)) {
                features.add(i);
            }
        }

        // get pagerank score
        BufferedReader pagerank = new BufferedReader(new FileReader(parameters.get("letor:pageRankFile")));
        String line = pagerank.readLine();
        while (line != null) {
            String[] tokens = line.split("\t");
            double pscore = Double.parseDouble(tokens[1]);
            pageranks.put(tokens[0], pscore);
            line = pagerank.readLine();
        }
        pagerank.close();

        // set up top-level domain
        top_domain = new HashSet<String>(Arrays.asList("org", "edu", "gov", "int", "mil"));
    }

    public void process() throws Exception {
        generateTrainingData();
        trainModel();
        generateTestData();
        produceScoreSVM();
        rerank();
    }

    private void rerank() throws IOException {
        String prediction = parameters.get("letor:testingDocumentScores");
        BufferedReader reader = new BufferedReader(new FileReader(prediction));
        Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(parameters.get("trecEvalOutputPath")), "utf-8"));
        String line = null;

        for (int i = 0; i < qids_test.size(); i ++) {
            int qid = qids_test.get(i);
            ScoreList r = initial_retrieval.get(qid);
            int size = r.size();
            for (int j = 0; j < size; j ++) {
                line = reader.readLine();
                Double new_score = Double.parseDouble(line);
                r.setDocidScore(j, new_score);
            }
            r.sort();
            r.truncate(100);
            QryEval.printResults2File(String.valueOf(qid), r, writer);
            QryEval.printResults(String.valueOf(qid), r);
        }
        reader.close();
        writer.close();
    }

    private void generateTestData() throws IOException {
        readTestFile();
        RetrievalModelBM25 model = new RetrievalModelBM25(parameters);
        Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(parameters.get("letor:testingFeatureVectorsFile")), "utf-8"));
        for (int i = 0; i < qids_test.size(); i ++) {
            int qid = qids_test.get(i);
            String qLine = querys_test.get(qid);
            ScoreList r = QryEval.processQuery(qLine, model);
            r.sort();
            r.truncate(100);
            initial_retrieval.put(qid, r);
            String[] qterms = QryEval.tokenizeQuery(qLine);
            ArrayList<Vector> feature_vectors = new ArrayList<Vector>();
            generateExpandedTerm(qid, r, 1);
            for (int j = 0; j < r.size(); j ++) {
                int docid = r.getDocid(j);
                Vector vec = new Vector(qid, docid);
                computeScore(vec, qterms, 1);
                feature_vectors.add(vec);
            }
            normalize(feature_vectors);
            writeFeature2File(feature_vectors, writer);
        }
        writer.close();
    }

    private void generateTrainingData() throws IOException {
        readQueryFile();
        readRelevantJudgeFile();
        Writer writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(parameters.get("letor:trainingFeatureVectorsFile")), "utf-8"));
        for (int i = 0; i < qids.size(); i ++) {
            int qid = qids.get(i);
            String query = querys.get(qid);
            String[] qterms = QryEval.tokenizeQuery(query);
            ArrayList<Vector> rv_docs = rv_judge.get(qid);
            ScoreList r = QryEval.processQuery(query, new RetrievalModelBM25(parameters));
            generateExpandedTerm(qid, r, 0);
            for (Vector vec : rv_docs) {
                computeScore(vec, qterms, 0);
            }
            normalize(rv_docs);
            writeFeature2File(rv_docs, writer);
        }
        writer.close();
    }

    private void generateExpandedTerm(int qid, ScoreList r, int type) throws IOException {
        PriorityQueue<QryEval.ExpandTerm> pq = new PriorityQueue<QryEval.ExpandTerm>();
        ArrayList<Integer> top_docs = new ArrayList<Integer>();
        ArrayList<Double> scores = new ArrayList<Double>();
        for (int i = 0; i < 10; i ++) {
            top_docs.add(r.getDocid(i));
            scores.add(r.getDocidScore(i));
        }
        QryEval.getExpandTerm(pq, top_docs, scores, 0, 50);
        ArrayList<String> expanded_term = new ArrayList<String>();
        while (!pq.isEmpty()) expanded_term.add(pq.poll().term);
        if (type == 0) expand_terms_train.put(qid, expanded_term);
        else expand_terms_test.put(qid, expanded_term);
    }

    private void computeScore(Vector vec, String[] qterms, int type) throws IOException {
        for (int i = 0; i < features.size(); i ++) {
            int feature = features.get(i);
            computeScore(vec, qterms, feature, type);
        }
    }

    private void computeScore(Vector vec, String[] qterms, int feature, int type) throws IOException {
        switch (feature) {
            case 1:
                int spamScore = Integer.parseInt (Idx.getAttribute ("score", vec.internal_id));
                vec.addScore(spamScore);
                break;
            case 2:
                String rawUrl = Idx.getAttribute ("rawUrl", vec.internal_id);
                int count = 0;
                for (int i = 0; i < rawUrl.length(); i ++) {
                    char ch = rawUrl.charAt(i);
                    if (ch == '/') count ++;
                }
                vec.addScore(count);
                break;
            case 3:
                String rawUrl2 = Idx.getAttribute ("rawUrl", vec.internal_id);
                int wiki = 0;
                if (rawUrl2.contains("wikipedia.org")) wiki = 1;
                vec.addScore(wiki);
                break;
            case 4:
                double prkscore = -1;
                if (pageranks.containsKey(vec.doc_name)) prkscore = pageranks.get(vec.doc_name);
                vec.addScore(prkscore);
                break;
            case 5:
                double f5 = BM25(qterms, vec.internal_id, "body");
                vec.addScore(f5);
                break;
            case 6:
                double f6 = Indri(qterms, vec.internal_id, "body");
                vec.addScore(f6);
                break;
            case 7:
                double f7 = overlapScore(qterms, vec.internal_id, "body");
                vec.addScore(f7);
                break;
            case 8:
                double f8 = BM25(qterms, vec.internal_id, "title");
                vec.addScore(f8);
                break;
            case 9:
                double f9 = Indri(qterms, vec.internal_id, "title");
                vec.addScore(f9);
                break;
            case 10:
                double f10 = overlapScore(qterms, vec.internal_id, "title");
                vec.addScore(f10);
                break;
            case 11:
                double f11 = BM25(qterms, vec.internal_id, "url");
                vec.addScore(f11);
                break;
            case 12:
                double f12 = Indri(qterms, vec.internal_id, "url");
                vec.addScore(f12);
                break;
            case 13:
                double f13 = overlapScore(qterms, vec.internal_id, "url");
                vec.addScore(f13);
                break;
            case 14:
                double f14 = BM25(qterms, vec.internal_id, "inlink");
                vec.addScore(f14);
                break;
            case 15:
                double f15 = Indri(qterms, vec.internal_id, "inlink");
                vec.addScore(f15);
                break;
            case 16:
                double f16 = overlapScore(qterms, vec.internal_id, "inlink");
                vec.addScore(f16);
                break;
            case 17:
                double f17 = expandedOverlapScore(vec, "body", type);
                vec.addScore(f17);
                break;
            case 18:
                double f18 = SpamDetectScore(vec.internal_id);
                vec.addScore(f18);
                break;
        }
    }

    private double Indri(String[] qterms, int docid, String field) throws IOException {
        double score = 1.0;
        int match = 0;

        TermVector doc = new TermVector(docid, field);
        double doclen = doc.positionsLength();
        if (doclen == 0) return -1;

        long C = Idx.getSumOfFieldLengths(field);
        for (String qterm : qterms) {
            double result;
            long ctf = Idx.INDEXREADER.totalTermFreq(new Term(field, new BytesRef (qterm)));
            if (doc.indexOfStem(qterm) != -1) {
                match ++;
                int i = doc.indexOfStem(qterm);
                double tf = (double)doc.stemFreq(i);
                double dirichlet = (tf + mu * (ctf / (double) C)) / (double)(mu + doclen);
                result = (1 - lambda) * dirichlet + lambda * (ctf / (double) C);
            } else {
                double tf = 0;
                double dirichlet = (tf + mu * (ctf / (double) C)) / (mu + doclen);
                result = (1 - lambda) * dirichlet + lambda * (ctf / (double) C);
            }
            score *= result;
        }

        if (match == 0) score = 0.0;
        return Math.pow(score, 1.0 / qterms.length);
    }


    private double BM25(String[] qterms, int docid, String field) throws IOException {
        double score = 0;
        List<String> qterm = Arrays.asList(qterms);
        TermVector doc = new TermVector(docid, field);
        int size = doc.stemsLength();
        double avglen = field_lengths.get(field);
        int doclen = doc.positionsLength();
        if (doclen == 0) return -1;

        for (int i = 1; i < size; i ++) {
            String term = doc.stemString(i);
            if (qterm.contains(term)) {
                // get parameters
                int df = doc.stemDf(i);
                int tf = doc.stemFreq(i);

                // calculate BM25 score
                double RSJ = Math.log((double) (N - df + 0.5) / (double) (df + 0.5));
                RSJ = Math.max(0.0, RSJ);
                double user_weight = (double)(k_3 + 1) / (k_3 + 1) ;
                double tf_weight = (double)tf / (double) (tf + k_1 * ((1 - b) + b * doclen / avglen));

                double result = user_weight * RSJ * tf_weight;
                score += result;
            }
        }
        return score;
    }

    private double overlapScore (String[] qterms, int docid, String field) throws IOException {
        TermVector doc = new TermVector(docid, field);
        int doclen = doc.positionsLength();
        if (doclen == 0) return -1;

        int count = 0;
        for (String qterm : qterms) {
            if (doc.indexOfStem(qterm) != -1) {
                count ++;
            }
        }
        return (double) count / qterms.length;
    }

    private double expandedOverlapScore(Vector vec, String field, int type) throws IOException {
        int qid = vec.qid;
        List<String> expandterms;
        if (type == 0) {
            expandterms = expand_terms_train.get(qid);
        } else {
            expandterms = expand_terms_test.get(qid);
        }
        TermVector doc = new TermVector(vec.internal_id, field);
        int doclen = doc.positionsLength();
        if (doclen == 0) return -1;
        int count = 0;
        for (String qterm : expandterms) {
            if (doc.indexOfStem(qterm) != -1) {
                count ++;
            }
        }
        return (double) count / expandterms.size();
    }

    private double SpamDetectScore(int docid) throws IOException {
        String rawUrl = Idx.getAttribute ("rawUrl", docid);
        double score = 1.0;

        // if contains IP address, score minus 0.5
        Matcher m = p.matcher(rawUrl);
        if (m.find()) score -= 1;

        // if contains top domain, score add 1
        for (String domain : top_domain) {
            if (rawUrl.contains(domain)) score += 1.0;
        }
        return score;
    }

    private void normalize(ArrayList<Vector> rv_docs) {
        ArrayList<Double> max = new ArrayList<Double>();
        ArrayList<Double> min = new ArrayList<Double>();
        int size = rv_docs.size();
        int feature_size = features.size();

        for (int i = 0; i < feature_size; i ++) {
            max.add(Double.MIN_VALUE);
            min.add(Double.MAX_VALUE);
        }


        for (int i = 0; i < size; i ++) {
            Vector vec = rv_docs.get(i);
            for (int j = 0; j < features.size(); j ++) {
                double score = vec.scores.get(j);
                if (score == -1) continue;
                if (max.get(j) < score) {
                    max.set(j, score);
                }
                if (min.get(j) > score) {
                    min.set(j, score);
                }
            }
        }

        for (int i = 0; i < feature_size; i ++) {
            double maxval = max.get(i);
            double minval = min.get(i);
            if (minval == maxval || (maxval == Double.MIN_VALUE && minval == Double.MAX_VALUE)) {
                for (int j = 0; j < size; j ++) {
                    Vector vec = rv_docs.get(j);
                    vec.scores.set(i, 0.0);
                }
                continue;
            }

            for (int j = 0; j < size; j ++) {
                Vector vec = rv_docs.get(j);
                double score = vec.scores.get(i);
                if (score == -1) {
                    vec.scores.set(i, 0.0);
                    continue;
                }
                double normal = (score - minval) / (maxval - minval);
                vec.scores.set(i, normal);
            }
        }
    }

    private void readQueryFile() throws IOException {
        BufferedReader input = new BufferedReader(new FileReader(parameters.get("letor:trainingQueryFile")));
        String qLine = input.readLine();
        while (qLine != null) {
            int d = qLine.indexOf(':');
            if (d < 0) throw new IllegalArgumentException("Syntax error:  Missing ':' in query line.");
            int qid = Integer.parseInt(qLine.substring(0, d));
            String query = qLine.substring(d + 1);
            qids.add(qid);
            querys.put(qid, query);
            qLine = input.readLine();
        }
        Collections.sort(qids);
        input.close();
    }

    private void readTestFile() throws IOException {
        BufferedReader input = new BufferedReader(new FileReader(parameters.get("queryFilePath")));
        String qLine = input.readLine();
        while (qLine != null) {
            int d = qLine.indexOf(':');
            if (d < 0) throw new IllegalArgumentException("Syntax error:  Missing ':' in query line.");
            int qid = Integer.parseInt(qLine.substring(0, d));
            String query = qLine.substring(d + 1);
            qids_test.add(qid);
            querys_test.put(qid, query);
            qLine = input.readLine();
        }
        Collections.sort(qids_test);
        input.close();
    }

    private void readRelevantJudgeFile() throws IOException {
        BufferedReader file = new BufferedReader(new FileReader(parameters.get("letor:trainingQrelsFile")));
        String fLine = file.readLine();
        while (fLine != null) {
            String[] tokens = fLine.split(" ");
            int qid = Integer.parseInt(tokens[0]);
            try {
                int internal_id = Idx.getInternalDocid(tokens[2]);
                if (!rv_judge.containsKey(qid)) {
                    rv_judge.put(qid, new ArrayList<Vector>());
                }
                rv_judge.get(qid).add(new Vector(fLine, internal_id));
            } catch (Exception e) {
//                System.err.println(e.getMessage());
            }
            fLine = file.readLine();
        }
        file.close();
    }


    private void writeFeature2File (ArrayList<Vector> rv_docs, Writer writer) throws IOException {
        for (int i = 0; i < rv_docs.size(); i ++) {
            writer.write(rv_docs.get(i).toString());
        }
    }

    private void trainModel() throws Exception {
        // runs svm_rank_learn from within Java to train the model
        // execPath is the location of the svm_rank_learn utility,
        // which is specified by letor:svmRankLearnPath in the parameter file.
        // FEAT_GEN.c is the value of the letor:c parameter.
        String qrelsFeatureOutputFile = parameters.get("letor:trainingFeatureVectorsFile");
        String execPath = parameters.get("letor:svmRankLearnPath");
        String modelOutputFile = parameters.get("letor:svmRankModelFile");
        String c = parameters.get("letor:svmRankParamC");
        String [] cmd = new String[] { execPath, "-c", String.valueOf(c), qrelsFeatureOutputFile, modelOutputFile };
        System.out.println(cmd[0] + " " + cmd[1] + " " + cmd[2] + " " + cmd[3] + " " + cmd[4]);
        Process cmdProc = Runtime.getRuntime().exec(cmd);

        // The stdout/stderr consuming code MUST be included.
        // It prevents the OS from running out of output buffer space and stalling.
        // consume stdout and print it out for debugging purposes
        BufferedReader stdoutReader = new BufferedReader(
                new InputStreamReader(cmdProc.getInputStream()));
        String line;
        while ((line = stdoutReader.readLine()) != null) {
            System.out.println(line);
        }

        // consume stderr and print it for debugging purposes
        BufferedReader stderrReader = new BufferedReader(
                new InputStreamReader(cmdProc.getErrorStream()));
        while ((line = stderrReader.readLine()) != null) {
            System.out.println(line);
        }

        // get the return value from the executable. 0 means success, non-zero
        // indicates a problem
        int retValue = cmdProc.waitFor();
        if (retValue != 0) {
            throw new Exception("SVM Rank crashed.");
        }
    }


    private void produceScoreSVM() throws Exception {
        // runs svm_rank_learn from within Java to train the model
        // execPath is the location of the svm_rank_learn utility,
        // which is specified by letor:svmRankLearnPath in the parameter file.
        // FEAT_GEN.c is the value of the letor:c parameter.
        // svm_rank_classify test.dat model.dat predictions

        String execPath = parameters.get("letor:svmRankClassifyPath");
        String modelData = parameters.get("letor:svmRankModelFile");
        String testData = parameters.get("letor:testingFeatureVectorsFile");
        String prediction = parameters.get("letor:testingDocumentScores");
        Process cmdProc = Runtime.getRuntime().exec(
                new String[] { execPath, testData, modelData, prediction});

        // The stdout/stderr consuming code MUST be included.
        // It prevents the OS from running out of output buffer space and stalling.
        // consume stdout and print it out for debugging purposes
        BufferedReader stdoutReader = new BufferedReader(
                new InputStreamReader(cmdProc.getInputStream()));
        String line;
        while ((line = stdoutReader.readLine()) != null) {
            System.out.println(line);
        }

        // consume stderr and print it for debugging purposes
        BufferedReader stderrReader = new BufferedReader(
                new InputStreamReader(cmdProc.getErrorStream()));
        while ((line = stderrReader.readLine()) != null) {
            System.out.println(line);
        }

        // get the return value from the executable. 0 means success, non-zero
        // indicates a problem
        int retValue = cmdProc.waitFor();
        if (retValue != 0) {
            throw new Exception("SVM Rank crashed.");
        }
    }


    public String defaultQrySopName () {
        return new String ("#ERROR");
    }

    private class Vector implements Comparable<Vector> {
        private ArrayList<Double> scores = new ArrayList<Double>();
        private int internal_id = -1;
        private int qid = -1;
        private int rv_score = -1;
        private String doc_name = null;

        public Vector(String fline, int id) {
            String[] tokens = fline.split(" ");
            this.qid = Integer.parseInt(tokens[0]);
            this.rv_score = Integer.parseInt(tokens[3]);
            this.doc_name = tokens[2];
            this.internal_id = id;
        }

        public Vector(int qid, int id) throws IOException {
            this.qid = qid;
            this.internal_id = id;
            this.doc_name = Idx.getExternalDocid(id);
            this.rv_score = 0;
        }

        public void addScore(double score) {
            scores.add(score);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(rv_score + " qid:" + qid + " ");
            for (int i = 0; i < features.size(); i ++) {
                sb.append(features.get(i) + ":" + scores.get(i) + " ");
            }
            sb.append("# " + doc_name + "\n");
            return sb.toString();
        }

        public int compareTo(Vector v2) {
            if (this.rv_score < v2.rv_score) return -1;
            else if (this.rv_score > v2.rv_score) return 1;
            else return 0;
        }
    }
}
