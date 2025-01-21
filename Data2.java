package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.mesos.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.mesos.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;
import java.io.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Holds the information of a data set. Each row contains a single data point. Primary computations
 * of PCA are performed by the Data object.
 * @author	Kushal Ranjan
 * @version	051313
 */
class Data2 {
    double[][] matrix; //matrix[i] is the ith row; matrix[i][j] is the ith row, jth column

    /**
     * Constructs a new data matrix.
     * @param vals	data for new Data object; dimensions as columns, data points as rows.
     */
    Data2(double[][] vals) {
        matrix = Matrix.copy(vals);
    }

    /**
     * Test code. Constructs an arbitrary data table of 5 data points with 3 variables, normalizes
     * it, and computes the covariance matrix and its eigenvalues and orthonormal eigenvectors.
     * Then determines the two principal components.
     */
    public static void main(String[] args) throws SQLException, IOException {
        SQLDatabaseConnection MySql = new SQLDatabaseConnection();
        StringBuilder ss = new StringBuilder();

        ResultSet SegmentsId = MySql.Select(" SELECT id from Segments");
        while (SegmentsId.next()) {
            int id = Integer.parseInt(SegmentsId.getString(1));
            String line = "";
            String line2 = "";
            double totalAccuracy = 0.0;
            double totalFalse = 0.0;
            double totalRecord = 0.0;
            double totalAnomalous = 0.0;
            double finalAccuracy = 0.0;
            double max = 0.0;
            double min = 10000.0;

            BufferedReader br = new BufferedReader(new FileReader("E:/Mirzaie/flink-1.11.2/Grid/Experiments/Test Data Set/01_2020/a_"+id+".csv"));
            br.readLine();
            while ((line = br.readLine()) != null){
                String[] l = line.split(",");
                double preCount = Double.parseDouble(l[2]);
                double new_count =  (l[9].isEmpty())?-1120.0:Double.parseDouble(l[9]);
                double isAnomalous = Double.parseDouble(l[10]);
                double[][] data = new double[][]{{preCount, preCount, new_count}};
                Data2 dat = new Data2(data);
                dat.center();

                EigenSet eigen = dat.getCovarianceEigenSet();
                double[][] vals = {eigen.values};
                double [][] res = multiplyMatrices(data , eigen.vectors);
                double [][] resRes = multiplyMatrices(res , res);
                double [][] res2 = divisionMatrices(resRes , vals);
                BufferedReader br2 = new BufferedReader(new FileReader("E:/Mirzaie/Data Sets/2020/LNM.csv"));
                double minn = 0.0;
                double maxx = 10.0;
                while ((line2 = br2.readLine()) != null) {
                    String[] l2 = line2.split(",");
                    int idd = Integer.parseInt(l2[0]);
                    if (idd == id){
                        minn = Double.parseDouble(l2[4]);
                        maxx = Double.parseDouble(l2[5]);
                    }
                }
                totalRecord += 1;
                if (isAnomalous == 1.0){
                    totalAnomalous += 1;
                    if (res2[0][0] < maxx && res2[0][0] > minn){
                        totalFalse += 1;
                    }
                }
                else{
                    if (res2[0][0] < maxx && res2[0][0] > minn){
                        totalAccuracy += 1;
                    }
                }
            }
            System.out.println(id + "," +totalRecord + "," + (totalRecord - totalAnomalous) + "," +totalAnomalous + "," +totalAccuracy + "," +(totalAnomalous-totalFalse));
            //ss.append(id + "," + totalAccuracy / totalRecord + "\n");

            //System.out.println(id + " done!");
        }/*
        FileWriter csvWriterOrigin = new FileWriter("E:/Mirzaie/Data Sets/2020/GNM.csv", true);
        csvWriterOrigin.append(ss);
        csvWriterOrigin.append("\n");
        csvWriterOrigin.flush();
        csvWriterOrigin.close();*/

    }

    /**
     * PCA implemented using the NIPALS algorithm. The return value is a double[][], where each
     * double[] j is an array of the scores of the jth data point corresponding to the desired
     * number of principal components.
     * @param input			input raw data array
     * @param numComponents	desired number of PCs
     * @return				the scores of the data array against the PCS
     */
    static double[][] PCANIPALS(double[][] input, int numComponents) {
        Data2 data = new Data2(input);
        data.center();
        double[][][] PCA = data.NIPALSAlg(numComponents);
        double[][] scores = new double[numComponents][input[0].length];
        for(int point = 0; point < scores[0].length; point++) {
            for(int comp = 0; comp < PCA.length; comp++) {
                scores[comp][point] = PCA[comp][0][point];
            }
        }
        return scores;
    }

    /**
     * Implementation of the non-linear iterative partial least squares algorithm on the data
     * matrix for this Data object. The number of PCs returned is specified by the user.
     * @param numComponents	number of principal components desired
     * @return				a double[][][] where the ith double[][] contains ti and pi, the scores
     * 						and loadings, respectively, of the ith principal component.
     */
    double[][][] NIPALSAlg(int numComponents) {
        final double THRESHOLD = 0.00001;
        double[][][] out = new double[numComponents][][];
        double[][] E = Matrix.copy(matrix);
        for(int i = 0; i < out.length; i++) {
            double eigenOld = 0;
            double eigenNew = 0;
            double[] p = new double[matrix[0].length];
            double[] t = new double[matrix[0].length];
            double[][] tMatrix = {t};
            double[][] pMatrix = {p};
            for(int j = 0; j < t.length; j++) {
                t[j] = matrix[i][j];
            }
            do {
                eigenOld = eigenNew;
                double tMult = 1/Matrix.dot(t, t);
                tMatrix[0] = t;
                p = Matrix.scale(Matrix.multiply(Matrix.transpose(E), tMatrix), tMult)[0];
                p = Matrix.normalize(p);
                double pMult = 1/Matrix.dot(p, p);
                pMatrix[0] = p;
                t = Matrix.scale(Matrix.multiply(E, pMatrix), pMult)[0];
                eigenNew = Matrix.dot(t, t);
            } while(Math.abs(eigenOld - eigenNew) > THRESHOLD);
            tMatrix[0] = t;
            pMatrix[0] = p;
            double[][] PC = {t, p}; //{scores, loadings}
            E = Matrix.subtract(E, Matrix.multiply(tMatrix, Matrix.transpose(pMatrix)));
            out[i] = PC;
        }
        return out;
    }

    /**
     * Previous algorithms for performing PCA
     */

    /**
     * Performs principal component analysis with a specified number of principal components.
     * @param input			input data; each double[] in input is an array of values of a single
     * 						variable for each data point
     * @param numComponents	number of components desired
     * @return				the transformed data set
     */
    static double[][] principalComponentAnalysis(double[][] input, int numComponents) {
        Data2 data = new Data2(input);
        data.center();
        EigenSet eigen = data.getCovarianceEigenSet();
        double[][] featureVector = data.buildPrincipalComponents(numComponents, eigen);
        double[][] PC = Matrix.transpose(featureVector);
        double[][] inputTranspose = Matrix.transpose(input);
        return Matrix.transpose(Matrix.multiply(PC, inputTranspose));
    }

    /**
     * Returns a list containing the principal components of this data set with the number of
     * loadings specified.
     * @param numComponents	the number of principal components desired
     * @param eigen			EigenSet containing the eigenvalues and eigenvectors
     * @return				the numComponents most significant eigenvectors
     */
    double[][] buildPrincipalComponents(int numComponents, EigenSet eigen) {
        double[] vals = eigen.values;
        if(numComponents > vals.length) {
            throw new RuntimeException("Cannot produce more principal components than those provided.");
        }
        boolean[] chosen = new boolean[vals.length];
        double[][] vecs = eigen.vectors;
        double[][] PC = new double[numComponents][];
        for(int i = 0; i < PC.length; i++) {
            int max = 0;
            while(chosen[max]) {
                max++;
            }
            for(int j = 0; j < vals.length; j++) {
                if(Math.abs(vals[j]) > Math.abs(vals[max]) && !chosen[j]) {
                    max = j;
                }
            }
            chosen[max] = true;
            PC[i] = vecs[max];
        }
        return PC;
    }

    /**
     * Uses the QR algorithm to determine the eigenvalues and eigenvectors of the covariance
     * matrix for this data set. Iteration continues until no eigenvalue changes by more than
     * 1/10000.
     * @return	an EigenSet containing the eigenvalues and eigenvectors of the covariance matrix
     */
    EigenSet getCovarianceEigenSet() {
        double[][] data = covarianceMatrix();
        return Matrix.eigenDecomposition(data);
    }

    /**
     * Constructs the covariance matrix for this data set.
     * @return	the covariance matrix of this data set
     */
    double[][] covarianceMatrix() {
        double[][] out = new double[matrix.length][matrix.length];
        for(int i = 0; i < out.length; i++) {
            for(int j = 0; j < out.length; j++) {
                double[] dataA = matrix[i];
                double[] dataB = matrix[j];
                out[i][j] = covariance(dataA, dataB);
            }
        }
        return out;
    }

    /**
     * Returns the covariance of two data vectors.
     * @param a	double[] of data
     * @param b	double[] of data
     * @return	the covariance of a and b, cov(a,b)
     */
    static double covariance(double[] a, double[] b) {
        if(a.length != b.length) {
            throw new MatrixException("Cannot take covariance of different dimension vectors.");
        }
        double divisor = a.length - 1;
        double sum = 0;
        double aMean = mean(a);
        double bMean = mean(b);
        for(int i = 0; i < a.length; i++) {
            sum += (a[i] - aMean) * (b[i] - bMean);
        }
        return sum/divisor;
    }

    /**
     * Centers each column of the data matrix at its mean.
     */
    void center() {
        matrix = normalize(matrix);
    }


    /**
     * Normalizes the input matrix so that each column is centered at 0.
     */
    double[][] normalize(double[][] input) {
        double[][] out = new double[input.length][input[0].length];
        for(int i = 0; i < input.length; i++) {
            double mean = mean(input[i]);
            for(int j = 0; j < input[i].length; j++) {
                out[i][j] = input[i][j] - mean;
            }
        }
        return out;
    }

    /**
     * Calculates the mean of an array of doubles.
     * @param entries	input array of doubles
     */
    static double mean(double[] entries) {
        double out = 0;
        for(double d: entries) {
            out += d/entries.length;
        }
        return out;
    }
    static double[][] multiplyMatrices(double[][] firstMatrix, double[][] secondMatrix) {
        double[][] result = new double[firstMatrix.length][secondMatrix[0].length];

        for (int row = 0; row < result.length; row++) {
            for (int col = 0; col < result[row].length; col++) {
                result[row][col] = multiplyMatricesCell(firstMatrix, secondMatrix, row, col);
            }
        }

        return result;
    }
    static double multiplyMatricesCell(double[][] firstMatrix, double[][] secondMatrix, int row, int col) {
        double cell = 0;
        for (int i = 0; i < secondMatrix.length; i++) {
            cell += firstMatrix[row][i] * secondMatrix[i][col];
        }
        return cell;
    }
    static double[][] divisionMatrices(double[][] firstMatrix, double[][] secondMatrix) {
        double[][] result = new double[firstMatrix.length][secondMatrix[0].length];

        for (int row = 0; row < result.length; row++) {
            for (int col = 0; col < result[row].length; col++) {
                //result[row][col] = firstMatrix[row][col] / secondMatrix[row][col];
                result[row][col] = divisionMatricesCell(firstMatrix, secondMatrix, row, col);
            }
        }

        return result;
    }
    static double divisionMatricesCell(double[][] firstMatrix, double[][] secondMatrix, int row, int col) {
        double cell = 0;
        for (int i = 0; i < secondMatrix.length; i++) {
            cell += firstMatrix[row][i] / secondMatrix[i][col];
        }
        return cell;
    }
}