package com.company;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TrafficTracker {
    public static void main(String[] args) throws SQLException, IOException {

        SQLDatabaseConnection MySql = new SQLDatabaseConnection();
        String filePathInputTrain = "Experiments/train_413_1.csv";

        ResultSet SegmentsId = MySql.Select(" SELECT id, gIndex FROM Segments where id = 413");
        while (SegmentsId.next()) {
            FileWriter InputTrainOrigin = new FileWriter(filePathInputTrain);

            InputTrainOrigin.append("prefCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("preCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("presCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("fCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("count");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("sCount");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("predicted");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("error");
            InputTrainOrigin.append(",");
            InputTrainOrigin.append("rangeDis");
            InputTrainOrigin.append("\n");

            InputTrainOrigin.flush();
            InputTrainOrigin.close();

            int id = Integer.parseInt(SegmentsId.getString(1));
            String gIndex = SegmentsId.getString(2);

            ResultSet Ans = MySql.Select("  SELECT  t1.gIndex, t2.id, t3.time as sTime, " +
                    "                                       DATEADD(mi, 10 ,t3.time) as eTime, " +
                    "                                       (t3.bus_count + t3.message_count) as count, " +
                    "                                       (t4.bus_count + t4.message_count ) as fcount, " +
                    "                                       (t5.bus_count + t5.message_count ) as scount " +

                    "                               FROM Grid as t1 INNER JOIN Segments as t2 ON t1.gIndex = t2.gIndex " +
                    "                                       INNER JOIN Traffics as t3 ON t3.segment_id = t2.id " +
                    "                                       INNER JOIN Traffics as t4 On t4.segment_id = t2.fNeighbor AND t4.time BETWEEN DATEADD(mi, -1 ,t3.time) AND DATEADD(mi, 1 ,t3.time) " +
                    "                                       INNER JOIN Traffics as t5 On t5.segment_id = t2.sNeighbor AND t5.time BETWEEN DATEADD(mi, -1 ,t3.time) AND DATEADD(mi, 1 ,t3.time) " +
                    "                               WHERE t2.id = " + id + " " +
                    "                               ORDER BY t3.time ASC");

            double preCount = -1;
            double prefCount = -1;
            double presCount = -1;
            int duplicateDate = 0;
            String uniqueDate = null;
            StringBuilder TrainString = new StringBuilder();

            while (Ans.next()) {
                String date = Ans.getString(3);

                if (uniqueDate == null) {
                    uniqueDate = date;
                } else if (uniqueDate.equals(date)) {
                    duplicateDate++;
                    continue;
                }
                uniqueDate = date;

                double count = Double.parseDouble(Ans.getString(5));
                preCount = (preCount < 0) ? count : preCount; // For first Row value

                double fCount = (Ans.getString(6) == null) ? 0 : Double.parseDouble(Ans.getString(6));
                prefCount = (prefCount < 0) ? fCount : prefCount; // For first Row value

                double sCount = (Ans.getString(7) == null) ? 0 : Double.parseDouble(Ans.getString(7));
                presCount = (presCount < 0) ? sCount : presCount; // For first Row value
                double x1;
                double x2;
                double x3;
                double x4;

                if (prefCount > fCount) {
                    x1 = count + (prefCount - fCount);
                    x2 = count + prefCount;
                } else {
                    x1 = count;
                    x2 = count + prefCount;
                }
                if (presCount >= sCount) {
                    x3 = count - sCount;
                    if (x3 < 0)
                        x3 = 0;
                    x4 = count;
                } else {
                    x3 = count - (sCount - presCount);
                    if (x3 < 0)
                        x3 = 0;
                    x4 = count - sCount;
                    if (x4 < 0)
                        x4 = 0;
                }
                double minVal = Math.min(x1, Math.min(x2, Math.min(x3, x4)));
                double maxVal = Math.max(x1, Math.max(x2, Math.max(x3, x4)));
                double error;
                int min = (int) (((maxVal + minVal)/2)-3);
                if (min < 0)
                    min = 0;
                int max = (int) (((maxVal + minVal)/2)+3);

                if (count <= max && count >= min) {
                    error = 0;
                } else {
                    error = Math.min(Math.abs(count - min), Math.abs(count - max));
                }
                double rangeDis = max - min;



                TrainString.append(prefCount + "," + preCount + "," + presCount + "," + fCount + "," + count + "," + sCount + "," + min + "-" + max + "," + error + "," + rangeDis + "\n");
                preCount = count;
                prefCount = fCount;
                presCount = sCount;
            }

            FileWriter InputTrain = new FileWriter(filePathInputTrain, true);
            InputTrain.append(TrainString);
            InputTrain.flush();
            InputTrain.close();
        }
    }
}
