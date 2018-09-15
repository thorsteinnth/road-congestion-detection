package CongestedComponents;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

/**
 * Procedure:
 * Load graph.
 * Read measurement stream.
 * Weight edges in graph with measurements (weight of edge is measurement from destination vertex).
 * Filter out uncongested edges.
 * Run connected components on the congested edges.
 * Explode the connected components state updates into table form.
 * Write results to Kafka or file sink.
 * */
public final class CongestedComponents
{
    /**
     * Name of Kafka topic to write results to
     * */
    public static String kafkaTopic = "congested_components_cclass_threshold_5_results";

    /**
     * Set congestion class threshold
     * */
    private static int congestionClassThreshold = 5;

    public static void main(String[] args) throws Exception
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("CongestedComponents.CongestedComponents")
                .config("spark.master", "local[*]")    // Run in local mode - single JVM, max threads
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Add asynchronous listener for monitoring
        spark.streams().addListener(new StreamingQueryListener() {
            @Override
            public void onQueryStarted(QueryStartedEvent queryStarted) {
                System.out.println("Query started: " + queryStarted.id());
            }
            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                System.out.println("Query terminated: " + queryTerminated.id());
            }
            @Override
            public void onQueryProgress(QueryProgressEvent queryProgress) {
                System.out.println("Query made progress: " + queryProgress.progress());
            }
        });

        // Load graph
        // Note: Only need the edges, not the vertices

        StructType edgesSchema = new StructType()
                .add("src", "string")
                .add("dst", "string")
                .add("src_X", "double")
                .add("src_Y", "double")
                .add("dst_X", "double")
                .add("dst_Y", "double")
                .add("same_road", "boolean")
                .add("is_dummy", "boolean");

        // Load graph to static DataFrame

        String edgesPath = "data/graph_6_sensors_valid_2016-5-9_to_9999-1-1_edges_base_road_all.csv";

        Dataset<Row> edges = spark
                .read()
                .schema(edgesSchema)
                .option("header", "true")
                .csv(edgesPath);

        // Generate a unique numeric ID for each vertex in the graph.

        Dataset<Row> allVertices = edges.select(edges.col("src"))
                .union(edges.select(edges.col("dst")))
                .withColumnRenamed("src", "id")
                .distinct()
                .orderBy("id");
        allVertices.createOrReplaceTempView("allVerticesView");
        Dataset<Row> allVerticesNewId = spark.sql("select row_number() over (order by 'id') as numericId, * from allVerticesView");

        Dataset<Row> edgesNumericIdTmp = edges
                .join(allVerticesNewId,
                        edges.col("src").equalTo(allVerticesNewId.col("id")))
                .withColumnRenamed("numericId", "srcNumericId")
                .drop("id");
        Dataset<Row> edgesNumericId = edgesNumericIdTmp
                .join(allVerticesNewId,
                        edgesNumericIdTmp.col("dst").equalTo(allVerticesNewId.col("id")))
                .withColumnRenamed("numericId", "dstNumericId")
                .drop("id");

        // Set up streaming DAG
        // Simulate stream source by reading csv files from file system
        String dataSourceFolder = "data/stream_input";

        StructType recordSchema = new StructType()
                .add("eventTime", "timestamp")
                .add("detectorId", "string")
                .add("road", "string")
                .add("kmRef", "integer")
                .add("detectorNumber", "integer")
                .add("flowIn", "integer")
                .add("averageSpeed", "integer")
                .add("density", "double")
                .add("vFreeflowMin", "double")
                .add("congestionClass", "integer");

        Dataset<Row> rawRecords = spark
                .readStream()
                .option("sep", ",")
                .option("header", "false")
                .schema(recordSchema)
                .option("includeTimestamp", true) // Ingestion timestamp
                .csv(dataSourceFolder);

        // Convert Row objects to measurement class
        Dataset<Measurement> measurements = rawRecords.as(Encoders.bean(Measurement.class));

        // Process each measurement
        MapFunction<Measurement, ProcessedMeasurement> processMeasurement = new MapFunction<Measurement, ProcessedMeasurement>() {
            @Override
            public ProcessedMeasurement call(Measurement measurement) throws Exception {
                return new ProcessedMeasurement(
                        measurement.getEventTime(),
                        measurement.getDetectorId(),
                        measurement.getRoad(),
                        measurement.getKmRef(),
                        measurement.getDetectorNumber(),
                        measurement.getFlowIn(),
                        measurement.getAverageSpeed(),
                        measurement.getDensity(),
                        measurement.getvFreeflowMin(),
                        measurement.getCongestionClass()
                );
            }
        };

        Dataset<ProcessedMeasurement> processedMeasurements =
                measurements.map(processMeasurement, Encoders.bean(ProcessedMeasurement.class));

        // Join processed measurements with edges,
        // so that a measurement gets placed on the edge that has the measurement sensor as dst
        // Note: Inner join. Can't do left outer join with streaming.
        Dataset<Row> processedMeasurementsEdges = processedMeasurements.join(
                edgesNumericId,
                processedMeasurements.col("detectorId").equalTo(edgesNumericId.col("dst")));

        // Filter out uncongested edges
        Dataset<Row> congestedEdges = processedMeasurementsEdges
                .filter(processedMeasurementsEdges.col("congestionClassStreaming")
                        .geq(congestionClassThreshold)
                );

        // Connected components

        // Select the necessary fields for CC
        Dataset<Row> congestedEdgesCCReady = congestedEdges
                .select("eventTime", "srcNumericId", "dstNumericId")
                .withColumnRenamed("srcNumericId", "src")
                .withColumnRenamed("dstNumericId", "dst");

        Dataset<StreamingConnectedComponents.CCStateUpdate> ccStateUpdates =
                StreamingConnectedComponents.run(congestedEdgesCCReady);

        /*
        ccUpdates is of the form
        root
                |-- eventTime: timestamp (nullable = true)
            |-- final: boolean (nullable = true)
            |-- vertexComponentMap: map (nullable = true)
            |    |-- key: integer
            |    |-- value: integer (valueContainsNull = true)

         Need to get it into rows of the form:
         eventTime, final, vertexId, componentId
         */

        FlatMapFunction<StreamingConnectedComponents.CCStateUpdate, CCStateUpdateExploded> explodeVertexIdComponentIdMap =
                new FlatMapFunction<StreamingConnectedComponents.CCStateUpdate, CCStateUpdateExploded>() {
                    @Override
                    public Iterator<CCStateUpdateExploded> call(StreamingConnectedComponents.CCStateUpdate ccStateUpdate) {
                        ArrayList<CCStateUpdateExploded> explodedStates = new ArrayList<>();
                        Timestamp eventTime = ccStateUpdate.getEventTime();
                        boolean isFinal = ccStateUpdate.isFinal();
                        for (int vertexId : ccStateUpdate.getVertexComponentMap().keySet())
                        {
                            int componentId = ccStateUpdate.getVertexComponentMap().get(vertexId);
                            explodedStates.add(new CCStateUpdateExploded(eventTime, isFinal, vertexId, componentId));
                        }
                        return explodedStates.iterator();
                    }
                };

        Dataset<CCStateUpdateExploded> ccStateUpdatesExploded =
                ccStateUpdates.flatMap(
                        explodeVertexIdComponentIdMap,
                        Encoders.bean(CCStateUpdateExploded.class)
                );

        // Get the actual ID for the numeric IDs used by CC again
        Dataset<Row> ccStateUpdatesExplodedWithIDs = ccStateUpdatesExploded.join(
                allVerticesNewId,
                ccStateUpdatesExploded.col("vertexId").equalTo(allVerticesNewId.col("numericId"))
        ).drop("numericId");

        // Generate a unique key for Kafka's key-value schema
        Dataset<Row> ccStateUpdatesExplodedWithIDsAndKey = ccStateUpdatesExplodedWithIDs.withColumn(
                "key",
                functions.concat(
                        ccStateUpdatesExplodedWithIDs.col("eventTime").cast("string"),
                        functions.lit("_"),
                        ccStateUpdatesExplodedWithIDs.col("id"),
                        functions.lit("_"),
                        ccStateUpdatesExplodedWithIDs.col("final").cast("string")
                )
        );

        // Write to console
        StreamingQuery query = ccStateUpdatesExplodedWithIDsAndKey
                .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") // Convert the whole DF to JSON
                .writeStream()
                .outputMode("update") // MapGroupsWithState requires update mode
                .format("console")
                .option("truncate", "false")
                .start();

        // Write to Kafka
        /*StreamingQuery query = ccStateUpdatesExplodedWithIDsAndKey
                .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") // Convert the whole DF to JSON
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", kafkaTopic)
                .option("checkpointLocation", "") // Insert checkpoint location here
                .outputMode("update") // MapGroupsWithState requires update mode
                .start();*/

        query.awaitTermination();
    }

    /**
     * Sensor measurement
     */
    public static class Measurement implements Serializable
    {
        private Timestamp eventTime;
        private String detectorId;
        private String road;
        private int kmRef;
        private int detectorNumber;
        private int flowIn;
        private int averageSpeed;
        private double density;
        private double vFreeflowMin;
        private int congestionClass;

        public Measurement() {
        }

        public Measurement(Timestamp eventTime, String detectorId, String road, int kmRef, int detectorNumber, int flowIn, int averageSpeed, double density, double vFreeflowMin, int congestionClass) {
            this.eventTime = eventTime;
            this.detectorId = detectorId;
            this.road = road;
            this.kmRef = kmRef;
            this.detectorNumber = detectorNumber;
            this.flowIn = flowIn;
            this.averageSpeed = averageSpeed;
            this.density = density;
            this.vFreeflowMin = vFreeflowMin;
            this.congestionClass = congestionClass;
        }

        public Timestamp getEventTime() {
            return eventTime;
        }

        public void setEventTime(Timestamp eventTime) {
            this.eventTime = eventTime;
        }

        public String getDetectorId() {
            return detectorId;
        }

        public void setDetectorId(String detectorId) {
            this.detectorId = detectorId;
        }

        public String getRoad() {
            return road;
        }

        public void setRoad(String road) {
            this.road = road;
        }

        public int getKmRef() {
            return kmRef;
        }

        public void setKmRef(int kmRef) {
            this.kmRef = kmRef;
        }

        public int getDetectorNumber() {
            return detectorNumber;
        }

        public void setDetectorNumber(int detectorNumber) {
            this.detectorNumber = detectorNumber;
        }

        public int getFlowIn() {
            return flowIn;
        }

        public void setFlowIn(int flowIn) {
            this.flowIn = flowIn;
        }

        public int getAverageSpeed() {
            return averageSpeed;
        }

        public void setAverageSpeed(int averageSpeed) {
            this.averageSpeed = averageSpeed;
        }

        public double getDensity() {
            return density;
        }

        public void setDensity(double density) {
            this.density = density;
        }

        public double getvFreeflowMin() {
            return vFreeflowMin;
        }

        public void setvFreeflowMin(double vFreeflowMin) {
            this.vFreeflowMin = vFreeflowMin;
        }

        public int getCongestionClass() {
            return congestionClass;
        }

        public void setCongestionClass(int congestionClass) {
            this.congestionClass = congestionClass;
        }
    }

    /**
     * Processed sensor measurement
     * */
    public static class ProcessedMeasurement extends Measurement
    {
        private int congestionClassStreaming;

        public ProcessedMeasurement(Timestamp eventTime, String detectorId, String road, int kmRef, int detectorNumber, int flowIn, int averageSpeed, double density, double vFreeflowMin, int congestionClass) {
            super(eventTime, detectorId, road, kmRef, detectorNumber, flowIn, averageSpeed, density, vFreeflowMin, congestionClass);
            this.congestionClassStreaming = calculateCongestionClass();
        }

        private int calculateCongestionClass()
        {
            // Stepwise depending on how far away the average speed is from v min freeflow - in increments of 5 km/h

            double diff = this.getvFreeflowMin() - this.getAverageSpeed();

            if (diff <= 0)
                return 0;

            Integer[] thresholds = {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50};

            int idx = 0;
            while (idx < thresholds.length - 1)
            {
                int lower = thresholds[idx];
                int upper = thresholds[idx + 1];
                if (diff >= lower && diff < upper)
                    return idx + 1;
                idx++;
            }

            return idx + 1;
        }

        public int getCongestionClassStreaming() {
            return congestionClassStreaming;
        }

        public void setCongestionClassStreaming(int congestionClassStreaming) {
            this.congestionClassStreaming = congestionClassStreaming;
        }
    }

    /**
     * CCState update with the vertexID-componentID map exploded
     * */
    public static class CCStateUpdateExploded implements Serializable
    {
        private Timestamp eventTime;
        private boolean isFinal;
        private int vertexId;
        private int componentId;

        public CCStateUpdateExploded() {
        }

        public CCStateUpdateExploded(Timestamp eventTime, boolean isFinal, int vertexId, int componentId) {
            this.eventTime = eventTime;
            this.isFinal = isFinal;
            this.vertexId = vertexId;
            this.componentId = componentId;
        }

        public Timestamp getEventTime() {
            return eventTime;
        }

        public void setEventTime(Timestamp eventTime) {
            this.eventTime = eventTime;
        }

        public boolean isFinal() {
            return isFinal;
        }

        public void setFinal(boolean aFinal) {
            isFinal = aFinal;
        }

        public int getVertexId() {
            return vertexId;
        }

        public void setVertexId(int vertexId) {
            this.vertexId = vertexId;
        }

        public int getComponentId() {
            return componentId;
        }

        public void setComponentId(int componentId) {
            this.componentId = componentId;
        }
    }
}
