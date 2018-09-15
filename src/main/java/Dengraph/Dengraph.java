package Dengraph;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.StreamingQueryListener;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;
import Dengraph.StreamingIncrementalDengraph.*;

public final class Dengraph
{
    /**
     * Name of Kafka topic to write results to
     * */
    public static String kafkaTopic = "dengraph_results";

    /**
     * Dengraph parameter - neighborhood radius (epsilon)
     * */
    private static double neighborhoodRadius = 0.035;

    /**
     * Dengraph parameter - minimum number of nodes in neighborhood (eta)
     * */
    private static int minNumberOfNodesInNeighborhood = 2;

    public static void main(String[] args) throws Exception
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("Dengraph.DengraphPipeline")
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

        // Load graph to static DataFrame
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

        String edgesPath = "data/graph_6_sensors_valid_2016-5-9_to_9999-1-1_edges_base_road_all.csv";

        Dataset<Row> rawEdges = spark
                .read()
                .schema(edgesSchema)
                .option("header", "true")
                .csv(edgesPath);

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

        // Join measurements with edges,
        // so that a measurement gets placed on the edge that has the measurement sensor as dst
        // Note: Inner join. Can't do left outer join with streaming.
        Dataset<Row> measurementsEdges = measurements.join(
                rawEdges,
                measurements.col("detectorId").equalTo(rawEdges.col("dst")));

        // Convert to stream of weighted edges
        String weightField = "density";
        Dataset<Row> measurementsEdgesCleaned = measurementsEdges.select("eventTime", "src", "dst", weightField)
                .withColumnRenamed(weightField, "weight");
        Dataset<Edge> edges = measurementsEdgesCleaned.as(Encoders.bean(Edge.class));

        // Convert to stream of Dengraph edges (undirected, with distance measure)
        MapFunction<Edge, DenGraphEdge> convertToDengraphEdge = new MapFunction<Edge, DenGraphEdge>() {
            @Override
            public DenGraphEdge call(Edge edge) throws Exception {
                return new DenGraphEdge(edge.eventTime, edge.src, edge.dst, edge.weight);
            }
        };
        Dataset<DenGraphEdge> dengraphEdges =
                edges.map(convertToDengraphEdge, Encoders.bean(DenGraphEdge.class));

        // Dengraph

        MapGroupsWithStateFunction<Timestamp, DenGraphEdge, DengraphState, DengraphStateUpdate> updateDengraphState
                = new MapGroupsWithStateFunction<Timestamp, DenGraphEdge, DengraphState, DengraphStateUpdate>() {
            @Override
            public DengraphStateUpdate call(Timestamp timestamp, Iterator<DenGraphEdge> iterator, GroupState<DengraphState> groupState) throws Exception {

                DengraphState currentState;

                if (groupState.exists())
                {
                    // When Spark deserializes the GroupState into a Java class it makes the maps be AbstractMap
                    // That doesn't have a put() operation.
                    // Creating a new DengraphState object and setting the maps as HashMaps.
                    currentState = new DengraphState();
                    currentState.setEdgeMap(new HashMap<>(groupState.get().getEdgeMap()));
                    currentState.setVertexStates(new HashMap<>(groupState.get().getVertexStates()));
                }
                else
                {
                    currentState = new DengraphState();
                }

                if (groupState.hasTimedOut())
                {
                    // State has timed out, send final update and remove state
                    DengraphStateUpdate finalUpdate = new DengraphStateUpdate(
                            timestamp,true, groupState.get().getVertexStates()
                    );
                    groupState.remove();
                    return finalUpdate;
                }

                IncrementalDengraph dengraph =
                        new IncrementalDengraph(currentState, neighborhoodRadius, minNumberOfNodesInNeighborhood);

                while (iterator.hasNext())
                {
                    DenGraphEdge newEdge = iterator.next();
                    dengraph.addEdge(newEdge);
                }

                DengraphState newState = new DengraphState();
                newState.setEdgeMap(currentState.getEdgeMap());
                newState.setVertexStates(currentState.getVertexStates());

                groupState.update(newState);

                // Processing time timeout
                // Set timeout such that the session will be expired if no data received for 20 seconds
                //groupState.setTimeoutDuration("20 seconds");

                // Event time timeout with watermarks
                // State is timed out when the watermark advances beyond this timeout timestamp
                // Set as 30 seconds so that when the watermark reaches the next minute, this minute is timed out
                groupState.setTimeoutTimestamp(timestamp.getTime(), "30 seconds");

                return new DengraphStateUpdate(
                        timestamp, false, groupState.get().getVertexStates());
            }
        };

        Dataset<DengraphStateUpdate> dengraphStateUpdates = dengraphEdges
                // How late data is allowed to be, else dropped.
                // 0 seconds -> watermark follows the max event time.
                .withWatermark("eventTime", "0 seconds")
                .groupByKey(
                        new MapFunction<DenGraphEdge, Timestamp>() {
                            @Override
                            public Timestamp call(DenGraphEdge denGraphEdge)
                            {
                                return denGraphEdge.getEventTime();
                            }
                        }, Encoders.TIMESTAMP())
                .mapGroupsWithState(
                        updateDengraphState,
                        Encoders.bean(DengraphState.class),
                        Encoders.bean(DengraphStateUpdate.class),
                        GroupStateTimeout.EventTimeTimeout()
                );

        /*
        Explode dengraphStateUpdates

        dengraphStateUpdates is of the form:

        root
        |-- eventTime: timestamp (nullable = true)
        |-- final: boolean (nullable = true)
        |-- vertexStates: map (nullable = true)
        |    |-- key: string
        |    |-- value: struct (valueContainsNull = true)
        |    |    |-- clusterId: integer (nullable = true)
        |    |    |-- id: string (nullable = true)
        |    |    |-- vertexType: string (nullable = true)

        Need to get it into rows of the form:
        eventTime, final, vertexId, clusterId, vertexType
        */

        FlatMapFunction<DengraphStateUpdate, DengraphStateUpdateExploded> explodeDengraphStateUpdate =
                new FlatMapFunction<DengraphStateUpdate, DengraphStateUpdateExploded>() {
                    @Override
                    public Iterator<DengraphStateUpdateExploded> call(DengraphStateUpdate dengraphStateUpdate) {
                        List<DengraphStateUpdateExploded> explodedStates = new ArrayList<>();
                        Timestamp eventTime = dengraphStateUpdate.getEventTime();
                        boolean isFinal = dengraphStateUpdate.isFinal();
                        for (String vertexId : dengraphStateUpdate.getVertexStates().keySet())
                        {
                            VertexState vertexState = dengraphStateUpdate.getVertexStates().get(vertexId);
                            DengraphStateUpdateExploded explodedState = new DengraphStateUpdateExploded(
                                    eventTime,
                                    isFinal,
                                    vertexState.getId(),
                                    vertexState.getClusterId(),
                                    vertexState.getVertexType().toString()
                            );
                            explodedStates.add(explodedState);
                        }
                        return explodedStates.iterator();
                    }
                };

        Dataset<DengraphStateUpdateExploded> dengraphStateUpdatesExploded =
                dengraphStateUpdates.flatMap(
                        explodeDengraphStateUpdate,
                        Encoders.bean(DengraphStateUpdateExploded.class)
                );

        // Generate a unique key for Kafka's key-value schema
        Dataset<Row> dengraphStateUpdatesExplodedWithKey = dengraphStateUpdatesExploded.withColumn(
                "key",
                functions.concat(
                        dengraphStateUpdatesExploded.col("eventTime").cast("string"),
                        functions.lit("_"),
                        dengraphStateUpdatesExploded.col("vertexId"),
                        functions.lit("_"),
                        dengraphStateUpdatesExploded.col("final").cast("string")
                )
        );

        // Write to console
        StreamingQuery query = dengraphStateUpdatesExplodedWithKey
                .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") // Convert the whole DF to JSON
                .writeStream()
                .outputMode("update") // MapGroupsWithState requires update mode
                .format("console")
                .option("truncate", "false")
                .start();

        // Write to Kafka
        /*
        StreamingQuery query = dengraphStateUpdatesExplodedWithKey
                .selectExpr("CAST(key AS STRING)", "to_json(struct(*)) AS value") // Convert the whole DF to JSON
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", kafkaTopic)
                .option("checkpointLocation", "") // Checkpoint dir location here
                .outputMode("update") // MapGroupsWithState requires update mode
                .start();
                */

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
     * Weighted edge
     * */
    public static class Edge implements Serializable
    {
        private Timestamp eventTime;
        private String src;
        private String dst;
        private double weight;

        public Edge() {
        }

        public Edge(Timestamp eventTime, String src, String dst, double weight) {
            this.eventTime = eventTime;
            this.src = src;
            this.dst = dst;
            this.weight = weight;
        }

        public Timestamp getEventTime() {
            return eventTime;
        }

        public void setEventTime(Timestamp eventTime) {
            this.eventTime = eventTime;
        }

        public String getSrc() {
            return src;
        }

        public void setSrc(String src) {
            this.src = src;
        }

        public String getDst() {
            return dst;
        }

        public void setDst(String dst) {
            this.dst = dst;
        }

        public double getWeight() {
            return weight;
        }

        public void setWeight(double weight) {
            this.weight = weight;
        }
    }

    /**
     * DengraphState update with the vertex states map exploded
     * */
    public static class DengraphStateUpdateExploded implements Serializable
    {
        private Timestamp eventTime;
        private boolean isFinal;
        private String vertexId;
        private int clusterId;
        private String vertexType;

        public DengraphStateUpdateExploded() {
        }

        public DengraphStateUpdateExploded(Timestamp eventTime, boolean isFinal, String vertexId, int clusterId, String vertexType) {
            this.eventTime = eventTime;
            this.isFinal = isFinal;
            this.vertexId = vertexId;
            this.clusterId = clusterId;
            this.vertexType = vertexType;
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

        public String getVertexId() {
            return vertexId;
        }

        public void setVertexId(String vertexId) {
            this.vertexId = vertexId;
        }

        public int getClusterId() {
            return clusterId;
        }

        public void setClusterId(int clusterId) {
            this.clusterId = clusterId;
        }

        public String getVertexType() {
            return vertexType;
        }

        public void setVertexType(String vertexType) {
            this.vertexType = vertexType;
        }
    }
}
