package CongestedComponents;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.*;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;

/**
 * Streaming connected components implementation
 * */
public class StreamingConnectedComponents
{
    public static Dataset<CCStateUpdate> run(Dataset<Row> rawEdges) throws Exception
    {
        // Convert raw edges to Edge class used by CC
        Dataset<Edge> edges = rawEdges.as(Encoders.bean(Edge.class));

        MapGroupsWithStateFunction<Timestamp, Edge, CCState, CCStateUpdate> updateCCState =
                new MapGroupsWithStateFunction<Timestamp, Edge, CCState, CCStateUpdate>() {
                    @Override
                    public CCStateUpdate call(Timestamp timestamp, Iterator<Edge> iterator, GroupState<CCState> groupState) throws Exception
                    {
                        CCState currentState;

                        if (groupState.exists())
                        {
                            currentState = groupState.get();
                        }
                        else
                        {
                            currentState = new CCState();
                        }

                        if (groupState.hasTimedOut())
                        {
                            // State has timed out, send final update and remove state
                            CCStateUpdate finalUpdate = new CCStateUpdate(timestamp, true, groupState.get().getVertexComponentMap());
                            groupState.remove();
                            return finalUpdate;
                        }

                        while (iterator.hasNext())
                        {
                            Edge currentEdge = iterator.next();
                            Map<Integer, Integer> vertexComponentMap = currentState.getVertexComponentMap();
                            Map<Integer, ArrayList<Integer>> componentVerticesMap = currentState.getComponentVerticesMap();

                            // If seen for the first time, create a component with the ID of the min of the vertex IDs
                            if (!vertexComponentMap.containsKey(currentEdge.getSrc())
                                    && !vertexComponentMap.containsKey(currentEdge.getDst()))
                            {
                                // Neither src nor dst already in component, add a new component for them
                                int newComponentId = Math.min(currentEdge.getSrc(), currentEdge.getDst());

                                ArrayList<Integer> verticesInComponent = new ArrayList<>();
                                verticesInComponent.add(currentEdge.getSrc());
                                verticesInComponent.add(currentEdge.getDst());
                                componentVerticesMap.put(newComponentId, verticesInComponent);

                                vertexComponentMap.put(currentEdge.getSrc(), newComponentId);
                                vertexComponentMap.put(currentEdge.getDst(), newComponentId);
                            }
                            else if (vertexComponentMap.containsKey(currentEdge.getSrc())
                                    && vertexComponentMap.containsKey(currentEdge.getDst()))
                            {
                                // Both vertices already in components
                                // If in different components, merge them
                                // If in the same component, do nothing

                                int srcComponent = vertexComponentMap.get(currentEdge.getSrc());
                                int dstComponent = vertexComponentMap.get(currentEdge.getDst());

                                if (srcComponent != dstComponent)
                                {
                                    // Vertices are in different components
                                    // Merge them and update the component ID to the min of the component IDs

                                    int targetComponent = Math.min(srcComponent, dstComponent);
                                    int mergeComponent = Math.max(srcComponent, dstComponent);
                                    ArrayList<Integer> targetVertices = componentVerticesMap.get(targetComponent);
                                    ArrayList<Integer> verticesToMerge = componentVerticesMap.get(mergeComponent);

                                    targetVertices.addAll(verticesToMerge);
                                    componentVerticesMap.put(targetComponent, targetVertices);

                                    for (Integer vertex : verticesToMerge)
                                    {
                                        vertexComponentMap.put(vertex, targetComponent);
                                    }
                                }
                            }
                            else
                            {
                                // Only one of the vertices belongs to a component
                                // Add the other one to the same component
                                if (vertexComponentMap.containsKey(currentEdge.getSrc()))
                                {
                                    // Src edge in component, add dst to it
                                    int targetComponent = vertexComponentMap.get(currentEdge.getSrc());

                                    ArrayList<Integer> targetComponentVertices = componentVerticesMap.get(targetComponent);
                                    targetComponentVertices.add(currentEdge.getDst());
                                    componentVerticesMap.put(targetComponent, targetComponentVertices);

                                    vertexComponentMap.put(currentEdge.getDst(), targetComponent);
                                }
                                else
                                {
                                    // Dst edge in component, add src to it

                                    int targetComponent = vertexComponentMap.get(currentEdge.getDst());

                                    ArrayList<Integer> targetComponentVertices = componentVerticesMap.get(targetComponent);
                                    targetComponentVertices.add(currentEdge.getSrc());
                                    componentVerticesMap.put(targetComponent, targetComponentVertices);

                                    vertexComponentMap.put(currentEdge.getSrc(), targetComponent);
                                }
                            }

                            currentState.setComponentVerticesMap(componentVerticesMap);
                            currentState.setVertexComponentMap(vertexComponentMap);
                        }

                        CCState newState = new CCState();
                        newState.setVertexComponentMap(currentState.getVertexComponentMap());
                        newState.setComponentVerticesMap(currentState.getComponentVerticesMap());

                        groupState.update(newState);

                        // Processing time timeout
                        // Set timeout such that the session will be expired if no data received for 20 seconds
                        //groupState.setTimeoutDuration("20 seconds");

                        // Event time timeout with watermarks
                        // State is timed out when the watermark advances beyond this timeout timestamp
                        // Set as 30 seconds so that when the watermark reaches the next minute, this minute is timed out
                        groupState.setTimeoutTimestamp(timestamp.getTime(), "30 seconds");

                        return new CCStateUpdate(timestamp, false, groupState.get().getVertexComponentMap());
                    }
                };

        Dataset<CCStateUpdate> ccUpdates = edges
                // How late data is allowed to be, else dropped.
                // 0 seconds -> watermark follows the max event time.
                .withWatermark("eventTime", "0 seconds")
                .groupByKey(
                        new MapFunction<Edge, Timestamp>()
                        {
                            @Override public Timestamp call(Edge edge)
                            {
                                return edge.getEventTime();
                            }
                        }, Encoders.TIMESTAMP())
                .mapGroupsWithState(
                        updateCCState,
                        Encoders.bean(CCState.class),
                        Encoders.bean(CCStateUpdate.class),
                        GroupStateTimeout.EventTimeTimeout()
                );

        return ccUpdates;
    }

    // Classes

    public static class Edge implements Serializable {
        private Timestamp eventTime;
        private int src;
        private int dst;

        public Edge() {
        }

        public Edge(Timestamp eventTime, int src, int dst) {
            this.eventTime = eventTime;
            this.src = src;
            this.dst = dst;
        }

        public Timestamp getEventTime() {
            return eventTime;
        }

        public void setEventTime(Timestamp eventTime) {
            this.eventTime = eventTime;
        }

        public int getSrc() {
            return src;
        }

        public void setSrc(int src) {
            this.src = src;
        }

        public int getDst() {
            return dst;
        }

        public void setDst(int dst) {
            this.dst = dst;
        }
    }

    public static class CCState implements Serializable
    {
        private Map<Integer, ArrayList<Integer>> componentVerticesMap;
        private Map<Integer, Integer> vertexComponentMap;

        public CCState()
        {
            this.componentVerticesMap = new HashMap<>();
            this.vertexComponentMap = new HashMap<>();
        }

        public Map<Integer, ArrayList<Integer>> getComponentVerticesMap() {
            return componentVerticesMap;
        }

        public void setComponentVerticesMap(Map<Integer, ArrayList<Integer>> componentVerticesMap) {
            this.componentVerticesMap = componentVerticesMap;
        }

        public Map<Integer, Integer> getVertexComponentMap() {
            return vertexComponentMap;
        }

        public void setVertexComponentMap(Map<Integer, Integer> vertexComponentMap) {
            this.vertexComponentMap = vertexComponentMap;
        }
    }

    public static class CCStateUpdate implements Serializable
    {
        private Timestamp eventTime;
        private boolean isFinal;
        private Map<Integer, Integer> vertexComponentMap;

        public CCStateUpdate() {
        }

        public CCStateUpdate(Timestamp eventTime, boolean isFinal, Map<Integer, Integer> vertexComponentMap) {
            this.eventTime = eventTime;
            this.isFinal = isFinal;
            this.vertexComponentMap = vertexComponentMap;
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

        public Map<Integer, Integer> getVertexComponentMap() {
            return vertexComponentMap;
        }

        public void setVertexComponentMap(Map<Integer, Integer> vertexComponentMap) {
            this.vertexComponentMap = vertexComponentMap;
        }
    }
}
