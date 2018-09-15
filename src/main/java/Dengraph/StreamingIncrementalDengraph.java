package Dengraph;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;

public class StreamingIncrementalDengraph
{
    public static class IncrementalDengraph
    {
        private DengraphState state;
        private double neighborhoodRadius;
        private int minNumberOfNodesInNeighborhood;

        public IncrementalDengraph(DengraphState state, double neighborhoodRadius, int minNumberOfNodesInNeighborhood) {
            this.state = state;
            this.neighborhoodRadius = neighborhoodRadius;
            this.minNumberOfNodesInNeighborhood = minNumberOfNodesInNeighborhood;
        }

        public void addEdge(DenGraphEdge changedEdge)
        {
            // Get last state for this edge
            DenGraphEdge oldEdge = this.state.getEdge(changedEdge.getSrc(), changedEdge.getDst());

            // Update current graph with edge
            this.state.addOrReplaceEdge(changedEdge);

            // State is not carried over between minutes, so only positive updates will occur

            // Positive change
            if ((oldEdge == null || oldEdge.distance > this.neighborhoodRadius)
                    && changedEdge.distance <= this.neighborhoodRadius)
            {
                updateClustering(changedEdge.getSrc());
                updateClustering(changedEdge.getDst());
            }
        }

        private void updateClustering(String vertexId)
        {
            VertexState vertex = this.state.getVertexStates().get(vertexId);
            List<VertexState> neighborhood = computeNeighborhood(vertex);

            if (neighborhood.size() >= this.minNumberOfNodesInNeighborhood)
            {
                // This is a core vertex

                Set<Integer> setOfDistinctClusterIds = new HashSet<>();
                for (VertexState neighbor : neighborhood)
                {
                    if (neighbor.vertexType == VertexState.VertexType.CORE)
                        setOfDistinctClusterIds.add(neighbor.clusterId);
                }

                if (vertex.vertexType == VertexState.VertexType.CORE)
                    setOfDistinctClusterIds.add(vertex.clusterId);

                if (setOfDistinctClusterIds.size() == 0)
                {
                    // Creation of a new cluster

                    int newClusterId = getMaxClusterId() + 1;
                    vertex.setClusterId(newClusterId);
                    for (VertexState neighbor : neighborhood)
                    {
                        neighbor.setVertexType(VertexState.VertexType.BORDER);
                        neighbor.setClusterId(newClusterId);
                    }
                }

                if ((setOfDistinctClusterIds.size() == 1) && (vertex.getVertexType() != VertexState.VertexType.CORE))
                {
                    // Absorption of a node to a cluster

                    int clusterId = setOfDistinctClusterIds.iterator().next();  // NOTE: Only one in the set
                    vertex.setClusterId(clusterId);
                    for (VertexState neighbor : neighborhood)
                    {
                        if (neighbor.getVertexType() != VertexState.VertexType.CORE)
                        {
                            neighbor.setVertexType(VertexState.VertexType.BORDER);
                            neighbor.setClusterId(clusterId);
                        }
                    }
                }

                if (setOfDistinctClusterIds.size() > 1)
                {
                    // Merging of clusters

                    int newClusterId = getMaxClusterId() + 1;
                    vertex.setClusterId(newClusterId);

                    for (VertexState v : this.state.getVertexStates().values())
                    {
                        if (setOfDistinctClusterIds.contains(v.getClusterId()))
                        {
                            v.setClusterId(newClusterId);
                        }
                    }

                    for (VertexState neighbor : neighborhood)
                    {
                        if (neighbor.getVertexType() != VertexState.VertexType.CORE)
                        {
                            neighbor.setVertexType(VertexState.VertexType.BORDER);
                            neighbor.setClusterId(newClusterId);
                        }
                    }
                }

                vertex.setVertexType(VertexState.VertexType.CORE);
            }
        }

        private int getMaxClusterId()
        {
            int maxClusterId = -1;

            for (String vertexStateKey : this.state.getVertexStates().keySet())
            {
                VertexState vertexState = this.state.getVertexStates().get(vertexStateKey);

                if (vertexState.clusterId > maxClusterId)
                {
                    maxClusterId = vertexState.clusterId;
                }
            }

            return maxClusterId;
        }

        private List<VertexState> computeNeighborhood(VertexState vertex)
        {
            // The neighborhood of p is the set of vertices that are more proximal to p than epsilon
            List<VertexState> neighborhood = new ArrayList<>();

            for (DenGraphEdge edge : this.state.getEdgeMap().values())
            {
                if (vertex.id.equals(edge.getSrc()) && (edge.getDistance() <= this.neighborhoodRadius))
                    neighborhood.add(this.state.getVertexStates().get(edge.getDst()));
                else if (vertex.id.equals(edge.getDst()) && (edge.getDistance() <= this.neighborhoodRadius))
                    neighborhood.add(this.state.getVertexStates().get(edge.getSrc()));
            }

            return neighborhood;
        }
    }

    /**
     * Undirected edge with distance measure
     * */
    public static class DenGraphEdge extends Dengraph.Edge
    {
        private double distance;

        public DenGraphEdge() {
        }

        public DenGraphEdge(Timestamp eventTime, String src, String dst, double weight)
        {
            super(eventTime, src, dst, weight);

            double proximity;

            if (src.equals(dst))
                proximity = 1;
            else
                proximity = 1 - (1/this.getWeight());

            this.distance = 1 - proximity;
        }

        public double getDistance() {
            return distance;
        }

        public void setDistance(double distance) {
            this.distance = distance;
        }
    }

    public static class DengraphState implements Serializable
    {
        /**
         * Undirected edges. Need reciprocity.
         * */
        private Map<SrcDstPair, DenGraphEdge> edgeMap;
        private Map<String, VertexState> vertexStates;

        public DengraphState()
        {
            this.edgeMap = new HashMap<>();
            this.vertexStates = new HashMap<>();
        }

        public DengraphState(Map<SrcDstPair, DenGraphEdge> edgeMap, Map<String, VertexState> vertexStates)
        {
            this.edgeMap = edgeMap;
            this.vertexStates = vertexStates;
        }

        public void addOrReplaceEdge(DenGraphEdge edge)
        {
            // Add (or replace) edge to graph
            this.edgeMap.put(new SrcDstPair(edge.getSrc(), edge.getDst()), edge);

            // Create states for the nodes in the edge if necessary
            if (!this.vertexStates.containsKey(edge.getSrc()))
                this.vertexStates.put(edge.getSrc(), new VertexState(edge.getSrc()));
            if (!this.vertexStates.containsKey(edge.getDst()))
                this.vertexStates.put(edge.getDst(), new VertexState(edge.getDst()));
        }

        public DenGraphEdge getEdge(String src, String dst)
        {
            return this.edgeMap.get(new SrcDstPair(src, dst));
        }

        public Map<SrcDstPair, DenGraphEdge> getEdgeMap() {
            return edgeMap;
        }

        public void setEdgeMap(Map<SrcDstPair, DenGraphEdge> edgeMap) {
            this.edgeMap = edgeMap;
        }

        public Map<String, VertexState> getVertexStates() {
            return vertexStates;
        }

        public void setVertexStates(Map<String, VertexState> vertexStates) {
            this.vertexStates = vertexStates;
        }
    }

    public static class DengraphStateUpdate implements Serializable
    {
        private Timestamp eventTime;
        private boolean isFinal;
        private Map<String, VertexState> vertexStates;

        public DengraphStateUpdate() {
        }

        public DengraphStateUpdate(Timestamp eventTime, boolean isFinal, Map<String, VertexState> vertexStates) {
            this.eventTime = eventTime;
            this.isFinal = isFinal;
            this.vertexStates = vertexStates;
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

        public Map<String, VertexState> getVertexStates() {
            return vertexStates;
        }

        public void setVertexStates(Map<String, VertexState> vertexStates) {
            this.vertexStates = vertexStates;
        }
    }

    public static class VertexState implements Serializable
    {
        public enum VertexType
        {
            CORE,
            BORDER,
            NOISE
        }

        private String id;
        private int clusterId;
        private VertexType vertexType;

        public VertexState() {
        }

        public VertexState(String id)
        {
            this.id = id;
            this.clusterId = -1;
            this.vertexType = VertexType.NOISE;
        }

        public VertexState(String id, int clusterId, VertexType vertexType)
        {
            this.id = id;
            this.clusterId = clusterId;
            this.vertexType = vertexType;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public int getClusterId() {
            return clusterId;
        }

        public void setClusterId(int clusterId) {
            this.clusterId = clusterId;
        }

        public VertexType getVertexType() {
            return vertexType;
        }

        public void setVertexType(VertexType vertexType) {
            this.vertexType = vertexType;
        }

        @Override
        public String toString() {
            return "VertexState{" +
                    "id='" + id + '\'' +
                    ", clusterId=" + clusterId +
                    ", vertexType=" + vertexType +
                    '}';
        }
    }

    public static class SrcDstPair implements Serializable
    {
        private String src;
        private String dst;

        public SrcDstPair() {
        }

        public SrcDstPair(String src, String dst) {
            this.src = src;
            this.dst = dst;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SrcDstPair that = (SrcDstPair) o;
            return Objects.equals(src, that.src) &&
                    Objects.equals(dst, that.dst);
        }

        @Override
        public int hashCode() {
            return Objects.hash(src, dst);
        }
    }
}
