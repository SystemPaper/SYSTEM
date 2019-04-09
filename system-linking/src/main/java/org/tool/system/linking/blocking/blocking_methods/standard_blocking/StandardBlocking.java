package org.tool.system.linking.blocking.blocking_methods.standard_blocking;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import org.tool.system.linking.blocking.blocking_methods.data_structures.StandardBlockingComponent;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.AssignBlockIndex;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.AssignVertexIndex;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.ComputeBlockSize;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.ComputePartitionEnumerationStartPoint;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.ComputePrevBlocksPairNo_AllPairs;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.ConcatAllInfoToVertex;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.ConcatVertextoPartitionInfo;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.CreatePairedVertices;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.DiscoverPartitionId;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.EnumeratePartitionEntities;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.PartitionVertices;
import org.tool.system.linking.blocking.blocking_methods.standard_blocking.functions.*;


import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;


public class StandardBlocking implements Serializable {
    private StandardBlockingComponent standardBlockingComponent;


    public StandardBlocking(StandardBlockingComponent StandardBlockingComponent) {
        standardBlockingComponent = StandardBlockingComponent;
    }

    public DataSet<Tuple2<Vertex, Vertex>> execute (DataSet<Vertex> vertices, HashMap<String, HashSet<String>> graphPairs, String dataset) throws Exception {

        DataSet<Tuple2<Vertex, String>> vertex_key = null ;
        if (dataset.equals("P"))
            vertex_key = vertices.map(new GetKeyP(3));
        else if (dataset.equals("G"))
            vertex_key = vertices.map(new GetKeyGeo(1));

        else if (dataset.equals("M"))
            vertex_key = vertices.flatMap(new GetKeyM(1));


        ////////
        vertex_key = vertex_key.flatMap(new RemoveEmptyKeys());
//        System.out.println(vertex_key.count());
        ///////



        DataSet<Tuple3<Vertex, String, Integer>> vertex_key_partitionId = vertex_key.map(new DiscoverPartitionId());

        DataSet<Tuple3<String, Integer, Long>> key_partitionId_no = vertex_key_partitionId.groupBy(2,1).combineGroup(new EnumeratePartitionEntities());
        UnsortedGrouping<Tuple3<String, Integer, Long>> key_partitionId_no_GroupedByKey = key_partitionId_no.groupBy(0);

        /* Generate vertex index */
        DataSet<Tuple3<String, Integer, Long>> key_partitionId_startPoint = key_partitionId_no_GroupedByKey.sortGroup(1, Order.ASCENDING).reduceGroup(new ComputePartitionEnumerationStartPoint());
        DataSet<Tuple4<Vertex, String, Integer, Long>> vertex_key_partitionId_startPoint =
                key_partitionId_startPoint.join(vertex_key_partitionId).where(0,1).equalTo(1,2).with(new ConcatVertextoPartitionInfo());
        DataSet<Tuple3<Vertex, String, Long>> vertex_key_vertexId = vertex_key_partitionId_startPoint.groupBy(1,2).reduceGroup(new AssignVertexIndex());

        /* Prepare key (block) information (size, index, no. of pairs in prev blocks, no. of all pairs) */
        DataSet <Tuple3<String, Long, Long>> key_size_index = DataSetUtils.zipWithUniqueId(key_partitionId_no_GroupedByKey.reduceGroup(new ComputeBlockSize())).map(new AssignBlockIndex());
        DataSet<Tuple5<String, Long, Long, Long, Long>> key_size_index_prevBlocksPairs_allPairs = key_size_index.reduceGroup(new ComputePrevBlocksPairNo_AllPairs());

        /* Provide information (BlockIndex, BlockSize, PrevBlockPairs, allPairs) for each vertex         */
        DataSet<Tuple6<Vertex, String, Long, Long, Long, Long>> vertex_key_VIndex_blockSize_prevBlocksPairs_allPairs =
                vertex_key_vertexId.join(key_size_index_prevBlocksPairs_allPairs).where(1).equalTo(0).with(new ConcatAllInfoToVertex());
        /* Load Balancing */
        Integer ReducerNo = standardBlockingComponent.getParallelismDegree();
        DataSet<Tuple5<Vertex, String, Long, Boolean, Integer>> vertex_key_VIndex_isLast_reducerId =
                vertex_key_VIndex_blockSize_prevBlocksPairs_allPairs.flatMap(new ReplicateAndAssignReducerId(ReducerNo));
        vertex_key_VIndex_isLast_reducerId = vertex_key_VIndex_isLast_reducerId.partitionCustom(new PartitionVertices(), 4);

        /* Make pairs */
        DataSet<Tuple2<Vertex, Vertex>> PairedVertices = vertex_key_VIndex_isLast_reducerId
        		.groupBy(1)
        		.sortGroup(2, Order.ASCENDING)
        		.combineGroup(new CreatePairedVertices(
        				standardBlockingComponent.getIntraGraphComparison(),
                        standardBlockingComponent.getIsNewNewCmpr(),
                        standardBlockingComponent.getIsCmprCnflct(),
        				graphPairs));

//        System.out.println(PairedVertices.count());
        return PairedVertices;
    }

    public class RemoveEmptyKeys implements org.apache.flink.api.common.functions.FlatMapFunction<Tuple2<Vertex, String>, Tuple2<Vertex, String>> {
        @Override
        public void flatMap(Tuple2<Vertex, String> in, Collector<Tuple2<Vertex, String>> collector) throws Exception {
            if (!in.f1.equals(""))
                collector.collect(in);
        }
    }

    public class GetKeyP implements MapFunction<Vertex, Tuple2<Vertex, String>> {
        private int keyLength;
        public GetKeyP (int length){keyLength = length;}
        @Override
        public Tuple2<Vertex, String> map(Vertex in) {
            String name="";
            String surname = "";
            if (in.hasProperty("name"))
                name = in.getPropertyValue("name").toString().toLowerCase();
            if (in.hasProperty("surname"))
                surname = in.getPropertyValue("surname").toString().toLowerCase();

            String key = name.substring(0, Math.min(keyLength, name.length()))+surname.substring(0, Math.min(keyLength, surname.length()));
            return Tuple2.of(in, key);
        }
    }
    public class GetKeyM implements FlatMapFunction<Vertex, Tuple2<Vertex, String>> {
        private int keyLength;
        public GetKeyM (int length){keyLength = length;}
        @Override
        public  void flatMap(Vertex in, Collector<Tuple2<Vertex, String>> out) {
//            List<String> fieldList = new ArrayList();
//            if (in.hasProperty("field"))
//                fieldList.add(in.getPropertyValue("field").toString());
//            else if (in.hasProperty("vertices_field")){
//                List<PropertyValue> vertices_field = in.getPropertyValue("vertices_field").getList();
//                for(PropertyValue f:vertices_field)
//                    fieldList.add(f.toString());
//            }
//            for (String field: fieldList) {
//
//                field = field.toLowerCase().replaceAll("\\s+", " ").trim();
//                String key = field.substring(0, Math.min(keyLength, field.length()));
//                out.collect(Tuple2.of(in, key));
//            }
            String field = "";
            if (in.hasProperty("field"))
                field = in.getPropertyValue("field").toString();
            else if (in.hasProperty("vertices_field"))
                field = in.getPropertyValue("vertices_field").getList().get(0).toString();

            field = field.toLowerCase().replaceAll("\\s+", " ").trim();
            String key = field.substring(0, Math.min(keyLength, field.length()));
            out.collect(Tuple2.of(in, key));

        }
    }
    public class GetKeyGeo implements MapFunction<Vertex, Tuple2<Vertex, String>> {
        private int keyLength;
        public GetKeyGeo (int length){keyLength = length;}
        @Override
        public Tuple2<Vertex, String> map(Vertex in) {
            String label = "";
            if (in.hasProperty("label"))
                label = in.getPropertyValue("label").toString();
            else if (in.hasProperty("vertices_label"))
                label = in.getPropertyValue("vertices_label").getList().get(0).toString();

            label = label.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().trim();
            label = label.toLowerCase().replaceAll("\\s+", " ").trim();
            String key = label.substring(0, Math.min(keyLength, label.length()));
            return Tuple2.of(in, key);
        }
    }
}





























