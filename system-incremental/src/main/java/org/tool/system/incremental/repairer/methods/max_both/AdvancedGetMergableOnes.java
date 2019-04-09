package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class AdvancedGetMergableOnes implements GroupReduceFunction<Tuple6<Vertex, Vertex, String, String, String, Double>,
        Tuple5<Vertex, Vertex, String, String, Double>> {
    private int grpByPos;
    public AdvancedGetMergableOnes(int grpByPos){
        this.grpByPos = grpByPos;
    }
    @Override
    public void reduce(Iterable<Tuple6<Vertex, Vertex, String, String, String, Double>> iterable,
                       Collector<Tuple5<Vertex, Vertex, String, String, Double>> collector) throws Exception {
        List<Tuple5<Vertex, Vertex, String, String, Double>> selectedList = new ArrayList();
        for (Tuple6<Vertex, Vertex, String, String, String, Double> it : iterable)
                selectedList.add(Tuple5.of(it.f0, it.f1, it.f2, it.f3, it.f5));
        selectedList = findMergables(selectedList, grpByPos);
        for (Tuple5<Vertex, Vertex, String, String, Double> selected: selectedList)
            collector.collect(selected);
    }

    private List<Tuple5<Vertex,Vertex,String,String, Double>> findMergables(List<Tuple5<Vertex, Vertex, String, String, Double>> selectedList, int grpByPos) {
        List<Tuple5<Vertex, Vertex, String, String, Double>> output = new ArrayList();
        selectedList.sort(new srcListComparator(grpByPos));
        for (Tuple5<Vertex, Vertex, String, String, Double> selected: selectedList){
            if (isMergable(output, selected, grpByPos))
                output.add(selected);
        }
        return output;
    }
    private class srcListComparator implements Comparator <Tuple5<Vertex, Vertex, String, String, Double>> {
        private int grpByPos;
        public srcListComparator (int grpByPos){
            this.grpByPos = grpByPos;
        }
        @Override
        public int compare(Tuple5<Vertex, Vertex, String, String, Double> o1, Tuple5<Vertex, Vertex, String, String, Double> o2) {
//            int srcNo1;
//            int srcNo2;
//            if (grpByPos == 0) {
//                srcNo1 = o1.f3.split(",").length;
//                srcNo2 = o2.f3.split(",").length;
//            }
//            else {
//                srcNo1 = o1.f2.split(",").length;
//                srcNo2 = o2.f2.split(",").length;
//            }
//            if (srcNo1 > srcNo2)
//                return -1;
//            else if (srcNo2 > srcNo1)
//                return 1;
//            return 0;
            int compRes = o1.f4.compareTo(o2.f4);
            if(compRes != 0)
                return compRes;
            else {
                int srcNo1;
                int srcNo2;
                if (grpByPos == 0) {
                    srcNo1 = o1.f3.split(",").length;
                    srcNo2 = o2.f3.split(",").length;
                }
                else {
                    srcNo1 = o1.f2.split(",").length;
                    srcNo2 = o2.f2.split(",").length;
                }
                if (srcNo1 > srcNo2)
                    return -1;
                else if (srcNo2 > srcNo1)
                    return 1;
                return 0;
            }
        }
    }
    private boolean isMergable (List<Tuple5<Vertex, Vertex, String, String, Double>> list,
                                Tuple5<Vertex, Vertex, String, String, Double> item, int grpByPos){
        String itemSrces;
        if (grpByPos == 0)
            itemSrces = item.f3;
        else
            itemSrces = item.f2;
        for (Tuple5<Vertex, Vertex, String, String, Double> l : list) {
            String lSrces;
            if (grpByPos == 0)
                lSrces = l.f3;
            else
                lSrces = l.f2;
            if (!isConsistent(lSrces, itemSrces))
                return false;
        }
        return true;
    }
    private boolean isConsistent(String srces1, String srces2){
        String[] srcList1 = srces1.split(",");
        List<String> srcList2 = Arrays.asList(srces2.split(","));
        for(String s: srcList1){
            if (srcList2.contains(s))
                return false;
        }
        return true;
    }
}





















