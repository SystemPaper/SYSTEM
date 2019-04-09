package org.tool.system.incremental.repairer.methods.max_both;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class GetMergableOnes implements GroupReduceFunction<Tuple5<Vertex, Vertex, String, String, String>, Tuple4<Vertex, Vertex, String, String>> {
    private int grpByPos;
    public GetMergableOnes (int grpByPos){
        this.grpByPos = grpByPos;
    }
    @Override
    public void reduce(Iterable<Tuple5<Vertex, Vertex, String, String, String>> iterable, Collector<Tuple4<Vertex, Vertex, String, String>> collector) throws Exception {
        List<Tuple4<Vertex, Vertex, String, String>> selectedList = new ArrayList();
        for (Tuple5<Vertex, Vertex, String, String, String> it : iterable)
                selectedList.add(Tuple4.of(it.f0, it.f1, it.f2, it.f3));
        selectedList = findMergables(selectedList, grpByPos);
        for (Tuple4<Vertex, Vertex, String, String> selected: selectedList)
            collector.collect(selected);
    }

    private List<Tuple4<Vertex,Vertex,String,String>> findMergables(List<Tuple4<Vertex, Vertex, String, String>> selectedList, int grpByPos) {
        List<Tuple4<Vertex, Vertex, String, String>> output = new ArrayList();
        selectedList.sort(new srcListComparator(grpByPos));
        for (Tuple4<Vertex, Vertex, String, String> selected: selectedList){
            if (isMergable(output, selected, grpByPos))
                output.add(selected);
        }
        return output;
    }
    private class srcListComparator implements Comparator <Tuple4<Vertex, Vertex, String, String>> {
        private int grpByPos;
        public srcListComparator (int grpByPos){
            this.grpByPos = grpByPos;
        }
        @Override
        public int compare(Tuple4<Vertex, Vertex, String, String> o1, Tuple4<Vertex, Vertex, String, String> o2) {
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
    private boolean isMergable (List<Tuple4<Vertex, Vertex, String, String>> list, Tuple4<Vertex, Vertex, String, String> item, int grpByPos){
        String itemSrces;
        if (grpByPos == 0)
            itemSrces = item.f3;
        else
            itemSrces = item.f2;
        for (Tuple4<Vertex, Vertex, String, String> l : list) {
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





















