package matching;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import data.Graph;
import data.InvalidFieldException;
import data.InvalidTupleException;
import data.KeyCollisionException;
import data.Schema;
import data.Table;
import data.Tuple;

public class GraphMatcher implements Matcher<IdGraph> {

    public static String ASSIGNED = "assigned";
    public static String CROSSED = "crossed";
    public static String MARKED = "marked";
    public static String MI = "mi";
    public static String MJ = "mj";
    public static String IJ = "ij";
    private ArrayList<String> nodeSigFields;
    private ArrayList<String> edgeSigFields;
    private HashMap<String, Number> range;
    private LinkedHashMap<Integer, int[]> m = new LinkedHashMap<Integer, int[]>();
    private SimilarityMetric stringMetric;

    public GraphMatcher() {
        nodeSigFields = new ArrayList<>();
        edgeSigFields = new ArrayList<>();

        range = new HashMap<>();
        //sigSchema = signatures.getSchema();
        //sigSchema.addField(", type)
    }

    public void setStringMetric(SimilarityMetric m) {
        this.stringMetric = m;
    }

    public void addSignatureNodeField(String... names) {
        for (String s : names) {
            nodeSigFields.add(s);
        }
    }

    public void addSignatureEdgeField(String... names) {
        for (String s : names) {
            edgeSigFields.add(s);
        }
    }

    public void setFieldRange(String field, Number r) {
        range.put(field, r);
    }

    public void hungarian(Table in) throws InvalidFieldException,
            InvalidTupleException,
            KeyCollisionException {
        step1(in);
        step2(in);
        step4(in);

    }

    //public void step1(double[][] in){
    private void step1(Table in) throws InvalidFieldException, InvalidTupleException, KeyCollisionException {
        Table filtered;
        Double min;
        Iterator<Tuple> it;
        Tuple t;
        Set<Object> is = in.getColumn(MI).getUniqueValues();
        for (Object i : is) {//to know which rows number there are
            filtered = in.filterEquals(MI, i);
            min = (Double) filtered.min("v");
            it = filtered.iterator();
            while (it.hasNext()) {
                t = it.next();
                t.set("v", (Double) t.get("v") - min);
            }
        }

    }

    private void step2(Table in) throws InvalidFieldException, InvalidTupleException, KeyCollisionException {
        Table filtered;
        Double min;
        Iterator<Tuple> it;
        Tuple t;
        Set<Object> is = in.getColumn(MJ).getUniqueValues();
        for (Object j : is) {//to know which rows number there are
            filtered = in.filterEquals(MJ, j);
            min = (Double) filtered.min("v");
            it = filtered.iterator();
            while (it.hasNext()) {
                t = it.next();
                t.set("v", (Double) t.get("v") - min);
            }
        }
    }

    private int step3(Table in) throws InvalidFieldException, InvalidTupleException, KeyCollisionException {
        in.reset(CROSSED);
        in.reset(ASSIGNED);
        boolean go = true;
        int nlines = 0;
        Set<Integer> assignedRows = new HashSet<Integer>();

        Table filteredV0 = in.filterEquals("v", 0d);
        Table filteredIJ;

        //1. For each row or column with a single zero value cell that has not be ASSIGNED or eliminated, box that zero value as an ASSIGNED cell.
        //2. For every zero that becomes ASSIGNED, cross out (X) all other zeros in the same row and the same column.
        Set<Integer> lines = in.<Integer>getUniqueValues(MI);
        Set<Integer> columns = in.<Integer>getUniqueValues(MJ);

        Integer i, j;
        Tuple t;

        int stopSizeBefor;
        do {
            stopSizeBefor = filteredV0.filterEquals(ASSIGNED, 0).size();

            for (Object oi : lines) {
                i = (Integer) oi;
                filteredIJ = filteredV0.filterEquals(MI, i).filterEquals(ASSIGNED, 0);
                if (filteredIJ.size() == 1) {
                    t = filteredIJ.iterator().next();
                    t.set(ASSIGNED, 1);
                    assignedRows.add(i);

                    nlines++;
                    j = (Integer) t.get(MJ);

                    filteredIJ = filteredV0.filterEquals(MJ, j).filterEquals(ASSIGNED, 0).filterDifferent(MI, i);
                    Iterator<Tuple> it = filteredIJ.iterator();
                    while (it.hasNext()) {
                        t = it.next();
                        t.set(ASSIGNED, -1);

                    }
                }

            }

            for (Object oj : columns) {
                j = (Integer) oj;
                filteredIJ = filteredV0.filterEquals(MJ, j).filterEquals(ASSIGNED, 0);
                if (filteredIJ.size() == 1) {
                    t = filteredIJ.iterator().next();
                    t.set(ASSIGNED, 1);
                    nlines++;
                    i = (Integer) t.get(MI);
                    assignedRows.add(i);

                    filteredIJ = filteredV0.filterEquals(MI, i).filterDifferent(MJ, j);
                    Iterator<Tuple> it = filteredIJ.iterator();
                    while (it.hasNext()) {
                        t = it.next();
                        t.set(ASSIGNED, -1);
                    }
                }

            }

            filteredV0 = in.filterEquals("v", 0d).filterEquals(ASSIGNED, 0);
            go = filteredV0.size() != 0;
//			printAs3dArray(filteredV0, "0.000");

        } while (filteredV0.size() != stopSizeBefor);
        //3. If for a row and a column, there are two or more zeros and one cannot be chosen by inspection, choose the cell arbitrarily for assignment.
        //4. The above process may be continued until every zero cell is either ASSIGNED or CROSSED (X).

        if (nlines == in.getUniqueValues(MI).size()) {
            return nlines;
        } else {

            nlines = 0;
            Set<Integer> markedRows = new HashSet<Integer>();
            Set<Integer> markedColumns = new HashSet<Integer>();
            int numMarkedRows = 0;
            int numMarkedColumns = 0;
            boolean go2 = true;
            Collection<Integer> unassignedRows = new HashSet<Integer>();

            // 1. Mark all the rows that do not have assignments.
            unassignedRows.addAll(lines);
            unassignedRows.removeAll(assignedRows);
            lines.removeAll(assignedRows);

            markedRows.addAll(unassignedRows);
            numMarkedRows = markedRows.size();

            do {

                //2.Mark all the columns (not already MARKED) which have zeros in the MARKED rows.
                markedColumns.addAll(in.filterEquals(MI, markedRows.toArray()).filterEquals("v", 0d).<Integer>getUniqueValues(MJ));
                //3.Mark all the rows (not already MARKED) that have assignments in MARKED columns.

                Set temp = in.filterEquals(MJ, markedColumns.toArray()).filterEquals(ASSIGNED, 1).<Integer>getUniqueValues(MI);
                for (Object oi : temp) {
                    markedRows.add((Integer) oi);

                }

                go2 = !(markedRows.size() == numMarkedRows) && (markedColumns.size() == numMarkedColumns);

                numMarkedRows = markedRows.size();
                numMarkedColumns = markedColumns.size();

            } while (go2);
            //4. Repeat steps 5 (ii) and (iii) until no more rows or columns can be MARKED.

            //5. Draw straight lines through all unmarked rows and MARKED columns.
            Collection ctemp = in.getUniqueValues(MI);
            ctemp.removeAll(markedRows);

            for (Object crossedRows : ctemp) {
                this.crossLine(in, (Integer) crossedRows);
                nlines++;
            }

            //ctemp = in.getUniqueValues(MJ);
            //ctemp.removeAll(markedColumns);
            for (Integer crossedColumn : markedColumns) {
                this.crossColumn(in, crossedColumn);
                nlines++;
            }
        }//fim else

        return nlines;

    }

    private void step4(Table in) throws InvalidFieldException,
            InvalidTupleException,
            KeyCollisionException {
        int lines = 0;
        lines = step3(in);

        while (lines != in.getColumn(MI).getUniqueValues().size()
                && lines != in.getColumn(MJ).getUniqueValues().size()) {
            step5(in);
            lines = step3(in);
        }
    }

    private void step5(Table in) throws InvalidFieldException, InvalidTupleException, KeyCollisionException {
        Table filtered = in.filterEquals(CROSSED, 0);
        Double min = (Double) filtered.min("v");

        if (min != null) {
            Tuple t;
            Iterator<Tuple> it = filtered.iterator();
            while (it.hasNext()) {
                t = it.next();
                t.set("v", (Double) t.get("v") - min);
            }

            filtered = in.filterLargerOrEquals(CROSSED, 2);
            it = filtered.iterator();
            while (it.hasNext()) {
                t = it.next();
                t.set("v", (Double) t.get("v") + min);
            }
        }
    }

    private boolean selectUniquePairs(Table in, String rowOrColumn, boolean unique, List<Tuple> pairs) throws InvalidFieldException, InvalidTupleException, KeyCollisionException {
        Table filtered = in.filterEquals("v", 0d).filterEquals(CROSSED, 0);
        boolean changed = false;

        Set<Integer> rows = filtered.getUniqueValues(rowOrColumn);

        Table rowsTable;
        Iterator<Tuple> rtit;
        for (Integer r : rows) {
            rowsTable = filtered.filterEquals(rowOrColumn, r);
            if (unique && rowsTable.size() == 1) {
                rtit = rowsTable.iterator();
                Tuple t = rtit.next();
                this.crossColumn(in, (Integer) t.get(MJ));
                this.crossLine(in, (Integer) t.get(MI));

                pairs.add(t);
                changed = true;
            }

            if (!unique) {
                rtit = rowsTable.iterator();
                Tuple t = rtit.next();
                if (t != null) {
                    this.crossColumn(in, (Integer) t.get(MJ));
                    this.crossLine(in, (Integer) t.get(MI));

                    pairs.add(t);
                    changed = true;
                    break;
                }
            }

        }
        return changed;
    }

    private List<Tuple> selectPairs(Table in) throws InvalidFieldException, InvalidTupleException, KeyCollisionException {

        List<Tuple> pairs = new ArrayList<Tuple>();
        //olha coluna por coluna
        //olha se tem coluna so com um 0
        //se tem só 1 zero, marca o zero
        boolean go = false;

        do {
            go = this.selectUniquePairs(in, MI, true, pairs);
            go = go || this.selectUniquePairs(in, MJ, true, pairs);

            if (go) {
                if (!this.selectUniquePairs(in, MI, false, pairs)) {
                    this.selectUniquePairs(in, MJ, false, pairs);
                }
            }
        } while (go);

        return pairs;
    }

    private int crossLine(Table in, int line) throws InvalidTupleException, KeyCollisionException {
        int answer = 0;
        Table lines = in.filterEquals(MI, line);
        Iterator<Tuple> it = lines.iterator();
        Tuple t;
        while (it.hasNext()) {
            t = it.next();
            t.set(CROSSED, (Integer) t.get(CROSSED) + 1);
            if ((Double) t.get("v") == 0d) {
                answer++;
            }
        }
        return answer;

    }

    private int crossColumn(Table in, int column) throws InvalidTupleException, KeyCollisionException {
        int answer = 0;
        Table columns = in.filterEquals(MJ, column);
        Iterator<Tuple> it = columns.iterator();
        Tuple t;
        while (it.hasNext()) {
            t = it.next();
            t.set(CROSSED, (Integer) t.get(CROSSED) + 1);
            if ((Double) t.get("v") == 0d) {
                answer++;
            }
        }

        return answer;

    }

    private Table heom(Graph g1, Graph g2, boolean out, boolean in) throws InvalidTupleException, InvalidFieldException, KeyCollisionException {
        Table answer = new Table();
        answer.getSchema().addField(MI, Integer.class);
        answer.getSchema().addField(MJ, Integer.class);
        answer.getSchema().addField("v", Double.class);//value

        Iterator<Tuple> it1 = g1.getNodesTable().iterator();
        Iterator<Tuple> it2 = g2.getNodesTable().iterator();
        Tuple n1;
        Tuple n2;

        int i = 0;
        int j = 0;
        while (it2.hasNext()) {
            n2 = it2.next();
            while (it1.hasNext()) {
                n1 = it1.next();
                answer.addRow(i, j, heom(n2, g2, n1, g1, out, in));
                i++;
            }
            j++;
        }
        return answer;
    }

    private double heom(Tuple tn1, Graph g1, Tuple tn2, Graph g2, boolean out,
            boolean in) throws InvalidTupleException {

        double sum = 0d;
        Double v1;
        Double v2;
        for (String f : nodeSigFields) {
            if (Number.class.isAssignableFrom(tn1.getType(f))
                    && Number.class.isAssignableFrom(tn2.getType(f))) {
                try {
                    v1 = Double.parseDouble(tn1.get(f).toString());
                } catch (NullPointerException e) {
                    v1 = 0 - Double.MAX_VALUE;
                }
                try {
                    v2 = Double.parseDouble(tn2.get(f).toString());
                } catch (NullPointerException e) {
                    v2 = 0 - Double.MAX_VALUE;
                }
                sum += Math.pow(sigmaNumber(v1, v2, 1d), 2);
            } else {
                sum += Math.pow(sigmaObject(tn1.get(f), tn2.get(f)), 2);
            }
        }

        ///////////////////////////////////////////////
        //COMECA EDGES
        //TODO fazer arestas saindo
        //////////////////////////////////
        //GRAU DAS EDGES
        int size = Math.max(g1.size(), g2.size());
        if (out) {
            sum += Math.pow(Math.abs(g1.getOutLinks(tn1).size()
                    - g2.getOutLinks(tn2).size()) / size, 2);

            Collection<Tuple> out1 = g1.getOutLinks(tn1);
            Collection<Tuple> out2 = g2.getOutLinks(tn2);

            //Object o;
            double sum1 = 0;
            double sum2 = 0;
            //pra cada field, |Sum(1)-Sum(2)/Max(Sum1, Sum2)|
            for (String f : edgeSigFields) {

                Class c1 = g1.getEdgesTable().getSchema().getFieldType(f);
                Class c2 = g2.getEdgesTable().getSchema().getFieldType(f);
                if (c1.equals(c2)) {
                    if (Number.class.isAssignableFrom(c1)) {
                        for (Tuple edge1 : out1) {
                            try {
                                v1 = Double.parseDouble(edge1.get(f).toString());
                            } catch (NullPointerException e) {
                                v1 = 0 - Double.MAX_VALUE;
                            }
                            if (edge1.get(f) != null) {
                                sum1 += v1;
                            }
                        }
                        for (Tuple edge2 : out2) {
                            try {
                                v2 = Double.parseDouble(edge2.get(f).toString());
                            } catch (NullPointerException e) {
                                v2 = 0 - Double.MAX_VALUE;
                            }
                            if (edge2.get(f) != null) {
                                sum2 += v2;
                            }
                        }
                    } else {
                        sum1 = g1.getEdgesTable().filterNotNull(f).size();
                        sum2 = g2.getEdgesTable().filterNotNull(f).size();
                    }

                } else {
                    sum1 = 0d;
                    sum2 = 0d;
                }

                sum1 = sum1 / out1.size();
                sum2 = sum2 / out2.size();

                //diff[i] = Math.abs( sum1-sum2/Math.max(sum1, sum2) );
                sum = Math.pow(Math.abs(sum1 - sum2 / Math.max(sum1, sum2)), 2);
                //i++;
            }
        }

        if (in) {
            sum += Math.pow(Math.abs(g1.getInLinks(tn1).size()
                    - g2.getInLinks(tn2).size()) / size, 2);

            Collection<Tuple> in1 = g1.getInLinks(tn1);
            Collection<Tuple> in2 = g2.getInLinks(tn2);

            //Object o;
            double sum1 = 0;
            double sum2 = 0;
            //pra cada field, |Sum(1)-Sum(2)/Max(Sum1, Sum2)|
            for (String f : edgeSigFields) {

                Class c1 = g1.getEdgesTable().getSchema().getFieldType(f);
                Class c2 = g2.getEdgesTable().getSchema().getFieldType(f);
                if (c1.equals(c2)) {
                    if (Number.class.isAssignableFrom(c1)) {
                        for (Tuple edge1 : in1) {
                            if (edge1.get(f) != null) {
                                sum1 += Double.parseDouble(edge1.get(f).toString());
                            }
                        }
                        for (Tuple edge2 : in2) {
                            if (edge2.get(f) != null) {
                                sum2 += Double.parseDouble(edge2.get(f).toString());
                            }
                        }
                    } else {
                        sum1 = g1.getEdgesTable().filterNotNull(f).size();
                        sum2 = g2.getEdgesTable().filterNotNull(f).size();
                    }

                } else {
                    sum1 = 0d;
                    sum2 = 0d;
                }

                sum1 = sum1 / in1.size();
                sum2 = sum2 / in2.size();

                //diff[i] = Math.abs( sum1-sum2/Math.max(sum1, sum2) );
                sum = Math.pow(Math.abs(sum1 - sum2 / Math.max(sum1, sum2)), 2);
                //i++;
            }
        }

        return Math.sqrt(sum);

    }

    private double sigmaNumber(Number v1, Number v2, Double range) {

        return Math.abs(Double.parseDouble(v1.toString())
                - Double.parseDouble(v2.toString())) / range;

    }

    private double sigmaObject(Object v1, Object v2) {
        double answer = 1d;

        if (v1.getClass().equals(v2.getClass())) {
            if (v2.equals(v1)) {
                answer = 0;
            }
        } else {
            if (v1.getClass().isAssignableFrom(v2.getClass())) {
                if ((v1.getClass().cast(v2)).equals(v1)) {
                    answer = 0;
                } else if ((v2.getClass().cast(v1)).equals(v2)) {
                    answer = 0;
                }
            }
        }
        return answer;
    }

    @Override
    public double distance(IdGraph idg1, IdGraph idg2) throws MatchException {

        try {
            Graph g1, g2;
            g1 = idg1.g;
            g2 = idg2.g;
            Table theom;

            theom = heom(g1, g2, true, true);

            Table hung = new Table(theom);
//            Table hung = Table.clone(theom);

            Schema s = hung.getSchema();

            s.addField(GraphMatcher.MI, Integer.class);
            s.addField(GraphMatcher.MJ, Integer.class);
            s.addField("v", Double.class);
            s.addField(GraphMatcher.ASSIGNED, Integer.class, 0);
            s.addField(GraphMatcher.CROSSED, Integer.class, 0);
            s.setKeyByColumns(GraphMatcher.IJ, GraphMatcher.MI, GraphMatcher.MJ);
            hung.reset(ASSIGNED);
            hung.reset(CROSSED);

            hungarian(hung);

            double answer = correspondenceSum(theom, hung) / Math.min(g1.size(), g2.size())
                    + Math.abs(g1.size() - g2.size());

            return answer;
        } catch (InvalidTupleException | InvalidFieldException | KeyCollisionException e) {
            throw new MatchException();
        }

    }

    private double correspondenceSum(Table theom, Table hung) throws
            InvalidTupleException, InvalidFieldException, KeyCollisionException {
        //Table filtered = hung.filterEquals("v", 1);

        Double sum = 0d;
        Tuple t;
        Tuple tsum;
        Iterator<Tuple> it = this.selectPairs(hung).iterator(); //filtered.iterator();
        while (it.hasNext()) {
            t = it.next();
            tsum = theom.searchRow(IJ, t.get(MI), t.get(MJ));
            sum += (Double) tsum.get("v");
        }

        return sum;
    }

    public void printAs3dArray(Table in, String pattern) throws
            InvalidTupleException, InvalidFieldException {

        Set<Object> il = in.getUniqueValues(MI);
        Set<Object> jl = in.getUniqueValues(MJ);

        java.text.NumberFormat nf = new java.text.DecimalFormat(pattern);;
        //Double[][] answer = new Double[(Integer) in.max(MI)][(Integer)in.min(MJ)];
        Double d;
        Tuple t;
        for (Object oi : il) {

            for (Object oj : jl) {
                t = in.searchRow(IJ, oi, oj);
                if (t != null) {
                    d = (Double) t.get("v");
                    if ((Integer) t.get(CROSSED) > 0) {
                        System.out.print(nf.format(d) + "! ");
                    } else {
                        System.out.print(nf.format(d) + "  ");
                    }
                }
            }
            System.out.println();

        }
        //return answer;
    }
}
