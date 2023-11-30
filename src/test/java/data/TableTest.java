/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

/**
 *
 * @author caio
 */
public class TableTest {

    Schema s;
    String key1 = "key1";
    String key2 = "key2";
    String field1 = "1";
    String field2 = "2";
    String field3 = "3";
    String field4 = "4";
    Class type1 = String.class;
    Class type2 = Integer.class;
    Class type3 = Object.class;
    Class type4 = Boolean.class;

    public TableTest() {
        s = new Schema().addField(field1, type1).addField(field2, type2).addField(field3, type3).addField(field4, type4, false);
        s.setKeyByColumns(key1, field1);
        s.setKeyByColumns(key2, field1, field2);
    }

    /**
     * Test of getSchema method, of class Table.
     */
    @Test
    public void testGetSchema() {
        System.out.println("getSchema");
        Table instance = new Table(s);
        Schema expResult = s;
        Schema result = instance.getSchema();
        assertEquals(expResult, result);
    }

    /**
     * Test of isEmpty method, of class Table.
     */
    @Test
    public void testIsEmpty() {
        System.out.println("isEmpty");
        Table instance = new Table(s);
        boolean expResult = true;
        boolean result = instance.isEmpty();
        assertEquals(expResult, result);
        try {
            instance.addRow(null, null, null);
        } catch (InvalidTupleException | InvalidFieldException | KeyCollisionException ex) {
            fail();
        }

        expResult = false;
        result = instance.isEmpty();
        assertEquals(expResult, result);
    }

    
    /**
     * Test of addRow method, of class Table.
     */
    @Test
    public void testAddRow() throws Exception {
        System.out.println("addRow");
        Object[] values = {"s", 1, s, false};
        Table instance = new Table(s);
        instance.addRow(values);
        int expResult = 1;
        assertEquals(expResult, instance.size());
    }

   
    /**
     * Test of searchRow method, of class Table.
     */
    @Test
    public void testSearchRow() {
        System.out.println("searchRow");
        Table instance = new Table(s);

        Tuple t1 = null;
        try {
            instance.addRow("s", 1, null, false);
        } catch (InvalidTupleException | InvalidFieldException | KeyCollisionException ex) {
            ex.printStackTrace();
        }
        try {
            instance.addRow("a", 2, null, false);
        } catch (InvalidTupleException | InvalidFieldException | KeyCollisionException ex) {
            ex.printStackTrace();
        }

        Tuple expResult = instance.getRow(0);
        Tuple result = instance.searchRow(key1, "s");
        assertEquals(expResult, result);

        result = instance.searchRow(key1, "x");
        assertNull(result);

        expResult = instance.getRow(1);
        result = instance.searchRow(key2, "a", 2);
        assertEquals(expResult, result);

        result = instance.searchRow(key2, "d", 2);
        System.err.println(result);
        assertNull(result);

        result = instance.searchRow(key2, "a", 32);
        System.err.println(result);
        assertNull(result);

        result = instance.searchRow(key2, "d", 32);
        System.err.println(result);
        assertNull(result);
    }

   
    
    @Test
    public void filterEqualFields() {
        System.out.println("numberOfColumns");
        Table instance = new Table();
        instance.getSchema().addField("FIELD0", Integer.class);
        instance.getSchema().addField("FIELD1", Integer.class);
        Tuple t0 = instance.addRow(1, 0);
        Tuple t1 = instance.addRow(0, 1);
        Tuple t2 = instance.addRow(2, 2);
        int expSize = 1;
        Table result = instance.filterEqualFields("FIELD0", "FIELD1");
        System.err.println(instance.filterEqualFields("FIELD0", "FIELD1"));
        assertEquals(expSize, result.size());
        
        assertTrue(result.stream().findFirst().get().equals(t2));
    }

}
