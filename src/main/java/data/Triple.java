/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package data;

import java.util.Objects;

/**
 *
 * @author caio
 * @param <T>
 * @param <U>
 * @param <V>
 */
public class Triple<T, U, V> {
    private T first;
    private U second;
    private V third;

    /**
     *
     * @param first
     * @param second
     */
    public Triple(T first, U second, V third) {
//        if(first==null || second==null){
//            throw new InvalidParameterException();
//        }
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public static <T, U, V> Triple Triple(T first, U second, V third){
        return new Triple(first, second, third);
    }
    /**
     * @return the first
     */
    public T getFirst() {
        return first;
    }

    /**
     * @param first the first to set
     */
    public void setFirst(T first) {
        this.first = first;
    }

    /**
     * @return the second
     */
    public U getSecond() {
        return second;
    }

    /**
     * @param second the second to set
     */
    public void setSecond(U second) {
        this.second = second;
    }

    
    public V getThird(){
        return third;
    }
    
    public void setThird(V third){
        this.third = third;
    }
    
    @Override
    public String toString() {
        return "<"+this.first+"|"+ this.second + "|"+this.third+">";
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 83 * hash + Objects.hashCode(this.first);
        hash = 83 * hash + Objects.hashCode(this.second);
        hash = 83 * hash + Objects.hashCode(this.third);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Triple<?, ?, ?> other = (Triple<?, ?, ?>) obj;
        if (!Objects.equals(this.first, other.first)) {
            return false;
        }
        if (!Objects.equals(this.second, other.second)) {
            return false;
        }
        if (!Objects.equals(this.third, other.third)) {
            return false;
        }
        return true;
    }
    
 
}
