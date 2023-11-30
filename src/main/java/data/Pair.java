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
 * @param <U>
 */
public class Pair<T, U> {
    private T first;
    private U second;

    /**
     *
     * @param first
     * @param second
     */
    public Pair(T first, U second) {
//        if(first==null || second==null){
//            throw new InvalidParameterException();
//        }
        this.first = first;
        this.second = second;
    }

    public static <T, U> Pair Pair(T first, U second){
        return new Pair(first, second);
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
//        if(first==null){
//            throw new InvalidParameterException();
//        }
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
//        if(second==null){
//            throw new InvalidParameterException();
//        }
        this.second = second;
    }

    @Override
    public String toString() {
        return "<"+this.first+"|"+ this.second + ">";
    }
    
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(this.first);
        hash = 41 * hash + Objects.hashCode(this.second);
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
        final Pair<?, ?> other = (Pair<?, ?>) obj;
        if (!Objects.equals(this.first, other.first)) {
            return false;
        }
        if (!Objects.equals(this.second, other.second)) {
            return false;
        }
        return true;
    }
    
    
}
