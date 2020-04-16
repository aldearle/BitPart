package uk.al_richard.bitpart.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

public class ListofLists<T> extends AbstractList<T> {

        private final List<List<T>> lists;
        private int size = 0;

        public ListofLists() {
            lists = new ArrayList<>();
        }

        public ListofLists( List<T> l ) {
            lists = new ArrayList<>();
            add(l);
        }


        public void add( List<T> list ) {
            lists.add( list );
            size += list.size();
        }

        public void addList(ListofLists<T> other) {
            if( other != null ) {
                List<List<T>> other_lists = other.lists;
                for (List<T> items : other_lists) {
                    add(items);
                }
            }
        }

        @Override
        public T get(int index) {
            if( index < size ) {
                int lists_index = 0;
                while( index >= lists.get(lists_index).size() ) {
                    index -= lists.get(lists_index).size();
                    lists_index++;
                }
                return lists.get(lists_index).get(index);
            } else {
                throw new IndexOutOfBoundsException();
            }
        }

        @Override
        public int size() {
            return size;
        }
}



