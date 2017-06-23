package io.connector.mysql;

import java.util.LinkedList;
import java.util.Queue;
import java.util.ListIterator;

public class linkedListQueue<E> {
    private LinkedList<E> list = new LinkedList<E>();

    public synchronized void append(E e) {
        list.add(e);
    }

    public synchronized E poll() {
        return list.poll();
    }

    public synchronized E get() {
        return list.getFirst();
    }

    public synchronized int getSize() {
        return list.size();
    }

    public synchronized boolean isEmpty() {
        return (this.getSize() == 0 ? true : false);
    }

    public synchronized E get(int index) {
        return list.get(index);
    }

    public synchronized ListIterator<E> getAll() {
        return list.listIterator();
        /*while (iterator.hasNext()) {
            System.out.println(iterator.next().getQuerierName());
        }*/
    }
}
