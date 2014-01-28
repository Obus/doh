package doh2.impl.ondemand;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;

public class RetainLastQueue<T>  {
    private final Queue<T> queue;
    private T last;

    public RetainLastQueue(Collection<T> t) {
        this.queue = new LinkedList<T>(t);
    }

    public T peek() {
        return queue.peek();
    }

    public T poll(){
        T poll = queue.poll();
        last = poll == null ? last : poll;
        return last;
    }

    public T getLast() {
        return last;
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
