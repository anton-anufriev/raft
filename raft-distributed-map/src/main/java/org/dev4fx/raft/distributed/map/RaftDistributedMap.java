package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class RaftDistributedMap<K extends Serializable,V extends Serializable> implements DistributedMap<K, V> {
    private final int mapId;
    private final ConcurrentMap<K, V> map;
    private final Queue<? super MapCommand<K,V>> commandQueue;

    private KeySetWrapper keySet;

    public RaftDistributedMap(final int mapId,
                              final ConcurrentMap<K, V> map,
                              final Queue<? super MapCommand<K,V>> commandQueue) {
        this.mapId = mapId;
        this.map = Objects.requireNonNull(map);
        this.commandQueue = Objects.requireNonNull(commandQueue);
    }

    public int mapId() {
        return mapId;
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean containsKey(final Object key) {
        return map.containsKey(key);
    }

    @Override
    public boolean containsValue(final Object value) {
        return map.containsValue(value);
    }

    @Override
    public V get(final Object key) {
        return map.get(key);
    }

    @Override
    public V put(final K key, final V value) {
        return getValue(nonBlockingPut(key, value), "put");
    }

    private <T> T getValue(final Future<T> future, final String operationName) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Waiting for " + operationName + " result was interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Execution of " + operationName + " failed", e);
        }
    }

    @Override
    public V remove(final Object key) {
        //FIXME definitely it can't be just Object, has to be K, but Map interface ...
        return getValue(nonBlockingRemove((K)key), "remove");
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> fromMap) {
        getValue(nonBlockingPutAll(fromMap), "putAll");
    }

    @Override
    public void clear() {
        getValue(nonBlockingClear(), "clear");
    }

    @Override
    public Set<K> keySet() {
        KeySetWrapper ks;
        return (ks = keySet) != null ? ks : (keySet = new KeySetWrapper(map.keySet()));
    }

    //FIXME support elements removal via raft
    @Override
    public Collection<V> values() {
        return map.values();
    }

    //FIXME support elements removal via raft
    @Override
    public Set<Entry<K, V>> entrySet() {
        return map.entrySet();
    }

    @Override
    public Future<V> nonBlockingPut(final K key, final V value) {
        final FutureResult<V> futureResult = new FutureResult<>();
        commandQueue.add(new PutCommand<>(mapId, key, value, futureResult));
        return futureResult.get();
    }

    @Override
    public Future<V> nonBlockingRemove(final K key) {
        final FutureResult<V> futureResult = new FutureResult<>();
        commandQueue.add(new RemoveCommand<>(mapId, key, futureResult));
        return futureResult.get();
    }

    @Override
    public Future<Void> nonBlockingPutAll(final Map<? extends K, ? extends V> fromMap) {
        final FutureResult<Void> futureResult = new FutureResult<>();
        commandQueue.add(new PutAllCommand<>(mapId, fromMap, futureResult));
        return futureResult.get();
    }

    @Override
    public Future<Void> nonBlockingClear() {
        final FutureResult<Void> futureResult = new FutureResult<>();
        commandQueue.add(new ClearCommand<>(mapId, futureResult));
        return futureResult.get();
    }

    @Override
    public String toString() {
        return map.toString();
    }

    private class KeySetWrapper implements Set<K> {
        final Set<K> delegateKeySet;

        public KeySetWrapper(final Set<K> delegateKeySet) {
            this.delegateKeySet = Objects.requireNonNull(delegateKeySet);
        }

        @Override
        public int size() {
            return delegateKeySet.size();
        }

        @Override
        public boolean isEmpty() {
            return delegateKeySet.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return delegateKeySet.contains(o);
        }

        @Override
        public Iterator<K> iterator() {
            return new KeySetIteratorWrapper(delegateKeySet.iterator());
        }

        @Override
        public Object[] toArray() {
            return delegateKeySet.toArray();
        }

        @Override
        public <T> T[] toArray(final T[] a) {
            return delegateKeySet.toArray(a);
        }

        @Override
        public boolean add(final K k) {
            return delegateKeySet.add(k);
        }

        @Override
        public boolean remove(final Object o) {
            return RaftDistributedMap.this.remove(0) != null;
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return delegateKeySet.containsAll(c);
        }

        @Override
        public boolean addAll(final Collection<? extends K> c) {
            return delegateKeySet.addAll(c);
        }

        @Override
        public boolean retainAll(final Collection<?> c) {
            if (c == null) throw new NullPointerException();
            boolean modified = false;
            for (Iterator<K> it = iterator(); it.hasNext();) {
                if (c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

        @Override
        public boolean removeAll(final Collection<?> c) {
            if (c == null) throw new NullPointerException();
            boolean modified = false;
            for (Iterator<K> it = iterator(); it.hasNext();) {
                if (!c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

        @Override
        public void clear() {
            RaftDistributedMap.this.clear();
        }
    }

    private class KeySetIteratorWrapper implements Iterator<K> {
        final Iterator<K> delegateIterator;
        K lastReturned;

        public KeySetIteratorWrapper(final Iterator<K> delegateIterator) {
            this.delegateIterator = Objects.requireNonNull(delegateIterator);
        }

        @Override
        public boolean hasNext() {
            return delegateIterator.hasNext();
        }

        @Override
        public K next() {
            return lastReturned = delegateIterator.next();
        }

        @Override
        public void remove() {
            RaftDistributedMap.this.remove(lastReturned);
        }
    }
}
