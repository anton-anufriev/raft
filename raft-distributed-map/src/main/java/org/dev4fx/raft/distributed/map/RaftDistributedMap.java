/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2018 hover-raft (tools4j), Anton Anufriev, Marco Terzer
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.dev4fx.raft.distributed.map;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class RaftDistributedMap<K extends Serializable,V extends Serializable> implements DistributedMap<K, V> {
    private final int mapId;
    private final ConcurrentMap<K, V> map;
    private final Queue<? super MapCommand<K,V>> commandQueue;

    private SetWrapper<K> keySet;
    private SetWrapper<Entry<K, V>> entrySet;
    private ValuesWrapper values;

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
        final SetWrapper<K> ks = keySet;
        if (ks == null) {
            final Consumer<K> remover = RaftDistributedMap.this::remove;
            final Set<K> delegateKeySet = map.keySet();
            return keySet = new SetWrapper<>(delegateKeySet, () -> new IteratorWrapper<>(delegateKeySet.iterator(), k-> k, remover));
        } else {
            return ks;
        }
    }

    @Override
    public Collection<V> values() {
        final ValuesWrapper valuesWrapper = values;
        if (valuesWrapper == null) {
            return values = new ValuesWrapper();
        } else {
            return valuesWrapper;
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        final SetWrapper<Entry<K, V>> es = entrySet;
        if (es == null) {
            final Consumer<Entry<K, V>> remover = entry -> RaftDistributedMap.this.remove(entry.getKey());
            final Set<Entry<K, V>> delegateEntrySet = map.entrySet();
            return entrySet = new SetWrapper<>(delegateEntrySet, () -> new IteratorWrapper<>(delegateEntrySet.iterator(), k -> k, remover));
        } else {
            return es;
        }
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

    private class SetWrapper<T> implements Set<T> {
        final Set<T> delegateKeySet;
        final Supplier<? extends Iterator<T>> iteratorFactory;

        public SetWrapper(final Set<T> delegateKeySet, final Supplier<? extends Iterator<T>> iteratorFactory) {
            this.delegateKeySet = Objects.requireNonNull(delegateKeySet);
            this.iteratorFactory = Objects.requireNonNull(iteratorFactory);
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
        public Iterator<T> iterator() {
            return iteratorFactory.get();
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
        public boolean add(final T k) {
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
        public boolean addAll(final Collection<? extends T> c) {
            return delegateKeySet.addAll(c);
        }

        @Override
        public boolean retainAll(final Collection<?> c) {
            if (c == null) throw new NullPointerException();
            boolean modified = false;
            for (Iterator<T> it = iterator(); it.hasNext();) {
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
            for (Iterator<T> it = iterator(); it.hasNext();) {
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

    private class ValuesWrapper implements Collection<V> {
        private final Consumer<Entry<K, V>> remover = entry -> RaftDistributedMap.this.remove(entry.getKey());

        @Override
        public int size() {
            return map.size();
        }

        @Override
        public boolean isEmpty() {
            return map.isEmpty();
        }

        @Override
        public boolean contains(final Object o) {
            return map.containsValue(o);
        }

        @Override
        public Iterator<V> iterator() {
            return new IteratorWrapper<>(map.entrySet().iterator(), Entry::getValue, remover);
        }

        @Override
        public Object[] toArray() {
            return map.values().toArray();
        }

        @Override
        public <T> T[] toArray(final T[] a) {
            return map.values().toArray(a);
        }

        @Override
        public boolean add(final V v) {
            return map.values().add(v);
        }

        @Override
        public boolean remove(final Object o) {
            if (o != null) {
                for (Iterator<V> it = iterator(); it.hasNext();) {
                    if (o.equals(it.next())) {
                        it.remove();
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public boolean containsAll(final Collection<?> c) {
            return map.values().containsAll(c);
        }

        @Override
        public boolean addAll(final Collection<? extends V> c) {
            return map.values().addAll(c);
        }

        @Override
        public boolean removeAll(final Collection<?> c) {
            if (c == null) throw new NullPointerException();
            boolean modified = false;
            for (Iterator<V> it = iterator(); it.hasNext();) {
                if (!c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

        @Override
        public boolean retainAll(final Collection<?> c) {
            if (c == null) throw new NullPointerException();
            boolean modified = false;
            for (Iterator<V> it = iterator(); it.hasNext();) {
                if (c.contains(it.next())) {
                    it.remove();
                    modified = true;
                }
            }
            return modified;
        }

        @Override
        public void clear() {
            map.clear();
        }
    }

    private class IteratorWrapper<T,V> implements Iterator<V> {
        final Iterator<T> delegateIterator;
        final Function<T, V> valueExtractor;
        final Consumer<T> remover;
        T lastReturned;

        public IteratorWrapper(final Iterator<T> delegateIterator, final Function<T, V> valueExtractor, final Consumer<T> remover) {
            this.delegateIterator = Objects.requireNonNull(delegateIterator);
            this.valueExtractor = Objects.requireNonNull(valueExtractor);
            this.remover = Objects.requireNonNull(remover);
        }

        @Override
        public boolean hasNext() {
            return delegateIterator.hasNext();
        }

        @Override
        public V next() {
            lastReturned = delegateIterator.next();
            return valueExtractor.apply(lastReturned);
        }

        @Override
        public void remove() {
            remover.accept(lastReturned);
        }
    }
}
