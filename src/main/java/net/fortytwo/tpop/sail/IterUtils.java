package net.fortytwo.tpop.sail;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

class IterUtils {
    static <D, R, X extends Exception> CloseableIteration<R, X> toCloseableIteration(
            final Iterator<D> iterator,
            final Function<D, R> mapping) {
        return new CloseableIteration<R, X>() {
            @Override
            public void close() throws X {
                // do nothing
            }

            @Override
            public boolean hasNext() throws X {
                return iterator.hasNext();
            }

            @Override
            public R next() throws X {
                return mapping.apply(iterator.next());
            }

            @Override
            public void remove() throws X {
                throw new UnsupportedOperationException();
            }
        };
    }

    static <T, X extends Exception> CloseableIteration<T, X> filter(
            final CloseableIteration<T, X> base,
            final Predicate<T> criterion) {
        return new CloseableIteration<T, X>() {
            private T nextElement;

            @Override
            public void close() throws X {
                base.close();
            }

            @Override
            public boolean hasNext() throws X {
                lookAhead();
                return null != nextElement;
            }

            @Override
            public T next() throws X {
                lookAhead();
                if (null == nextElement) {
                    throw new NoSuchElementException();
                }

                T tmp = nextElement;
                nextElement = null;
                return tmp;
            }

            @Override
            public void remove() throws X {
                throw new UnsupportedOperationException();
            }

            private void lookAhead() throws X {
                while (null == nextElement) {
                    if (!base.hasNext()) break;

                    T tmp = base.next();
                    if (criterion.test(tmp)) {
                        nextElement = tmp;
                    }
                }
            }
        };
    }

    static <T, X extends Exception> Collection<T> collect(final CloseableIteration<T, X> iter) throws X {
        try {
            Collection<T> result = new LinkedList<>();
            while (iter.hasNext()) {
                result.add(iter.next());
            }
            return result;
        } finally {
            iter.close();
        }
    }

    static <T, X extends Exception> long count(final CloseableIteration<T, X> iter) throws X {
        try {
            long count = 0;
            while (iter.hasNext()) {
                iter.next();
                count++;
            }
            return count;
        } finally {
            iter.close();
        }
    }
}
