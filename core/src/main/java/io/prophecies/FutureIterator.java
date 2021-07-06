package io.prophecies;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Future;

public class FutureIterator<T extends Future> implements Iterator<T> {
	private Queue<T> futures;

	public FutureIterator(Queue<T> futures) {
		this.futures = futures;
	}

	@Override
	public boolean hasNext() {
		return !futures.isEmpty();
	}

	@Override
	public T next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		Iterator<T> itt = futures.iterator();
		while (itt.hasNext()) {
			T f = itt.next();
			if (f.isDone()) {
				futures.remove(f);
				return f;
			}
		}
		return futures.remove();
	}
}
