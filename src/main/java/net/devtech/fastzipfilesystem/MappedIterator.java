package net.devtech.fastzipfilesystem;

import java.util.Iterator;
import java.util.function.Function;

public final class MappedIterator<F, T> implements Iterator<T> {
	final Function<F, T> function;
	final Iterator<F> from;
	
	public MappedIterator(Iterator<F> from, Function<F, T> function) {
		this.function = function;
		this.from = from;
	}
	
	@Override
	public boolean hasNext() {
		return this.from.hasNext();
	}
	
	@Override
	public T next() {
		return this.function.apply(this.from.next());
	}
}
