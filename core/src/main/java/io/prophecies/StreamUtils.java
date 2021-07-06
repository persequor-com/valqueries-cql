package io.prophecies;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils {
	public static Stream<Integer> range(int start, int end, int step) {
		// Standard JDK8 Stream.range does not accept a 'step' argument
		Iterable<Integer> rangeIterable = () -> new Iterator<Integer>() {
			int val;
			{
				val = start;
			}
			@Override
			public boolean hasNext() {
				return val <= end;
			}

			@Override
			public Integer next() {
				int ret = val;
				val += step;
				return ret;
			}
		};
		return StreamSupport.stream(rangeIterable.spliterator(), false);
	}
}
