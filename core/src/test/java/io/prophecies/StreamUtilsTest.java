package io.prophecies;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class StreamUtilsTest {
	@Test
	public void testIntRangeStream() {
		List<Integer> actual, expected;

		actual = StreamUtils.range(1, 1, 1).collect(Collectors.toList());
		expected = Arrays.asList(1);
		assertThat("Single element expected", actual, is(expected));

		actual = StreamUtils.range(1, 2, 3).collect(Collectors.toList());
		expected = Arrays.asList(1);
		assertThat("expected single element (given step value)", actual, is(expected));

		actual = StreamUtils.range(1, 5, 1).collect(Collectors.toList());
		expected = Arrays.asList(1, 2, 3, 4, 5);
		assertThat("expected an ordinary sequence [1;5]", actual, is(expected));

		actual = StreamUtils.range(0, 10, 3).collect(Collectors.toList());
		expected = Arrays.asList(0, 3, 6, 9);
		assertThat("expected every third number in sequence [0;10]", actual, is(expected));
	}
}
