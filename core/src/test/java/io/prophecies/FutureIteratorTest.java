package io.prophecies;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.Future;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FutureIteratorTest {

	@Mock
	Future<Boolean> finishedFuture;
	@Mock
	Future<Boolean> unfinishedFuture;
	@Mock
	Future<Boolean> aThirdFuture;

	@Before
	public void setup() {
		when(finishedFuture.isDone()).thenReturn(true);
		when(unfinishedFuture.isDone()).thenReturn(false);
		when(aThirdFuture.isDone()).thenReturn(false);
	}

	@Test
	public void happyPath() {
	FutureIterator<Future<Boolean>> iterator = new FutureIterator<>(Queues.newConcurrentLinkedQueue(Lists.newArrayList(unfinishedFuture, finishedFuture)));

		assertTrue(iterator.hasNext());
		assertSame(finishedFuture, iterator.next());
		assertTrue(iterator.hasNext());
		assertSame(unfinishedFuture, iterator.next());
		assertFalse(iterator.hasNext());
	}

	@Test
	public void happyPath_reOrderIfNextIsUnfinished() {
		FutureIterator<Future<Boolean>> iterator = new FutureIterator<>(Queues.newConcurrentLinkedQueue(Lists.newArrayList(unfinishedFuture, finishedFuture, aThirdFuture)));

		assertTrue(iterator.hasNext());
		assertSame(finishedFuture, iterator.next());

		when(aThirdFuture.isDone()).thenReturn(true);

		assertTrue(iterator.hasNext());
		assertSame(aThirdFuture, iterator.next());
		assertTrue(iterator.hasNext());
		assertSame(unfinishedFuture, iterator.next());
		assertFalse(iterator.hasNext());
	}
}
