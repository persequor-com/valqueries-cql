package io.prophecies.automapper;


import io.ran.CrudRepository;
import io.ran.Property;
import io.ran.RelationDescriber;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public interface PropheciesQuery<T> extends CrudRepository.InlineQuery<T, PropheciesQuery<T>> {
	<X> PropheciesQuery<T> subQuery(Function<T, X> field, Consumer<PropheciesQuery<X>> subQuery);
	<X> PropheciesQuery<T> subQuery(BiConsumer<T, X> field, Consumer<PropheciesQuery<X>> subQuery);
	<X> PropheciesQuery<T> subQueryList(Function<T, List<X>> field, Consumer<PropheciesQuery<X>> subQuery);
	<X> PropheciesQuery<T> subQueryList(BiConsumer<T, List<X>> field, Consumer<PropheciesQuery<X>> subQuery);
}
