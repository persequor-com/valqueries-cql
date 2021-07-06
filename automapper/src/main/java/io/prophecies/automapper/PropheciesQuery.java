package io.prophecies.automapper;


import io.ran.CrudRepository;
import io.ran.Property;
import io.ran.RelationDescriber;

import java.util.stream.Stream;

public interface PropheciesQuery<T> extends CrudRepository.InlineQuery<T, PropheciesQuery<T>> {

}
