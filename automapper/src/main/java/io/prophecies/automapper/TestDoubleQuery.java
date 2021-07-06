package io.prophecies.automapper;

import io.ran.CrudRepoBaseQuery;
import io.ran.CrudRepository;
import io.ran.GenericFactory;
import io.ran.MappingHelper;
import io.ran.Property;
import io.ran.RelationDescriber;
import io.ran.TestDoubleDb;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class TestDoubleQuery<T> extends io.ran.TestDoubleQuery<T, PropheciesQuery<T>> implements PropheciesQuery<T> {
	private final Class<T> model;
	private final GenericFactory genericFactory;
	List<Predicate<T>> filters = new ArrayList<>();
	private MappingHelper mappingHelper;
	private final TestDoubleDb store;

	public TestDoubleQuery(Class<T> modelType, MappingHelper mappingHelper, GenericFactory genericFactory, TestDoubleDb store) {
		super(modelType, genericFactory, mappingHelper, store);
		this.mappingHelper = mappingHelper;
		this.store = store;
		this.model = modelType;
		this.genericFactory = genericFactory;
	}

	@Override
	protected PropheciesQuery<T> getQuery(Class<?> aClass) {
		return new TestDoubleQuery<T>((Class)aClass, mappingHelper, genericFactory, store);
	}
}
